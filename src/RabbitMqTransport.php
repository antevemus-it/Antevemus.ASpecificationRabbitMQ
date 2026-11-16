<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ;

use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\TransportSendResult;
use Antevemus\ASpecificationRabbitMQ\Internal\Amqp;
use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;

/**
 * RabbitMqTransport - The Messages of the Outbox, Published With Publisher Confirms
 *
 * The IMessageTransport of RabbitMQ (called by OutboxRelay only). send() publishes every message of the batch to
 * the topic exchange of the topology and then waits, up to `confirmTimeout` seconds, for the answer of the broker
 * to each one; a message is confirmed only by its publisher confirm (`basic.ack`), which RabbitMQ sends once
 * every queue the message was routed to has stored it (for a quorum queue: written by a majority of its
 * replicas).
 *
 * - **The broker message**: routing key = the message name; body = Message::toJson(), the wire form of the core
 *   (`content_type` application/json); persistent (`delivery_mode` 2); `message_id` = the message id, `type` =
 *   the name, `timestamp` = the instant of the fact (seconds); headers `aspec-tenant` and `aspec-ordering-key`
 *   when the message has a tenant or an ordering key, so that a binding of a headers exchange or a policy can
 *   route on them.
 * - **mandatory**: a message no queue is bound for is returned by the broker (`basic.return`, 312 NO_ROUTE)
 *   and counted as failed, even though the broker then acknowledges it: it was stored nowhere.
 * - **basic.nack**: a queue refused the message (a full queue with `x-overflow: reject-publish`, an internal
 *   error): failed.
 * - **The connection**: opened by the connector on the first send and kept; a failure of the connection or of
 *   the channel (the broker unreachable, the exchange missing, the confirmation not in time) fails every message
 *   not confirmed yet, closes the connection, and the next send opens a new one. Nothing is thrown: the result
 *   says which messages failed and why.
 * - **The broker id** of a confirmed message is its message id: RabbitMQ gives a published message no id of its
 *   own (a delivery tag of a publish only counts the publishes of one channel), and `message_id` is the
 *   property a dead letter, the management UI or a trace find it by.
 *
 * Secrecy (RN-05 of the hook): a reason names the message (Message::describe(): its name, id and ordering key),
 * the broker (host, port, virtual host), the exchange and the answer of the broker, never the body.
 *
 * <code>
 * $relay = new OutboxRelay($outbox, new RabbitMqTransport($connector), maxAttempts: 10);
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RabbitMqTransport implements IMessageTransport
{
    private ?AbstractConnection $connection = null;

    private ?AMQPChannel $channel = null;

    /**
     * What the broker answered in the current send, by message id: 'ack', 'nack', or the reply of a return
     * (`312 NO_ROUTE`), which the ack that follows it does not replace.
     *
     * @var array<string, string>
     */
    private array $answers = [];

    /**
     * @param RabbitMqConnector $connector The broker
     * @param RabbitMqTopology $topology The exchange to publish to (its declaration is the application's: RabbitMqTopology::declare())
     * @param float $confirmTimeout Longest wait for the confirms of a batch, in seconds (> 0)
     * @throws InvalidArgumentException When the timeout is not positive
     */
    public function __construct(
        private readonly RabbitMqConnector $connector,
        private readonly RabbitMqTopology $topology = new RabbitMqTopology(),
        private readonly float $confirmTimeout = 5.0
    ) {
        if (!Amqp::isPositive($confirmTimeout)) {
            throw new InvalidArgumentException(sprintf('The confirm timeout must be a positive number of seconds, %s given.', $confirmTimeout));
        }
    }

    /** {@inheritdoc} */
    public function send(array $batch): TransportSendResult
    {
        if ($batch === []) {
            return new TransportSendResult();
        }
        $this->answers = [];
        $failure = null;
        try {
            $channel = $this->channel();
            foreach ($batch as $message) {
                $channel->basic_publish(self::brokerMessage($message), $this->topology->exchange(), $message->name, true);
            }
            $channel->wait_for_pending_acks_returns($this->confirmTimeout);
        } catch (Throwable $e) {
            $this->close();
            $failure = Amqp::describe($e);
        }

        $result = new TransportSendResult();
        foreach ($batch as $message) {
            $id = $message->messageId;
            $answer = $this->answers[$id] ?? null;
            $result = match ($answer) {
                'ack' => $result->withConfirmed($id, $id),
                'nack' => $result->withFailed($id, sprintf(
                    'RabbitMQ refused %s (basic.nack): a queue it is routed to rejected it (x-overflow reject-publish) or could not store it.',
                    $message->describe()
                )),
                null => $result->withFailed($id, Amqp::truncate(sprintf(
                    'RabbitMQ at %s did not confirm %s (%s).',
                    $this->connector->describe(),
                    $message->describe(),
                    $failure
                ))),
                default => $result->withFailed($id, sprintf(
                    'RabbitMQ returned %s as unroutable (%s): no queue is bound to routing key "%s" of exchange "%s".',
                    $message->describe(),
                    $answer,
                    $message->name,
                    $this->topology->exchange()
                )),
            };
        }

        return $result;
    }

    /** Closes the connection (the next send opens a new one). */
    public function close(): void
    {
        if ($this->connection !== null) {
            Amqp::closeQuietly($this->connection);
        }
        $this->connection = null;
        $this->channel = null;
    }

    /**
     * The broker message of a message (see the class description).
     */
    private static function brokerMessage(Message $message): AMQPMessage
    {
        $headers = [];
        if ($message->tenant !== null) {
            $headers[Amqp::HEADER_TENANT] = $message->tenant;
        }
        if ($message->orderingKey !== null) {
            $headers[Amqp::HEADER_ORDERING_KEY] = $message->orderingKey;
        }

        return new AMQPMessage($message->toJson(), [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'message_id' => $message->messageId,
            'type' => $message->name,
            'timestamp' => $message->occurredAt->getTimestamp(),
            'application_headers' => new AMQPTable($headers),
        ]);
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel !== null) {
            return $this->channel;
        }
        $this->connection = $this->connector->connect();
        $channel = $this->connection->channel();
        $channel->confirm_select();
        $channel->set_ack_handler(function (AMQPMessage $message): void {
            $this->answers[$message->get('message_id')] ??= 'ack';
        });
        $channel->set_nack_handler(function (AMQPMessage $message): void {
            $this->answers[$message->get('message_id')] = 'nack';
        });
        $channel->set_return_listener(function (int $code, string $text, string $exchange, string $routingKey, AMQPMessage $message): void {
            $this->answers[$message->get('message_id')] = sprintf('%d %s', $code, $text);
        });

        return $this->channel = $channel;
    }
}
