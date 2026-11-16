<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRabbitMQ\Internal\Amqp;
use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;

/**
 * RabbitMqSource - The Deliveries of One Quorum Queue to One Consumer
 *
 * The IMessageSource of RabbitMQ (read by MessageConsumer), over a consumer queue of RabbitMqTopology:
 *
 * - **receive($max, $waitMs)** pulls with `basic.get` and manual acknowledgement, one message at a time, until
 *   it has $max or the queue is empty; when it found none, it polls again every `pollIntervalMs` until $waitMs
 *   ran out. The source never holds a message the consumer was not handed: there is no prefetch buffer
 *   (`basic.qos` governs the deliveries a broker pushes to a `basic.consume`, which this source does not use),
 *   so the in-flight deliveries are exactly the ones receive() returned and not yet answered.
 * - **The body** is read by ReceivedMessage::decode(): what is not the wire form of the core comes back as an
 *   invalid delivery (MessageConsumer dead-letters it), never dropped.
 * - **The delivery count** is the `x-delivery-count` header a quorum queue puts on a redelivery (the deliveries
 *   before this one) plus 1, plus the times this source handed the delivery out again after a reject with
 *   requeue (below). A dead consumer's deliveries come back from the broker with the header raised.
 * - **reject($requeue = true) keeps the delivery** in this source, unacknowledged, and receive() hands it out
 *   again before anything it pulls from the broker, in the order the deliveries were first received. A quorum
 *   queue of RabbitMQ 3.13 puts a message rejected with requeue at the TAIL of the queue, behind the later
 *   messages of its ordering key, which would break the order of the key (IMessageSource: a requeued delivery
 *   comes again before any later delivery of the same key). Kept here, the delivery is still checked out from
 *   the broker: if this process dies, the broker gives it back at the head of the queue, before the messages
 *   never delivered, with its count raised.
 * - **reject($requeue = false) dead-letters**: the body and the properties are published (with a publisher
 *   confirm and `mandatory`) to the dead letter exchange of the topology with the routing key of the queue, so
 *   that they land in `<queue>.dead`, with three headers: `aspec-dead-reason` (the reason of the consumer, never
 *   the payload), `aspec-dead-delivery-count` and `aspec-dead-queue`; only once the broker confirmed the copy is
 *   the delivery acknowledged. A process that dies in between leaves the delivery to come again, and it may be
 *   dead-lettered twice (at-least-once).
 * - **ack()** is `basic.ack`.
 * - **A failure of the broker** (the connection lost, the queue missing, a dead letter not confirmed) closes the
 *   connection, forgets every delivery in flight (the broker gives them all back, in order, at the head of the
 *   queue) and throws a MessagingException naming the queue and the broker; the next receive() opens a new
 *   connection. ack() or reject() of a delivery this source did not hand out, or no longer holds (already
 *   answered, given back, from another source, of a connection that was lost), is a MessagingException too:
 *   the source answers only the ReceivedMessage objects it returned.
 *
 * Order holds for one source per queue: competing sources on one queue, as in any broker, keep it only per
 * source. Secrecy (RN-05 of the hook): no message of an exception and no header written carries the body.
 *
 * <code>
 * $consumer = new MessageConsumer('agents', new RabbitMqSource($connector, 'agents'), $handlers, $inbox, $transactions);
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RabbitMqSource implements IMessageSource
{
    private ?AbstractConnection $connection = null;

    private ?AMQPChannel $channel = null;

    /** Number of the current connection, the first part of every broker id it gives. */
    private int $generation = 0;

    /** Order in which the deliveries were first received. */
    private int $sequence = 0;

    /** Default pause between two pulls of an empty queue, in milliseconds. */
    public const DEFAULT_POLL_INTERVAL_MS = 25;

    /**
     * Broker id => a delivery held: the message of the broker, its count, its order of arrival, and the
     * ReceivedMessage handed out for it (null while it is given back), the only object ack() and reject() take.
     *
     * @var array<string, array{message: AMQPMessage, count: int, sequence: int, delivery: ?ReceivedMessage}>
     */
    private array $held = [];

    /** @var array<int, string> sequence => broker id, given back with requeue, handed out again first */
    private array $ready = [];

    /** What the broker answered to the dead letter being published ('ack', 'basic.nack', the reply of a return), null before an answer. */
    private ?string $deadAnswer = null;

    /**
     * @param RabbitMqConnector $connector The broker
     * @param string $queue The consumer queue (declared by RabbitMqTopology::declare())
     * @param RabbitMqTopology $topology The dead letter exchange
     * @param int $pollIntervalMs Pause between two pulls of an empty queue while receive() waits (>= 1)
     * @param float $confirmTimeout Longest wait for the confirm of a dead letter, in seconds (> 0)
     * @throws InvalidArgumentException When the queue name, the interval or the timeout is invalid
     */
    public function __construct(
        private readonly RabbitMqConnector $connector,
        private readonly string $queue,
        private readonly RabbitMqTopology $topology = new RabbitMqTopology(),
        private readonly int $pollIntervalMs = self::DEFAULT_POLL_INTERVAL_MS,
        private readonly float $confirmTimeout = 5.0
    ) {
        $topology->deadLetterQueueOf($queue);
        if ($pollIntervalMs < 1 || !Amqp::isPositive($confirmTimeout)) {
            throw new InvalidArgumentException('The poll interval of a RabbitMQ source is at least 1 ms, and its confirm timeout a positive number of seconds.');
        }
    }

    /** The consumer queue. */
    public function queue(): string
    {
        return $this->queue;
    }

    /** {@inheritdoc} */
    public function receive(int $max, int $waitMs): array
    {
        if ($max < 1) {
            throw new InvalidArgumentException(sprintf('receive() takes at least 1 delivery, %d given.', $max));
        }
        if ($waitMs < 0) {
            throw new InvalidArgumentException(sprintf('The wait cannot be negative, %d given.', $waitMs));
        }
        $deadline = hrtime(true) + $waitMs * 1_000_000;
        $received = $this->handOutReady($max);
        try {
            while (true) {
                while (count($received) < $max && ($message = $this->channel()->basic_get($this->queue)) instanceof AMQPMessage) {
                    $received[] = $this->track($message);
                }
                $remaining = $deadline - hrtime(true);
                if ($received !== [] || $remaining <= 0) {
                    return $received;
                }
                usleep(min($this->pollIntervalMs * 1000, intdiv($remaining, 1000)));
            }
        } catch (Throwable $e) {
            throw $this->failure('could not receive from', $e);
        }
    }

    /** {@inheritdoc} */
    public function ack(ReceivedMessage $message): void
    {
        $delivery = $this->mustBeInFlight($message);
        try {
            $this->channel()->basic_ack($delivery['message']->getDeliveryTag());
        } catch (Throwable $e) {
            throw $this->failure(sprintf('could not acknowledge delivery %s of', $message->brokerId), $e);
        }
        $this->forget($message->brokerId);
    }

    /** {@inheritdoc} */
    public function reject(ReceivedMessage $message, bool $requeue, string $reason): void
    {
        $delivery = $this->mustBeInFlight($message);
        if ($requeue) {
            $this->held[$message->brokerId]['delivery'] = null;
            $this->ready[$delivery['sequence']] = $message->brokerId;

            return;
        }
        try {
            $this->deadLetter($delivery['message'], $message->deliveryCount, $reason);
            $this->channel()->basic_ack($delivery['message']->getDeliveryTag());
        } catch (Throwable $e) {
            throw $this->failure(sprintf('could not dead-letter delivery %s of', $message->brokerId), $e);
        }
        $this->forget($message->brokerId);
    }

    /**
     * Deliveries this source holds: handed out and not answered, plus the ones given back with requeue.
     */
    public function heldCount(): int
    {
        return count($this->held);
    }

    /**
     * Closes the connection: the broker gives every delivery held back to the queue, at its head, and this
     * source forgets them. The next receive() opens a new connection.
     */
    public function close(): void
    {
        if ($this->connection !== null) {
            Amqp::closeQuietly($this->connection);
        }
        $this->connection = null;
        $this->channel = null;
        $this->held = [];
        $this->ready = [];
    }

    /**
     * @return list<ReceivedMessage>
     */
    private function handOutReady(int $max): array
    {
        ksort($this->ready);
        $received = [];
        foreach (array_slice($this->ready, 0, $max, true) as $sequence => $brokerId) {
            unset($this->ready[$sequence]);
            $count = ++$this->held[$brokerId]['count'];
            $received[] = $this->held[$brokerId]['delivery'] = ReceivedMessage::decode($this->held[$brokerId]['message']->getBody(), $count, $brokerId);
        }

        return $received;
    }

    private function track(AMQPMessage $message): ReceivedMessage
    {
        $brokerId = $this->generation . '.' . $message->getDeliveryTag();
        $before = Amqp::headersOf($message)[Amqp::HEADER_DELIVERY_COUNT] ?? null;
        $count = is_int($before) ? $before + 1 : ($message->isRedelivered() ? 2 : 1);
        $delivery = ReceivedMessage::decode($message->getBody(), $count, $brokerId);
        $this->held[$brokerId] = ['message' => $message, 'count' => $count, 'sequence' => $this->sequence++, 'delivery' => $delivery];

        return $delivery;
    }

    /**
     * @throws MessagingException When the broker did not confirm the copy
     */
    private function deadLetter(AMQPMessage $original, int $deliveryCount, string $reason): void
    {
        $headers = Amqp::headersOf($original);
        unset($headers[Amqp::HEADER_DELIVERY_COUNT]);
        $headers[Amqp::HEADER_DEAD_REASON] = Amqp::truncate($reason);
        $headers[Amqp::HEADER_DEAD_DELIVERY_COUNT] = $deliveryCount;
        $headers[Amqp::HEADER_DEAD_QUEUE] = $this->queue;
        $properties = $original->get_properties();
        $properties['delivery_mode'] = AMQPMessage::DELIVERY_MODE_PERSISTENT;
        $properties['application_headers'] = new AMQPTable($headers);
        $channel = $this->channel();
        $this->deadAnswer = null;
        $channel->basic_publish(new AMQPMessage($original->getBody(), $properties), $this->topology->deadLetterExchange(), $this->queue, true);
        $channel->wait_for_pending_acks_returns($this->confirmTimeout);
        if ($this->deadAnswer !== 'ack') {
            throw new MessagingException(sprintf(
                'the dead letter exchange "%s" did not store the copy (%s); is the dead letter queue "%s" declared?',
                $this->topology->deadLetterExchange(),
                $this->deadAnswer,
                $this->topology->deadLetterQueueOf($this->queue)
            ));
        }
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel !== null) {
            return $this->channel;
        }
        $this->generation++;
        $this->connection = $this->connector->connect();
        $channel = $this->connection->channel();
        $channel->confirm_select();
        // One dead letter is published at a time, so every answer of the channel is about it; the broker
        // returns an unroutable message before it acknowledges it, and the return is what counts.
        $channel->set_ack_handler(function (): void {
            $this->deadAnswer ??= 'ack';
        });
        $channel->set_nack_handler(function (): void {
            $this->deadAnswer = 'basic.nack';
        });
        $channel->set_return_listener(function (int $code, string $text): void {
            $this->deadAnswer = sprintf('returned %d %s', $code, $text);
        });

        return $this->channel = $channel;
    }

    /**
     * @return array{message: AMQPMessage, count: int, sequence: int, delivery: ?ReceivedMessage}
     * @throws MessagingException
     */
    private function mustBeInFlight(ReceivedMessage $message): array
    {
        if (($this->held[$message->brokerId]['delivery'] ?? null) !== $message) {
            throw new MessagingException(sprintf(
                'Delivery %s is not in flight on the RabbitMQ source of queue "%s": it was already answered, it came from another source, or its connection was lost (the broker delivers it again).',
                $message->brokerId,
                $this->queue
            ));
        }

        return $this->held[$message->brokerId];
    }

    private function forget(string $brokerId): void
    {
        unset($this->held[$brokerId]);
    }

    private function failure(string $what, Throwable $error): MessagingException
    {
        $this->close();

        return new MessagingException(Amqp::truncate(sprintf(
            'The RabbitMQ source %s queue "%s" at %s (%s); every delivery it held goes back to the queue.',
            $what,
            $this->queue,
            $this->connector->describe(),
            Amqp::describe($error)
        )), previous: $error);
    }
}
