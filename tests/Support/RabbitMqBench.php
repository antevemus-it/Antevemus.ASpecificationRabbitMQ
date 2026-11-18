<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecification\Messaging\Exceptions\InvalidMessageException;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\Testing\IMessagingBench;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use ArrayObject;
use LogicException;
use PhpAmqpLib\Connection\AbstractConnection;
use Throwable;
use WeakMap;

/**
 * This package against the test broker, as the contract battery of the core sees it (IMessagingBench of
 * antevemus/aspecification 2.15), and as the broker tests use it:
 *
 * - **Isolation**: one topology of unique exchanges per bench; per consumer, one quorum queue of a unique name,
 *   bound by the message names it reads (and its dead letter queue); close() deletes all of them.
 * - **source()**: a RabbitMqSource on the queue of the consumer, declared on its first source; a later member
 *   of the same consumer competes on the same queue, and names it adds are bound too.
 * - **successor() and restarted()**: the same mechanism on RabbitMQ, a new connection on the queue of the
 *   consumer: the socket of the dead process is cut (no connection.close, as a killed process), and the bench
 *   waits for the broker to give back what it held, at the head of the queue, with `x-delivery-count` raised.
 * - **transportWithLostConnection()**: a RabbitMqTransport whose connection loses its socket as soon as it is
 *   opened: every send fails every message with a reason, and nothing reaches the broker.
 * - **storeRaw()**: a body published raw to the exchange, with the message name as routing key.
 * - **deadLetters()**: the dead letter queue of the consumer, drained and kept, with the reason and the count of
 *   the headers `aspec-dead-reason` and `aspec-dead-delivery-count`.
 * - **receiveWaitMs()**: 0, since a message confirmed by the broker is in the quorum queue when send() returns.
 *
 * @version    1.0.1
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class RabbitMqBench implements IMessagingBench
{
    public readonly TestBroker $broker;

    public readonly RabbitMqTopology $topology;

    private readonly RabbitMqTransport $transport;

    /** @var list<RabbitMqTransport> transports of a lost connection, closed with the bench */
    private array $lostTransports = [];

    /** @var array<string, string> consumer => its queue */
    private array $queues = [];

    /** @var WeakMap<RabbitMqSource, array{queue: string, member: string, connections: ArrayObject<int, AbstractConnection>}> source => its queue, member and connections */
    private WeakMap $sources;

    /** @var list<ArrayObject<int, AbstractConnection>> the connections of every source made, closed with the bench */
    private array $connections = [];

    /** @var array<string, list<array{messageId: ?string, reason: string, deliveryCount: int}>> queue => dead letters read so far */
    private array $dead = [];

    public function __construct()
    {
        $this->sources = new WeakMap();
        $this->broker = new TestBroker();
        $this->topology = $this->broker->topology();
        $this->topology->declareExchanges($this->broker->channel());
        $this->transport = new RabbitMqTransport(TestBroker::connector(), $this->topology);
    }

    public function transport(): IMessageTransport
    {
        return $this->transport;
    }

    public function transportWithLostConnection(): IMessageTransport
    {
        $connector = RabbitMqConnector::fromFactory(static function (): AbstractConnection {
            $connection = TestBroker::connector()->connect();
            TestBroker::kill($connection);

            return $connection;
        }, 'amqp://test-broker (connection lost)');

        return $this->lostTransports[] = new RabbitMqTransport($connector, $this->topology);
    }

    public function source(string $consumer, array $names, string $member = 'm1'): IMessageSource
    {
        $queue = $this->queues[$consumer] ??= TestBroker::uniqueName('aspec.q.' . $consumer);
        $this->broker->declareQueue($this->topology, $queue, $names);

        return $this->sourceOn($queue, $member);
    }

    /** A source on an existing queue, whose connections the bench can cut. */
    public function sourceOn(string $queue, string $member = 'm1', int $pollIntervalMs = 25): RabbitMqSource
    {
        /** @var ArrayObject<int, AbstractConnection> $connections */
        $connections = new ArrayObject();
        $connector = RabbitMqConnector::fromFactory(static function () use ($connections): AbstractConnection {
            $connection = TestBroker::connector()->connect();
            $connections->append($connection);

            return $connection;
        }, 'amqp://test-broker');
        $source = new RabbitMqSource($connector, $queue, $this->topology, $pollIntervalMs);
        $this->sources[$source] = ['queue' => $queue, 'member' => $member, 'connections' => $connections];
        $this->connections[] = $connections;

        return $source;
    }

    /** Cuts the socket of every connection of a source, as a process that dies does. */
    public function kill(RabbitMqSource $source): void
    {
        foreach ($this->entry($source)['connections'] as $connection) {
            TestBroker::kill($connection);
        }
    }

    public function successor(IMessageSource $source): IMessageSource
    {
        return $this->replace($source, $this->entry($source)['member'] . '-next');
    }

    public function restarted(IMessageSource $source): IMessageSource
    {
        return $this->replace($source, $this->entry($source)['member']);
    }

    public function storeRaw(string $name, string $body): void
    {
        $this->broker->publishRaw($this->topology->exchange(), $name, $body);
    }

    public function deadLetters(IMessageSource $source): array
    {
        $queue = $this->entry($source)['queue'];
        foreach ($this->broker->drain($this->topology->deadLetterQueueOf($queue)) as $message) {
            $headers = $message->get('application_headers')->getNativeData();
            try {
                $messageId = Message::fromJson($message->getBody())->messageId;
            } catch (InvalidMessageException) {
                $messageId = null;
            }
            $this->dead[$queue][] = ['messageId' => $messageId, 'reason' => $headers['aspec-dead-reason'], 'deliveryCount' => $headers['aspec-dead-delivery-count']];
        }

        return $this->dead[$queue] ?? [];
    }

    public function receiveWaitMs(): int
    {
        return 0;
    }

    public function close(): void
    {
        foreach ($this->connections as $connections) {
            foreach ($connections as $connection) {
                try {
                    $connection->close();
                } catch (Throwable) {
                    // Killed by the test.
                }
            }
        }
        foreach ($this->lostTransports as $transport) {
            $transport->close();
        }
        $this->transport->close();
        $this->broker->cleanUp();
    }

    /** The process behind a source died: a new connection of $member on its queue, once the broker gave back what it held. */
    private function replace(IMessageSource $source, string $member): RabbitMqSource
    {
        $queue = $this->entry($source)['queue'];
        \assert($source instanceof RabbitMqSource);
        $expected = $this->broker->readyCount($queue) + $source->heldCount();
        $this->kill($source);
        $this->broker->waitForReady($queue, $expected);

        return $this->sourceOn($queue, $member);
    }

    /**
     * @return array{queue: string, member: string, connections: ArrayObject<int, AbstractConnection>}
     */
    private function entry(IMessageSource $source): array
    {
        if (!$source instanceof RabbitMqSource || !isset($this->sources[$source])) {
            throw new LogicException('not a source of this bench');
        }

        return $this->sources[$source];
    }
}
