<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecification\Messaging\Exceptions\InvalidMessageException;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use LogicException;
use PhpAmqpLib\Connection\AbstractConnection;

/**
 * This package against the test broker: one unique topology per bench, one quorum queue per consumer bound by
 * `#`, and a successor that cuts the socket of the dead consumer (no connection.close, as a killed process) and
 * waits for the broker to give back what it held before it starts.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class RabbitMqBench implements MessagingBench
{
    public readonly TestBroker $broker;

    public readonly RabbitMqTopology $topology;

    private readonly RabbitMqTransport $transport;

    /** @var array<int, array{queue: string, connections: \ArrayObject<int, AbstractConnection>}> source object id => its queue and connections */
    private array $sources = [];

    /** @var array<string, list<array{messageId: ?string, reason: string, deliveryCount: int}>> queue => dead letters read so far */
    private array $dead = [];

    public function __construct(?IMessageTransport $transport = null)
    {
        $this->broker = new TestBroker();
        $this->topology = $this->broker->topology();
        $this->topology->declareExchanges($this->broker->channel());
        $this->transport = new RabbitMqTransport(TestBroker::connector(), $this->topology);
    }

    public function transport(): IMessageTransport
    {
        return $this->transport;
    }

    public function source(string $consumer): IMessageSource
    {
        return $this->sourceOn($this->broker->declareQueue($this->topology, TestBroker::uniqueName('aspec.q.' . $consumer)));
    }

    /** A source on an existing queue, whose connections the bench can cut. */
    public function sourceOn(string $queue, int $pollIntervalMs = 25): RabbitMqSource
    {
        /** @var \ArrayObject<int, AbstractConnection> $connections */
        $connections = new \ArrayObject();
        $connector = RabbitMqConnector::fromFactory(static function () use ($connections): AbstractConnection {
            $connection = TestBroker::connector()->connect();
            $connections->append($connection);

            return $connection;
        }, 'amqp://test-broker');
        $source = new RabbitMqSource($connector, $queue, $this->topology, $pollIntervalMs);
        $this->sources[spl_object_id($source)] = ['queue' => $queue, 'connections' => $connections];

        return $source;
    }

    /** Cuts the socket of the current connection of a source, as a process that dies does. */
    public function kill(RabbitMqSource $source): void
    {
        foreach ($this->entry($source)['connections'] as $connection) {
            TestBroker::kill($connection);
        }
    }

    public function successor(IMessageSource $source): IMessageSource
    {
        $source instanceof RabbitMqSource || throw new LogicException('not a source of this bench');
        $queue = $this->entry($source)['queue'];
        $expected = $this->broker->readyCount($queue) + $source->heldCount();
        $this->kill($source);
        $this->broker->waitForReady($queue, $expected);

        return $this->sourceOn($queue);
    }

    public function storeRaw(string $body): void
    {
        $this->broker->publishRaw($this->topology->exchange(), 'crm.lead_created.v1', $body);
    }

    public function deadLetters(IMessageSource $source): array
    {
        $source instanceof RabbitMqSource || throw new LogicException('not a source of this bench');
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

    public function tearDown(): void
    {
        foreach ($this->sources as $entry) {
            foreach ($entry['connections'] as $connection) {
                try {
                    $connection->close();
                } catch (\Throwable) {
                    // Killed by the test.
                }
            }
        }
        $this->transport->close();
        $this->broker->cleanUp();
    }

    /**
     * @return array{queue: string, connections: \ArrayObject<int, AbstractConnection>}
     */
    private function entry(RabbitMqSource $source): array
    {
        return $this->sources[spl_object_id($source)] ?? throw new LogicException('not a source of this bench');
    }
}
