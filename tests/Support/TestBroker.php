<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

/**
 * The RabbitMQ broker of the tests, from the environment: RABBITMQ_HOST (127.0.0.1), RABBITMQ_PORT (5672),
 * RABBITMQ_USER and RABBITMQ_PASSWORD (guest) and RABBITMQ_VHOST (/). A test that needs it calls
 * TestBroker::require($this), which skips the test with a clear message when the broker cannot be reached.
 *
 * Every test declares its own exchanges and queues, under unique names (uniqueName()), and removes them in its
 * tearDown (cleanUp()), so tests never see each other's messages. The admin channel reads queues, publishes raw
 * bodies and waits for the broker to give back the deliveries of a dead connection.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class TestBroker
{
    private static ?bool $reachable = null;

    private static ?string $unreachableBecause = null;

    private ?AbstractConnection $admin = null;

    private ?AMQPChannel $confirming = null;

    /** @var list<string> */
    private array $queues = [];

    /** @var list<string> */
    private array $exchanges = [];

    /** The connector of the test broker. */
    public static function connector(): RabbitMqConnector
    {
        return new RabbitMqConnector(self::host(), self::port(), self::env('RABBITMQ_USER', 'guest'), self::env('RABBITMQ_PASSWORD', 'guest'), self::env('RABBITMQ_VHOST', '/'));
    }

    /** The URL of the test broker, as RabbitMqConnector::fromUrl() reads it. */
    public static function url(): string
    {
        return sprintf(
            'amqp://%s:%s@%s:%d/%s',
            rawurlencode(self::env('RABBITMQ_USER', 'guest')),
            rawurlencode(self::env('RABBITMQ_PASSWORD', 'guest')),
            self::host(),
            self::port(),
            rawurlencode(self::env('RABBITMQ_VHOST', '/'))
        );
    }

    /** True when a connection to the test broker opens (asked once per process). */
    public static function isReachable(): bool
    {
        if (self::$reachable === null) {
            try {
                self::connector()->connect()->close();
                self::$reachable = true;
            } catch (Throwable $e) {
                self::$reachable = false;
                self::$unreachableBecause = $e->getMessage();
            }
        }

        return self::$reachable;
    }

    /** Skips the test when the broker cannot be reached. */
    public static function require(TestCase $test): void
    {
        if (!self::isReachable()) {
            $test->markTestSkipped(sprintf(
                'RabbitMQ is not reachable at %s:%d (%s). Start one (docker run -d -p 5672:5672 rabbitmq:3.13) or set RABBITMQ_HOST/RABBITMQ_PORT/RABBITMQ_USER/RABBITMQ_PASSWORD/RABBITMQ_VHOST.',
                self::host(),
                self::port(),
                (string) self::$unreachableBecause
            ));
        }
    }

    /** A name no other test uses. */
    public static function uniqueName(string $prefix): string
    {
        return sprintf('%s.%s', $prefix, bin2hex(random_bytes(5)));
    }

    /** A topology of unique exchanges, removed by cleanUp(). */
    public function topology(): RabbitMqTopology
    {
        $topology = new RabbitMqTopology(self::uniqueName('aspec.test'));
        $this->exchanges[] = $topology->exchange();
        $this->exchanges[] = $topology->deadLetterExchange();

        return $topology;
    }

    /**
     * Declares a consumer queue (and its dead letter queue), removed by cleanUp().
     *
     * @param list<string> $patterns
     */
    public function declareQueue(RabbitMqTopology $topology, string $queue, array $patterns = ['#']): string
    {
        $this->track($queue, $topology->deadLetterQueueOf($queue));
        $topology->declareQueue($this->channel(), $queue, $patterns);

        return $queue;
    }

    /** Remembers queues to remove in cleanUp() (declared by the test itself). */
    public function track(string ...$queues): void
    {
        array_push($this->queues, ...$queues);
    }

    /** Remembers exchanges to remove in cleanUp(). */
    public function trackExchange(string ...$exchanges): void
    {
        array_push($this->exchanges, ...$exchanges);
    }

    /** The admin channel (reopened when a declaration closed it). */
    public function channel(): AMQPChannel
    {
        if ($this->admin === null || !$this->admin->isConnected()) {
            $this->admin = self::connector()->connect();
        }
        $channel = $this->admin->channel(1);
        if (!$channel->is_open()) {
            $this->admin = self::connector()->connect();
            $channel = $this->admin->channel(1);
        }

        return $channel;
    }

    /**
     * Publishes a raw body to an exchange (another producer writing to the same exchange).
     *
     * @param array<string, mixed> $headers
     */
    public function publishRaw(string $exchange, string $routingKey, string $body, array $headers = []): void
    {
        $this->channel();
        $channel = $this->admin->channel(2);
        if ($this->confirming !== $channel) {
            $channel->confirm_select();
            $this->confirming = $channel;
        }
        $channel->basic_publish(new AMQPMessage($body, ['delivery_mode' => 2, 'application_headers' => new AMQPTable($headers)]), $exchange, $routingKey);
        $channel->wait_for_pending_acks(5);
    }

    /** Messages ready in a queue (not counting the ones in flight). */
    public function readyCount(string $queue): int
    {
        [, $ready] = $this->channel()->queue_declare($queue, true);

        return (int) $ready;
    }

    /**
     * Takes every message of a queue out of it (acknowledged), in order.
     *
     * @return list<AMQPMessage>
     */
    public function drain(string $queue): array
    {
        $messages = [];
        while (($message = $this->channel()->basic_get($queue, true)) instanceof AMQPMessage) {
            $messages[] = $message;
        }

        return $messages;
    }

    /** Waits up to 5 s for a queue to have at least $count messages ready. */
    public function waitForReady(string $queue, int $count): void
    {
        $deadline = microtime(true) + 5.0;
        while ($this->readyCount($queue) < $count) {
            if (microtime(true) > $deadline) {
                throw new RuntimeException(sprintf('Queue %s did not reach %d ready messages in 5 s (%d).', $queue, $count, $this->readyCount($queue)));
            }
            usleep(10000);
        }
    }

    /** Cuts the socket of a connection, as a process that dies does (no connection.close). */
    public static function kill(AbstractConnection $connection): void
    {
        $socket = $connection->getIO()->getSocket();
        if (is_resource($socket)) {
            fclose($socket);
        }
    }

    /** Removes what the test declared. */
    public function cleanUp(): void
    {
        if (!self::isReachable()) {
            return;
        }
        foreach (array_unique($this->queues) as $queue) {
            try {
                $this->channel()->queue_delete($queue);
            } catch (Throwable) {
                // Already gone.
            }
        }
        foreach (array_unique($this->exchanges) as $exchange) {
            try {
                $this->channel()->exchange_delete($exchange);
            } catch (Throwable) {
                // Already gone.
            }
        }
        try {
            $this->admin?->close();
        } catch (Throwable) {
            // Broken already.
        }
        $this->admin = null;
    }

    private static function host(): string
    {
        return self::env('RABBITMQ_HOST', '127.0.0.1');
    }

    private static function port(): int
    {
        return (int) self::env('RABBITMQ_PORT', '5672');
    }

    private static function env(string $name, string $default): string
    {
        $value = getenv($name);

        return is_string($value) && $value !== '' ? $value : $default;
    }
}
