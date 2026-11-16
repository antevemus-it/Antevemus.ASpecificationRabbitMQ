<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Unit;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use InvalidArgumentException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPIOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * RabbitMqConnector without a broker: the ranges of its values, the URL, the factory, and a password that
 * never shows.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Unit
 */
final class RabbitMqConnectorTest extends TestCase
{
    public function testTheDefaultsNameALocalBroker(): void
    {
        self::assertSame('amqp://127.0.0.1:5672/', (new RabbitMqConnector())->describe());
        self::assertSame('amqp://rabbit.internal:5673/shop', (new RabbitMqConnector('rabbit.internal', 5673, 'app', 'pw', 'shop'))->describe());
    }

    /**
     * @return array<string, array{array<string, mixed>}>
     */
    public static function invalid(): array
    {
        return [
            'empty host' => [['host' => '']],
            'port 0' => [['port' => 0]],
            'port 65536' => [['port' => 65536]],
            'empty vhost' => [['vhost' => '']],
            'connection timeout 0' => [['connectionTimeout' => 0.0]],
            'read/write timeout 0' => [['readWriteTimeout' => 0.0]],
            'read/write timeout negative' => [['readWriteTimeout' => -1.0]],
            'negative heartbeat' => [['heartbeat' => -1]],
            'read/write timeout below twice the heartbeat' => [['heartbeat' => 6, 'readWriteTimeout' => 11.9]],
        ];
    }

    /**
     * @param array<string, mixed> $arguments
     */
    #[DataProvider('invalid')]
    public function testAValueOutOfItsRangeIsRefused(array $arguments): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RabbitMqConnector(...$arguments);
    }

    public function testATimeoutThatIsNotANumberIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RabbitMqConnector(connectionTimeout: NAN);
    }

    public function testTheLimitsAreAccepted(): void
    {
        self::assertSame('amqp://h:1/', (new RabbitMqConnector('h', 1))->describe());
        self::assertSame('amqp://h:65535/', (new RabbitMqConnector('h', 65535))->describe());
        self::assertSame('amqp://h:5672/', (new RabbitMqConnector('h', heartbeat: 6, readWriteTimeout: 12.0))->describe());
        self::assertSame('amqp://h:5672/', (new RabbitMqConnector('h', connectionTimeout: 0.001, readWriteTimeout: 0.001))->describe());
    }

    public function testTheErrorMessagesAreFixed(): void
    {
        try {
            new RabbitMqConnector('', password: 'pw-1');
            self::fail('no exception');
        } catch (InvalidArgumentException $e) {
            self::assertSame('A RabbitMQ connector needs a host, a port from 1 to 65535 and a virtual host.', $e->getMessage());
        }
        $this->expectExceptionMessage('The timeouts of a RabbitMQ connector must be positive, the heartbeat zero or more, and the read/write timeout at least twice the heartbeat.');
        new RabbitMqConnector(heartbeat: -1);
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function urls(): array
    {
        return [
            'everything' => ['amqp://app:p%40ss@rabbit.internal:5673/shop', 'amqp://rabbit.internal:5673/shop'],
            'an encoded vhost' => ['amqp://app:pw@rabbit/a%2Fb', 'amqp://rabbit:5672/a/b'],
            'the default vhost encoded' => ['amqp://app:pw@rabbit/%2F', 'amqp://rabbit:5672/'],
            'no path' => ['amqp://rabbit', 'amqp://rabbit:5672/'],
            'a slash' => ['amqp://rabbit/', 'amqp://rabbit:5672/'],
        ];
    }

    #[DataProvider('urls')]
    public function testAUrlIsRead(string $url, string $described): void
    {
        self::assertSame($described, RabbitMqConnector::fromUrl($url)->describe());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function badUrls(): array
    {
        return [
            'amqps' => ['amqps://app:s3cret-pw@rabbit'],
            'http' => ['http://app:s3cret-pw@rabbit'],
            'no host' => ['amqp:///s3cret-pw'],
            'not a URL' => ['s3cret-pw'],
            'unparseable' => ['amqp://app:s3cret-pw@:-1'],
            'a query' => ['amqp://app:s3cret-pw@rabbit/?heartbeat=10'],
            'a fragment' => ['amqp://app:s3cret-pw@rabbit/#x'],
        ];
    }

    #[DataProvider('badUrls')]
    public function testAnotherUrlIsRefusedWithoutRepeatingIt(string $url): void
    {
        try {
            RabbitMqConnector::fromUrl($url);
            self::fail('no exception');
        } catch (InvalidArgumentException $e) {
            self::assertSame('A RabbitMQ URL is amqp://user:password@host:port/vhost (the URL given is not shown, it may hold a password).', $e->getMessage());
            $frame = $e->getTrace()[0];
            self::assertSame('fromUrl', $frame['function']);
            if (isset($frame['args'])) {
                self::assertInstanceOf(\SensitiveParameterValue::class, $frame['args'][0], 'the URL is hidden in a trace that keeps arguments');
            }
        }
    }

    public function testTheCredentialsOfAUrlReachTheConnection(): void
    {
        // A closed port: the attempt fails, which proves the URL's host and port are the ones used.
        $this->expectException(AMQPIOException::class);
        $this->expectExceptionMessageMatches('#tcp://127\.0\.0\.1:1\b#');
        RabbitMqConnector::fromUrl('amqp://app:pw@127.0.0.1:1/shop')->connect();
    }

    public function testAFactoryIsRunForEachConnection(): void
    {
        $calls = 0;
        $connection = $this->createMock(AbstractConnection::class);
        $connector = RabbitMqConnector::fromFactory(static function () use (&$calls, $connection): AbstractConnection {
            $calls++;

            return $connection;
        }, 'amqp://cluster');

        self::assertSame($connection, $connector->connect());
        self::assertSame($connection, $connector->connect());
        self::assertSame(2, $calls);
        self::assertSame('amqp://cluster', $connector->describe());
        self::assertSame('amqp (custom connection)', RabbitMqConnector::fromFactory(static fn(): AbstractConnection => $connection)->describe());
    }

    public function testThePasswordNeverShowsInATrace(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        try {
            new RabbitMqConnector('', 5672, 'app', 'pw-in-a-trace');
            self::fail('no exception');
        } catch (InvalidArgumentException $e) {
            self::assertStringNotContainsString('pw-in-a-trace', $e->getTraceAsString());
            self::assertStringNotContainsString('pw-in-a-trace', print_r($e->getTrace(), true));
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function testAnUnreachableBrokerThrowsFromConnect(): void
    {
        $this->expectException(AMQPIOException::class);
        (new RabbitMqConnector('127.0.0.1', 1, connectionTimeout: 1.0))->connect();
    }
}
