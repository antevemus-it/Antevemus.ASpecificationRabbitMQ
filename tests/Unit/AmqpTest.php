<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Unit;

use Antevemus\ASpecificationRabbitMQ\Internal\Amqp;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The shared helpers of the transport and the source.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Unit
 */
final class AmqpTest extends TestCase
{
    public function testAFailureIsNamedByItsShortClassAndItsMessage(): void
    {
        self::assertSame('AMQPIOException: refused', Amqp::describe(new AMQPIOException('  refused ')));
        self::assertSame('RuntimeException', Amqp::describe(new RuntimeException('   ')));
        self::assertSame('Exception: boom', Amqp::describe(new \Exception('boom')), 'a class outside any namespace keeps its whole name');
    }

    public function testAReasonIsCutAtTwoThousandCharacters(): void
    {
        self::assertSame(str_repeat('é', 2000), Amqp::truncate(str_repeat('é', 2001)));
        self::assertSame('short', Amqp::truncate('short'));
    }

    public function testTheHeadersOfADelivery(): void
    {
        self::assertSame(['a' => 'b'], Amqp::headersOf(new AMQPMessage('', ['application_headers' => new AMQPTable(['a' => 'b'])])));
        self::assertSame([], Amqp::headersOf(new AMQPMessage('')));
    }

    public function testPositive(): void
    {
        self::assertTrue(Amqp::isPositive(0.001));
        self::assertFalse(Amqp::isPositive(0.0));
        self::assertFalse(Amqp::isPositive(-0.5));
        self::assertFalse(Amqp::isPositive(NAN));
    }

    public function testClosingABrokenConnectionIsQuiet(): void
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->expects(self::once())->method('close')->willThrowException(new AMQPIOException('broken'));

        Amqp::closeQuietly($connection);
    }
}
