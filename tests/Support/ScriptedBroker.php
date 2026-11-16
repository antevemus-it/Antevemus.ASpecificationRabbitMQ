<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * A broker in a script, for the unit tests: a mocked connection and channel of php-amqplib that record what the
 * transport or the source asks and answer what the test says. `answer` decides, for each published message,
 * what the broker answers when the confirms are awaited: 'ack', 'nack', 'return' (a 312 NO_ROUTE, then the
 * ack, as RabbitMQ does), 'silent' (no answer; the wait then throws `waitError`); `gets` is the queue that
 * basic_get pulls from (null = empty).
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class ScriptedBroker
{
    /** @var list<array{AMQPMessage, string, string, bool}> message, exchange, routing key, mandatory */
    public array $published = [];

    /** @var list<array{string, mixed...}> */
    public array $calls = [];

    /** @var Closure(AMQPMessage): string */
    public Closure $answer;

    public ?\Throwable $waitError = null;

    public ?\Throwable $connectError = null;

    /** @var list<?AMQPMessage> */
    public array $gets = [];

    public int $connections = 0;

    public int $closes = 0;

    public int $getCalls = 0;

    private ?Closure $ack = null;

    private ?Closure $nack = null;

    private ?Closure $return = null;

    /** @var list<AMQPMessage> waiting for an answer */
    private array $pending = [];

    /**
     * @param Closure(): array{AbstractConnection, AMQPChannel} $mocks Two fresh mocks (TestCase::createMock()) per connection
     */
    public function __construct(private readonly Closure $mocks)
    {
        $this->answer = static fn(): string => 'ack';
    }

    public function connector(): RabbitMqConnector
    {
        return RabbitMqConnector::fromFactory(function (): AbstractConnection {
            $this->connections++;
            if ($this->connectError !== null) {
                throw $this->connectError;
            }

            return $this->connection();
        }, 'amqp://scripted');
    }

    private function connection(): AbstractConnection
    {
        /** @var \PHPUnit\Framework\MockObject\MockObject&AbstractConnection $connection */
        /** @var \PHPUnit\Framework\MockObject\MockObject&AMQPChannel $channel */
        [$connection, $channel] = ($this->mocks)();
        $self = $this;
        $connection->method('channel')->willReturn($channel);
        $connection->method('close')->willReturnCallback(static function () use ($self): null {
            $self->closes++;

            return null;
        });
        $channel->method('confirm_select')->willReturnCallback(static function () use ($self): null {
            $self->calls[] = ['confirm_select'];

            return null;
        });
        $channel->method('set_ack_handler')->willReturnCallback(static function (callable $handler) use ($self): void {
            $self->ack = Closure::fromCallable($handler);
        });
        $channel->method('set_nack_handler')->willReturnCallback(static function (callable $handler) use ($self): void {
            $self->nack = Closure::fromCallable($handler);
        });
        $channel->method('set_return_listener')->willReturnCallback(static function (callable $handler) use ($self): void {
            $self->return = Closure::fromCallable($handler);
        });
        $channel->method('basic_publish')->willReturnCallback(static function (AMQPMessage $message, string $exchange = '', string $routingKey = '', bool $mandatory = false) use ($self): null {
            $self->published[] = [$message, $exchange, $routingKey, $mandatory];
            $self->pending[] = $message;

            return null;
        });
        $channel->method('wait_for_pending_acks_returns')->willReturnCallback(static function (float|int $timeout) use ($self): null {
            $self->calls[] = ['wait', $timeout];
            $silent = false;
            foreach ($self->pending as $message) {
                $answer = ($self->answer)($message);
                match ($answer) {
                    'ack' => ($self->ack)($message),
                    'nack' => ($self->nack)($message),
                    'return' => [($self->return)(312, 'NO_ROUTE', 'x', 'y', clone $message), ($self->ack)($message)],
                    default => $silent = true,
                };
            }
            $self->pending = [];
            if ($silent && $self->waitError !== null) {
                throw $self->waitError;
            }

            return null;
        });
        $channel->method('basic_get')->willReturnCallback(static function (string $queue = '') use ($self): ?AMQPMessage {
            $self->getCalls++;
            $self->calls[] = ['basic_get', $queue];

            return $self->gets === [] ? null : array_shift($self->gets);
        });
        foreach (['basic_ack', 'basic_nack', 'basic_reject'] as $method) {
            $channel->method($method)->willReturnCallback(static function (mixed ...$arguments) use ($self, $method): null {
                $self->calls[] = [$method, ...$arguments];

                return null;
            });
        }

        return $connection;
    }

    /**
     * A delivery of basic_get.
     *
     * @param array<string, mixed> $headers
     */
    public static function delivery(string $body, int $tag, array $headers = [], bool $redelivered = false): AMQPMessage
    {
        $message = new AMQPMessage($body, ['application_headers' => new \PhpAmqpLib\Wire\AMQPTable($headers), 'message_id' => 'm' . $tag]);
        $message->setDeliveryInfo($tag, $redelivered, 'aspec.messages', 'crm.lead_created.v1');

        return $message;
    }
}
