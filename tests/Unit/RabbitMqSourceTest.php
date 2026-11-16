<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Unit;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\ScriptedBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;

/**
 * RabbitMqSource without a broker: its arguments, the pull and its wait, the delivery count, the requeue kept in
 * the source, the dead letter and its confirm, and the failures.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Unit
 */
final class RabbitMqSourceTest extends TestCase
{
    private ScriptedBroker $broker;

    protected function setUp(): void
    {
        $this->broker = new ScriptedBroker(fn(): array => [$this->createMock(AbstractConnection::class), $this->createMock(AMQPChannel::class)]);
    }

    public function testTheArgumentsOfTheSource(): void
    {
        foreach ([['amq.q', 25, 5.0], ['q', 0, 5.0], ['q', 25, 0.0], ['q', 25, NAN]] as [$queue, $interval, $timeout]) {
            try {
                new RabbitMqSource(new RabbitMqConnector(), $queue, new RabbitMqTopology(), $interval, $timeout);
                self::fail("$queue $interval $timeout: no exception");
            } catch (InvalidArgumentException $e) {
                self::assertNotSame('', $e->getMessage());
            }
        }
        $this->expectExceptionMessage('The poll interval of a RabbitMQ source is at least 1 ms, and its confirm timeout a positive number of seconds.');
        new RabbitMqSource(new RabbitMqConnector(), 'q', pollIntervalMs: 0);
    }

    public function testTheSmallestIntervalAndTimeoutAreAccepted(): void
    {
        self::assertSame('q', (new RabbitMqSource(new RabbitMqConnector(), 'q', pollIntervalMs: 1, confirmTimeout: 0.001))->queue());
    }

    public function testTheArgumentsOfReceive(): void
    {
        $source = $this->source();
        foreach ([[0, 0, 'receive() takes at least 1 delivery, 0 given.'], [1, -1, 'The wait cannot be negative, -1 given.']] as [$max, $wait, $message]) {
            try {
                $source->receive($max, $wait);
                self::fail('no exception');
            } catch (InvalidArgumentException $e) {
                self::assertSame($message, $e->getMessage());
            }
        }
        self::assertSame(0, $this->broker->connections);
    }

    public function testDeliveriesAreReadWithTheirCount(): void
    {
        $this->broker->gets = [
            ScriptedBroker::delivery(TestMessages::make('A')->toJson(), 1),
            ScriptedBroker::delivery(TestMessages::make('B')->toJson(), 2, ['x-delivery-count' => 3]),
            ScriptedBroker::delivery(TestMessages::make('C')->toJson(), 3, [], redelivered: true),
            ScriptedBroker::delivery(TestMessages::make('D')->toJson(), 4, ['x-delivery-count' => '7'], redelivered: true),
            ScriptedBroker::delivery('nonsense', 5),
        ];

        $received = $this->source()->receive(10, 0);

        self::assertSame(['1.1', '1.2', '1.3', '1.4', '1.5'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $received));
        self::assertSame([1, 4, 2, 2, 1], array_map(static fn(ReceivedMessage $r): int => $r->deliveryCount, $received), 'x-delivery-count + 1; without it, the redelivered flag; a count that is not an integer is not trusted');
        self::assertSame(['A', 'B', 'C', 'D'], array_map(static fn(ReceivedMessage $r): string => $r->message()->payload['alias'], array_slice($received, 0, 4)));
        self::assertFalse($received[4]->isValid());
        self::assertSame(6, $this->broker->getCalls, 'pulled until the queue was empty');
    }

    public function testReceiveStopsAtMax(): void
    {
        $this->broker->gets = [ScriptedBroker::delivery(TestMessages::make('A')->toJson(), 1), ScriptedBroker::delivery(TestMessages::make('B')->toJson(), 2)];

        $received = $this->source()->receive(1, 0);

        self::assertCount(1, $received);
        self::assertSame(1, $this->broker->getCalls);
    }

    public function testAnEmptyQueueIsPolledAtTheIntervalUntilTheWaitRunsOut(): void
    {
        $source = new RabbitMqSource($this->broker->connector(), 'q', pollIntervalMs: 20);

        $started = hrtime(true);
        $received = $source->receive(5, 100);
        $elapsed = (hrtime(true) - $started) / 1e6;

        self::assertSame([], $received);
        self::assertGreaterThanOrEqual(100.0, $elapsed);
        self::assertLessThan(160.0, $elapsed, 'the last pause is cut to what is left of the wait');
        self::assertGreaterThanOrEqual(5, $this->broker->getCalls);
        self::assertLessThanOrEqual(7, $this->broker->getCalls);
    }

    public function testTheLastPauseIsCutToWhatIsLeftOfTheWait(): void
    {
        $source = new RabbitMqSource($this->broker->connector(), 'q', pollIntervalMs: 2000);

        $started = hrtime(true);
        $source->receive(5, 60);
        $elapsed = (hrtime(true) - $started) / 1e6;

        self::assertGreaterThanOrEqual(60.0, $elapsed);
        self::assertLessThan(500.0, $elapsed);
    }

    public function testTheDefaultPollInterval(): void
    {
        self::assertSame(25, RabbitMqSource::DEFAULT_POLL_INTERVAL_MS);
    }

    public function testNoWaitPullsOnce(): void
    {
        $started = hrtime(true);
        $received = $this->source()->receive(5, 0);

        self::assertSame([], $received);
        self::assertSame(1, $this->broker->getCalls);
        self::assertLessThan(15.0, (hrtime(true) - $started) / 1e6);
    }

    public function testAWaitEndsWithTheFirstDelivery(): void
    {
        $this->broker->gets = [null, null, ScriptedBroker::delivery(TestMessages::make('A')->toJson(), 1)];
        $source = new RabbitMqSource($this->broker->connector(), 'q', pollIntervalMs: 5);

        $received = $source->receive(5, 10_000);

        self::assertCount(1, $received);
        self::assertSame(4, $this->broker->getCalls, 'two empty pulls, the delivery, and one more pull that found the queue empty');
    }

    public function testARequeuedDeliveryComesFirstWithItsCountRaisedAndNothingGoesToTheBroker(): void
    {
        $this->broker->gets = [ScriptedBroker::delivery(TestMessages::make('A')->toJson(), 1), ScriptedBroker::delivery(TestMessages::make('B')->toJson(), 2), ScriptedBroker::delivery(TestMessages::make('C')->toJson(), 3)];
        $source = $this->source();
        [$a, $b] = $source->receive(2, 0);

        $source->reject($b, true, 'retry');
        $source->reject($a, true, 'retry');
        $first = $source->receive(1, 0);
        $rest = $source->receive(5, 0);

        self::assertSame(['1.1'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $first));
        self::assertSame(2, $first[0]->deliveryCount);
        self::assertSame(['1.2', '1.3'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $rest), 'given back first, then the queue');
        self::assertSame([2, 1], array_map(static fn(ReceivedMessage $r): int => $r->deliveryCount, $rest));
        self::assertSame([], array_values(array_filter($this->broker->calls, static fn(array $c): bool => in_array($c[0], ['basic_nack', 'basic_reject', 'basic_ack'], true))), 'the broker is not told');
        self::assertSame(3, $source->heldCount());
        $source->ack($first[0]);
        self::assertContains(['basic_ack', 1, false], $this->broker->calls);
        self::assertSame(2, $source->heldCount());
    }

    public function testAGivenBackInvalidDeliveryStaysInvalid(): void
    {
        $this->broker->gets = [ScriptedBroker::delivery('nonsense', 1)];
        $source = $this->source();
        [$invalid] = $source->receive(1, 0);
        $source->reject($invalid, true, 'retry');

        $again = $source->receive(1, 0);

        self::assertFalse($again[0]->isValid());
        self::assertSame($invalid->invalidReason(), $again[0]->invalidReason());
        self::assertSame(2, $again[0]->deliveryCount);
    }

    public function testADeadLetterIsPublishedConfirmedAndThenTheDeliveryAcknowledged(): void
    {
        $original = ScriptedBroker::delivery(TestMessages::make('A', key: 'k', tenant: 'acme')->toJson(), 7, ['aspec-tenant' => 'acme', 'x-delivery-count' => 1, 'trace' => 't']);
        $original->set('content_type', 'application/json');
        $this->broker->gets = [ScriptedBroker::delivery('x', 1), ScriptedBroker::delivery('y', 2), $original];
        $source = new RabbitMqSource($this->broker->connector(), 'agents', new RabbitMqTopology('shop'), confirmTimeout: 1.5);
        [, , $a] = $source->receive(3, 0);

        $source->reject($a, false, 'Refused.');

        [[$copy, $exchange, $routingKey, $mandatory]] = $this->broker->published;
        self::assertSame(['shop.dead', 'agents', true], [$exchange, $routingKey, $mandatory]);
        self::assertSame($original->getBody(), $copy->getBody());
        self::assertSame('m7', $copy->get('message_id'));
        self::assertSame('application/json', $copy->get('content_type'));
        self::assertSame(AMQPMessage::DELIVERY_MODE_PERSISTENT, $copy->get('delivery_mode'));
        self::assertSame(['aspec-tenant' => 'acme', 'trace' => 't', 'aspec-dead-reason' => 'Refused.', 'aspec-dead-delivery-count' => 2, 'aspec-dead-queue' => 'agents'], $copy->get('application_headers')->getNativeData());
        $tail = array_slice($this->broker->calls, -2);
        self::assertSame([['wait', 1.5], ['basic_ack', 7, false]], $tail, 'acknowledged only after the confirm of the copy');
        self::assertSame(2, $source->heldCount());
    }

    public function testADeadLetterTheBrokerDoesNotStoreThrowsAndGivesEverythingBack(): void
    {
        foreach (['nack' => 'basic.nack', 'return' => 'returned 312 NO_ROUTE'] as $answer => $text) {
            $this->setUp();
            $this->broker->gets = [ScriptedBroker::delivery('x', 1), ScriptedBroker::delivery('y', 2)];
            $this->broker->answer = static fn(): string => $answer;
            $source = new RabbitMqSource($this->broker->connector(), 'agents', new RabbitMqTopology('shop'));
            [$x, $y] = $source->receive(2, 0);
            try {
                $source->reject($x, false, 'dead');
                self::fail('no exception');
            } catch (MessagingException $e) {
                self::assertSame(sprintf('The RabbitMQ source could not dead-letter delivery 1.1 of queue "agents" at amqp://scripted (MessagingException: the dead letter exchange "shop.dead" did not store the copy (%s); is the dead letter queue "agents.dead" declared?); every delivery it held goes back to the queue.', $text), $e->getMessage());
            }
            self::assertSame(1, $this->broker->closes);
            self::assertSame(0, $source->heldCount());
            self::assertSame([], array_values(array_filter($this->broker->calls, static fn(array $c): bool => $c[0] === 'basic_ack')));
        }
    }

    public function testADeadLetterWithoutAnAnswerInTimeThrows(): void
    {
        $this->broker->gets = [ScriptedBroker::delivery('x', 1)];
        $this->broker->answer = static fn(): string => 'silent';
        $this->broker->waitError = new AMQPTimeoutException('late');
        $source = $this->source();
        [$x] = $source->receive(1, 0);

        $this->expectExceptionMessage('(AMQPTimeoutException: late); every delivery it held goes back to the queue.');
        $source->reject($x, false, 'dead');
    }

    public function testAFailureOfTheBrokerForgetsEverythingAndTheNextReceiveReconnects(): void
    {
        $this->broker->gets = [ScriptedBroker::delivery('x', 1), ScriptedBroker::delivery('y', 2)];
        $source = $this->source();
        [$x, $y] = $source->receive(2, 0);
        $source->reject($y, true, 'retry');
        $source->close();

        $this->broker->gets = [ScriptedBroker::delivery('x', 1)];
        $again = $source->receive(5, 0);

        self::assertSame(['2.1'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $again), 'what was given back is the broker\'s again; a new connection');
        self::assertSame(2, $this->broker->connections);
        $this->expectException(MessagingException::class);
        $source->ack($x);
    }

    public function testAnUnreachableBrokerThrowsANamedFailure(): void
    {
        $this->broker->connectError = new \RuntimeException('Connection refused');
        $source = $this->source();

        try {
            $source->receive(1, 0);
            self::fail('no exception');
        } catch (MessagingException $e) {
            self::assertSame('The RabbitMQ source could not receive from queue "q" at amqp://scripted (RuntimeException: Connection refused); every delivery it held goes back to the queue.', $e->getMessage());
            self::assertSame($this->broker->connectError, $e->getPrevious());
        }
        self::assertSame(0, $this->broker->closes);
    }

    public function testAnAnswerToAnUnknownDeliveryTalksToNobody(): void
    {
        $source = $this->source();

        foreach ([fn() => $source->ack(ReceivedMessage::invalid(1, '1.1', 'x')), fn() => $source->reject(ReceivedMessage::invalid(1, '1.1', 'x'), true, 'x'), fn() => $source->reject(ReceivedMessage::invalid(1, '1.1', 'x'), false, 'x')] as $answer) {
            try {
                $answer();
                self::fail('no exception');
            } catch (MessagingException $e) {
                self::assertSame('Delivery 1.1 is not in flight on the RabbitMQ source of queue "q": it was already answered, it came from another source, or its connection was lost (the broker delivers it again).', $e->getMessage());
            }
        }
        self::assertSame(0, $this->broker->connections);
    }

    public function testADeliveryOfAnotherSourceIsRefusedEvenWithTheSameBrokerId(): void
    {
        $this->broker->gets = [ScriptedBroker::delivery('x', 1), ScriptedBroker::delivery('y', 1)];
        $first = $this->source();
        $second = $this->source();
        [$x] = $first->receive(1, 0);
        [$y] = $second->receive(1, 0);
        self::assertSame($x->brokerId, $y->brokerId);

        try {
            $second->ack($x);
            self::fail('no exception');
        } catch (MessagingException $e) {
            self::assertStringStartsWith('Delivery 1.1 is not in flight', $e->getMessage());
        }
        $this->expectException(MessagingException::class);
        $second->ack(ReceivedMessage::invalid(1, '1.1', 'a copy of the delivery, not the one handed out'));
    }

    private function source(): RabbitMqSource
    {
        return new RabbitMqSource($this->broker->connector(), 'q');
    }
}
