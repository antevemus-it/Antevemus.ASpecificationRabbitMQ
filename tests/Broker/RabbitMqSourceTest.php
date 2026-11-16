<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Broker;

use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * RabbitMqSource against the broker: the pull and its wait, the delivery count, the requeue kept in the source
 * (and why: what a quorum queue does with a native requeue), the dead letters, and the failures.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Broker
 */
final class RabbitMqSourceTest extends BrokerTestCase
{
    public function testReceiveTakesAtMostMaxAndNothingMoreIsHeld(): void
    {
        $queue = $this->publish(['A', 'B', 'C']);
        $source = $this->bench->sourceOn($queue);

        $first = $source->receive(2, 0);

        self::assertSame(['A', 'B'], self::aliases($first));
        self::assertSame(2, $source->heldCount());
        self::assertSame(1, $this->broker->readyCount($queue), 'the third message stays in the queue: the source holds nothing it did not hand out');
        self::assertSame(['1.1', '1.2'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $first), 'connection 1, delivery tags 1 and 2');
        self::assertSame(['C'], self::aliases($source->receive(5, 0)));
    }

    public function testAnEmptyQueueIsPolledUntilTheWaitRunsOut(): void
    {
        $source = $this->bench->sourceOn($this->queue(), pollIntervalMs: 40);

        $started = hrtime(true);
        $none = $source->receive(10, 150);
        $waited = (hrtime(true) - $started) / 1e6;
        $started = hrtime(true);
        $nothing = $source->receive(10, 0);
        $immediate = (hrtime(true) - $started) / 1e6;

        self::assertSame([], $none);
        self::assertGreaterThanOrEqual(150.0, $waited);
        self::assertLessThan(400.0, $waited);
        self::assertSame([], $nothing);
        self::assertLessThan(100.0, $immediate);
    }

    public function testAWaitEndsAsSoonAsTheFirstPollFindsSomething(): void
    {
        $queue = $this->publish(['A']);
        $source = $this->bench->sourceOn($queue);

        $started = hrtime(true);
        $received = $source->receive(10, 5000);

        self::assertSame(['A'], self::aliases($received));
        self::assertLessThan(1000.0, (hrtime(true) - $started) / 1e6);
    }

    public function testAQuorumQueueOfThisBrokerRequeuesAtTheTailWhichIsWhyTheSourceKeepsARequeuedDelivery(): void
    {
        // The broker's own requeue (basic.nack with requeue), on a raw channel: A1 goes behind A2 and A3.
        $queue = $this->publish(['A1', 'A2', 'A3']);
        $channel = $this->broker->channel();
        $taken = $channel->basic_get($queue);
        $channel->basic_nack($taken->getDeliveryTag(), false, true);
        $native = array_map(static fn(AMQPMessage $m): string => json_decode($m->getBody(), true)['payload']['alias'], $this->broker->drain($queue));

        // The source: A1 rejected with requeue comes again before A2 and A3.
        $queue = $this->publish(['A1', 'A2', 'A3']);
        $source = $this->bench->sourceOn($queue);
        [$a1] = $source->receive(1, 0);
        $source->reject($a1, true, 'retry');
        $again = $source->receive(10, 0);

        self::assertSame(['A2', 'A3', 'A1'], $native, 'RabbitMQ 3.13 puts a requeued message of a quorum queue at the tail');
        self::assertSame(['A1', 'A2', 'A3'], self::aliases($again));
        self::assertSame([2, 1, 1], array_map(static fn(ReceivedMessage $r): int => $r->deliveryCount, $again));
        self::assertSame($a1->brokerId, $again[0]->brokerId, 'the same delivery, still checked out from the broker');
        self::assertSame(0, $this->broker->readyCount($queue));
    }

    public function testRequeuedDeliveriesComeBackInTheOrderTheyWereFirstReceived(): void
    {
        $queue = $this->publish(['A', 'B', 'C']);
        $source = $this->bench->sourceOn($queue);
        [$a, $b, $c] = $source->receive(3, 0);

        $source->reject($c, true, 'retry');
        $source->reject($a, true, 'retry');
        $source->ack($b);
        $first = $source->receive(1, 0);
        $second = $source->receive(1, 0);

        self::assertSame(['A'], self::aliases($first));
        self::assertSame(['C'], self::aliases($second));
        self::assertSame(2, $source->heldCount(), 'A and C held, B acknowledged');
    }

    public function testTheDeliveryCountOfADeadConsumersDeliveriesComesFromTheBroker(): void
    {
        $queue = $this->publish(['A', 'B']);
        $source = $this->bench->sourceOn($queue);
        [$a] = $source->receive(1, 0);
        $source->reject($a, true, 'retry');
        $source->reject($source->receive(1, 0)[0], true, 'retry');
        [$a3, $b1] = $source->receive(2, 0);

        $successor = $this->bench->successor($source);
        $after = $successor->receive(10, 0);

        self::assertSame([3, 1], [$a3->deliveryCount, $b1->deliveryCount]);
        self::assertSame(['A', 'B'], self::aliases($after), 'given back by the broker at the head of the queue, in order');
        self::assertSame([2, 2], array_map(static fn(ReceivedMessage $r): int => $r->deliveryCount, $after), 'x-delivery-count + 1: the redeliveries this source made itself are not known to the broker');
        self::assertSame(['1.1', '1.2'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $after), 'a new source: the delivery tags of its first connection');
    }

    public function testAClassicQueueGivesOnlyTheRedeliveredFlag(): void
    {
        $queue = TestBroker::uniqueName('aspec.classic');
        $this->broker->track($queue);
        $this->broker->channel()->queue_declare($queue, false, true, false, false);
        $this->broker->publishRaw('', $queue, TestMessages::make('A')->toJson());
        $source = $this->bench->sourceOn($queue);
        self::assertSame(1, $source->receive(1, 0)[0]->deliveryCount);

        $after = $this->bench->successor($source)->receive(1, 0);

        self::assertSame(2, $after[0]->deliveryCount, 'redelivered: at least the second delivery');
    }

    public function testADeadLetterKeepsTheMessageAndAddsTheReason(): void
    {
        $queue = $this->publish(['A'], key: 'quote:1', tenant: 'acme');
        $this->broker->publishRaw($this->topology->exchange(), 'crm.lead_created.v1', '{"broken": ', ['x-delivery-count' => 4, 'producer' => 'legacy']);
        $source = $this->bench->sourceOn($queue);
        [$a, $raw] = $source->receive(2, 0);

        $source->reject($a, false, 'Refused by the test.');
        $source->reject($raw, false, str_repeat('r', 2500));
        $dead = $this->broker->drain($this->topology->deadLetterQueueOf($queue));

        self::assertCount(2, $dead);
        self::assertSame(TestMessages::make('A', key: 'quote:1', tenant: 'acme')->toJson(), $dead[0]->getBody());
        self::assertSame(TestMessages::idOf('A'), $dead[0]->get('message_id'));
        self::assertSame('application/json', $dead[0]->get('content_type'));
        self::assertSame(2, $dead[0]->get('delivery_mode'));
        self::assertSame([
            'aspec-dead-delivery-count' => 1,
            'aspec-dead-queue' => $queue,
            'aspec-dead-reason' => 'Refused by the test.',
            'aspec-ordering-key' => 'quote:1',
            'aspec-tenant' => 'acme',
        ], self::headers($dead[0]));
        self::assertSame('{"broken": ', $dead[1]->getBody(), 'an invalid body is dead-lettered as it came');
        self::assertSame(['aspec-dead-delivery-count' => 5, 'aspec-dead-queue' => $queue, 'aspec-dead-reason' => str_repeat('r', 2000), 'producer' => 'legacy'], self::headers($dead[1]), 'the count of the broker is not copied; the reason is cut at 2 000 characters');
        self::assertSame(2, $dead[1]->get('delivery_mode'), 'persistent even when the original was not');
        self::assertSame(0, $source->heldCount());
        self::assertSame(0, $this->broker->readyCount($queue));
    }

    public function testADeadLetterTheBrokerCannotRouteGivesEverythingBackAndThrows(): void
    {
        // A dead letter exchange with no queue bound for this queue: the copy is returned.
        $queue = TestBroker::uniqueName('aspec.q');
        $this->broker->track($queue);
        $this->broker->channel()->queue_declare($queue, false, true, false, false, false, new AMQPTable(['x-queue-type' => 'quorum']));
        $this->broker->channel()->queue_bind($queue, $this->topology->exchange(), '#');
        (new RabbitMqTransport($this->connector, $this->topology))->send([TestMessages::make('A'), TestMessages::make('B')]);
        $source = $this->bench->sourceOn($queue);
        [$a, $b] = $source->receive(2, 0);

        try {
            $source->reject($a, false, 'dead');
            self::fail('no exception');
        } catch (MessagingException $e) {
            self::assertSame(sprintf(
                'The RabbitMQ source could not dead-letter delivery 1.1 of queue "%s" at amqp://test-broker (MessagingException: the dead letter exchange "%s" did not store the copy (returned 312 NO_ROUTE); is the dead letter queue "%s.dead" declared?); every delivery it held goes back to the queue.',
                $queue,
                $this->topology->deadLetterExchange(),
                $queue
            ), $e->getMessage());
        }
        $this->broker->waitForReady($queue, 2);

        self::assertSame(0, $source->heldCount());
        $this->expectException(MessagingException::class);
        $source->ack($b);
    }

    public function testAnAnswerToADeliveryTheSourceDoesNotHoldIsRefused(): void
    {
        $queue = $this->publish(['A', 'B']);
        $source = $this->bench->sourceOn($queue);
        [$a, $b] = $source->receive(2, 0);
        $source->ack($a);
        $source->reject($b, true, 'retry');

        foreach ([fn() => $source->ack($a), fn() => $source->reject($a, true, 'x'), fn() => $source->reject($b, false, 'x'), fn() => $source->ack(ReceivedMessage::invalid(1, '9.9', 'x'))] as $i => $answer) {
            try {
                $answer();
                self::fail("answer $i: no exception");
            } catch (MessagingException $e) {
                self::assertStringStartsWith('Delivery ', $e->getMessage());
                self::assertStringContainsString(sprintf('is not in flight on the RabbitMQ source of queue "%s": it was already answered, it came from another source, or its connection was lost (the broker delivers it again).', $queue), $e->getMessage());
            }
        }
        self::assertSame(1, $source->heldCount(), 'B, given back, is still held');
    }

    public function testCloseGivesEverythingBackAndForgetsIt(): void
    {
        $queue = $this->publish(['A', 'B']);
        $source = $this->bench->sourceOn($queue);
        [$a, $b] = $source->receive(2, 0);
        $source->reject($b, true, 'retry');

        $source->close();
        $source->close();
        $this->broker->waitForReady($queue, 2);
        $again = $source->receive(10, 0);

        self::assertSame(['A', 'B'], self::aliases($again));
        self::assertSame(['2.1', '2.2'], array_map(static fn(ReceivedMessage $r): string => $r->brokerId, $again));
        $this->expectException(MessagingException::class);
        $source->ack($a);
    }

    public function testALostConnectionThrowsAndTheNextReceiveReconnects(): void
    {
        $queue = $this->publish(['A']);
        $source = $this->bench->sourceOn($queue);
        [$a] = $source->receive(1, 0);
        $this->bench->kill($source);

        try {
            $source->ack($a);
            self::fail('no exception');
        } catch (MessagingException $e) {
            self::assertStringStartsWith(sprintf('The RabbitMQ source could not acknowledge delivery 1.1 of queue "%s" at amqp://test-broker (', $queue), $e->getMessage());
            self::assertInstanceOf(\Throwable::class, $e->getPrevious());
        }
        $this->broker->waitForReady($queue, 1);
        $again = $source->receive(1, 0);

        self::assertSame(['A'], self::aliases($again));
        self::assertSame(2, $again[0]->deliveryCount);
    }

    public function testAMissingQueueThrows(): void
    {
        $source = $this->bench->sourceOn(TestBroker::uniqueName('aspec.nowhere'));

        $this->expectException(MessagingException::class);
        $this->expectExceptionMessageMatches('/^The RabbitMQ source could not receive from queue "aspec\.nowhere\.[0-9a-f]+" at amqp:\/\/test-broker \(AMQPProtocolChannelException: NOT_FOUND - no queue .*\); every delivery it held goes back to the queue\.$/');
        $source->receive(1, 0);
    }

    public function testTheQueueIsNamed(): void
    {
        self::assertSame('agents', (new RabbitMqSource($this->connector, 'agents', new RabbitMqTopology()))->queue());
    }

    /**
     * A new queue with the messages of the aliases, sent by the transport.
     *
     * @param list<string> $aliases
     */
    private function publish(array $aliases, ?string $key = null, ?string $tenant = null): string
    {
        $queue = $this->queue();
        $transport = new RabbitMqTransport($this->connector, $this->topology);
        foreach ($aliases as $alias) {
            self::assertCount(1, $transport->send([TestMessages::make($alias, key: $key, tenant: $tenant)])->confirmedIds());
        }
        $transport->close();

        return $queue;
    }

    /**
     * @param list<ReceivedMessage> $received
     * @return list<string>
     */
    private static function aliases(array $received): array
    {
        return array_map(static fn(ReceivedMessage $r): string => (string) $r->message()->payload['alias'], $received);
    }

    /**
     * @return array<string, mixed>
     */
    private static function headers(AMQPMessage $message): array
    {
        $headers = $message->get('application_headers')->getNativeData();
        ksort($headers);

        return $headers;
    }
}
