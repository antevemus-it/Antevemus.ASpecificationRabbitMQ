<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Broker;

use Antevemus\ASpecification\Messaging\ConsumeReport;
use Antevemus\ASpecification\Messaging\Exceptions\MessagingException;
use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecification\Messaging\MessagePublisher;
use Antevemus\ASpecification\Messaging\Outbox\InMemoryMessageOutbox;
use Antevemus\ASpecification\Messaging\Outbox\MessageOutboxStatus;
use Antevemus\ASpecification\Messaging\OutboxRelay;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecification\Messaging\RelayReport;
use Antevemus\ASpecification\Messaging\TransportSendResult;
use Antevemus\ASpecification\StateMachine\Persistence\InMemory\InMemoryTransactionManager;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\EffectLog;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use RuntimeException;
use Throwable;

/**
 * The failures of D13 against the real broker: a relay that dies between the confirm and the mark, a consumer
 * whose connection is cut with deliveries unanswered, a broker that cannot be reached, a message no queue is
 * bound for, and the secrecy of every reason, exception and report on the way.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Broker
 */
final class FailureScenariosTest extends BrokerTestCase
{
    /** A value that must never leave the payload. */
    private const SECRET = 'S3CR3T-cpf-529.982.247-25';

    private InMemoryMessageOutbox $outbox;

    private InMemoryProcessedMessageStore $inbox;

    private EffectLog $effects;

    protected function setUp(): void
    {
        parent::setUp();
        $this->outbox = new InMemoryMessageOutbox();
        $this->inbox = new InMemoryProcessedMessageStore();
        $this->effects = new EffectLog();
    }

    public function testARelayThatDiesBetweenTheConfirmAndTheMarkSendsAgainAndTheEffectRunsOnce(): void
    {
        $queue = $this->queue();
        $transport = new RabbitMqTransport($this->connector, $this->topology);
        $dying = new class ($transport) implements IMessageTransport {
            public bool $die = true;

            public function __construct(private readonly IMessageTransport $inner)
            {
            }

            public function send(array $batch): TransportSendResult
            {
                $result = $this->inner->send($batch);
                if ($this->die) {
                    // The broker confirmed; the process dies before the outbox is marked.
                    throw new RuntimeException('process killed');
                }

                return $result;
            }
        };
        (new MessagePublisher($this->outbox))->publish(TestMessages::make('A', key: 'a'), TestMessages::make('B', key: 'b'));

        $died = (new OutboxRelay($this->outbox, $dying))->relayPending(100);
        $afterDeath = $this->outbox->find(TestMessages::idOf('A'))?->status;
        $dying->die = false;
        $restarted = (new OutboxRelay($this->outbox, $dying))->relayPending(100);
        $report = $this->consumer($this->bench->sourceOn($queue))->consume(100, 0);

        self::assertSame(0, $died->deliveredCount());
        self::assertSame(MessageOutboxStatus::FAILED, $afterDeath, 'not marked delivered: to be sent again');
        self::assertSame(2, $restarted->deliveredCount());
        self::assertSame(['A handled 1', 'B handled 1', 'A duplicate 1', 'B duplicate 1'], TestMessages::transcript($report));
        self::assertSame(['A', 'B'], $this->effects->all());
    }

    public function testDeliveriesOfAConsumerWhoseConnectionIsCutGoToAnotherConsumer(): void
    {
        $queue = $this->queue();
        (new OutboxRelay($this->publish(TestMessages::make('A1', key: 'a'), TestMessages::make('A2', key: 'a'), TestMessages::make('B1', key: 'b')), new RabbitMqTransport($this->connector, $this->topology)))->relayPending(100);
        $first = $this->bench->sourceOn($queue);
        $crash = true;
        // The handler of the first consumer commits A1; its process is killed before the acknowledgement.
        $consumer = $this->consumer($first, onHandled: function () use (&$crash, $first): void {
            if ($crash) {
                $crash = false;
                $this->bench->kill($first);
            }
        });
        try {
            $consumer->consume(100, 0);
            self::fail('the acknowledgement on a dead connection must throw');
        } catch (MessagingException $e) {
            self::assertStringStartsWith('The RabbitMQ source could not acknowledge delivery 1.1 of queue', $e->getMessage());
        }
        $this->broker->waitForReady($queue, 3);

        $report = $this->consumer($this->bench->sourceOn($queue))->consume(100, 0);

        // The queue held A1, B1, A2 (the relay sends the heads of a and b first).
        self::assertSame(['A1 duplicate 2', 'B1 handled 2', 'A2 handled 2'], TestMessages::transcript($report), 'everything the dead one held, given back in order with the count raised; A1 already committed');
        self::assertSame(['A1', 'B1', 'A2'], $this->effects->all());
    }

    public function testABrokerThatCannotBeReachedFailsTheWholeBatchAndNothingIsMarkedDelivered(): void
    {
        $outbox = $this->publish(TestMessages::make('A', key: 'a', payload: ['cpf' => self::SECRET]), TestMessages::make('B', payload: ['cpf' => self::SECRET]));
        $unreachable = new RabbitMqTransport(new RabbitMqConnector('127.0.0.1', 1, 'guest', self::SECRET), $this->topology);

        $report = (new OutboxRelay($outbox, $unreachable))->relayPending(100);

        self::assertSame(0, $report->deliveredCount());
        self::assertSame(2, $report->failedCount());
        foreach ([TestMessages::idOf('A'), TestMessages::idOf('B')] as $id) {
            self::assertStringStartsWith(sprintf('RabbitMQ at amqp://127.0.0.1:1/ did not confirm message "crm.lead_created.v1" (id %s', $id), $report->getFailures()[$id]);
            self::assertStringContainsString('(AMQPIOException: ', $report->getFailures()[$id]);
            self::assertSame(MessageOutboxStatus::FAILED, $outbox->find($id)?->status);
            self::assertSame(1, $outbox->find($id)?->attempts);
        }
        self::assertSecret($report);
    }

    public function testAMessageNoQueueIsBoundForFailsAndIsSentAgainOnceOneIs(): void
    {
        $outbox = $this->publish(TestMessages::make('A', name: 'billing.invoice_overdue.v1', key: 'invoice:7', payload: ['cpf' => self::SECRET]));
        $relay = new OutboxRelay($outbox, new RabbitMqTransport($this->connector, $this->topology));

        $lost = $relay->relayPending(100);
        $queue = $this->queue(['billing.*.v1']);
        $found = $relay->relayPending(100);

        self::assertSame(0, $lost->deliveredCount());
        self::assertStringContainsString('as unroutable (312 NO_ROUTE)', $lost->getFailures()[TestMessages::idOf('A')]);
        self::assertSame(1, $found->deliveredCount());
        self::assertSame(1, $this->broker->readyCount($queue));
        self::assertSecret($lost);
    }

    public function testNothingOnTheWayRepeatsThePayload(): void
    {
        $queue = $this->queue();
        $payload = ['cpf' => self::SECRET];
        $outbox = $this->publish(
            TestMessages::make('A', key: 'a', payload: $payload),
            TestMessages::make('B', key: 'b', payload: $payload),
            TestMessages::make('C', name: 'livechat.message_received.v1', payload: $payload),
            TestMessages::make('D', tenant: 'globex', payload: $payload),
        );
        $relayed = (new OutboxRelay($outbox, new RabbitMqTransport($this->connector, $this->topology)))->relayPending(100);
        $this->broker->publishRaw($this->topology->exchange(), 'crm.lead_created.v1', '{"cpf": "' . self::SECRET . '", ');
        $errors = [];
        $source = $this->bench->sourceOn($queue);
        $consumer = $this->consumer($source, failing: ['A', 'B'], maxDeliveries: 1, onError: function (Message $m, Throwable $e) use (&$errors): void {
            $errors[] = $e->getMessage();
        });

        $report = $consumer->consume(100, 0);
        $dead = $this->broker->drain($this->topology->deadLetterQueueOf($queue));
        $deliveries = $source->receive(10, 0);
        try {
            $source->ack(ReceivedMessage::invalid(1, '7.7', 'x'));
        } catch (MessagingException $e) {
            $exception = $e->getMessage();
        }

        self::assertSame(['A dead_lettered 1', 'B dead_lettered 1', 'C unknown_name 1', 'D handled 1', 'invalid dead_lettered 1'], TestMessages::transcript($report));
        self::assertCount(3, $dead);
        self::assertSame([], $deliveries);
        self::assertStringContainsString(self::SECRET, $errors[0], 'the handler\'s own exception, given to onHandlerError, does quote it');
        self::assertSecret($relayed);
        self::assertSecret($report);
        foreach ($dead as $letter) {
            self::assertStringNotContainsString(self::SECRET, json_encode($letter->get('application_headers')->getNativeData(), JSON_THROW_ON_ERROR));
        }
        self::assertStringNotContainsString(self::SECRET, $exception ?? '');
        $unreachable = new RabbitMqSource(new RabbitMqConnector('127.0.0.1', 1, 'guest', self::SECRET), $queue);
        try {
            $unreachable->receive(1, 0);
            self::fail('no exception');
        } catch (MessagingException $e) {
            self::assertStringNotContainsString(self::SECRET, $e->getMessage());
            self::assertStringNotContainsString(self::SECRET, (string) $e);
        }
    }

    // -------------------------------------------------------------------------------------------------------

    private function publish(Message ...$messages): InMemoryMessageOutbox
    {
        (new MessagePublisher($this->outbox))->publish(...$messages);

        return $this->outbox;
    }

    /**
     * @param list<string> $failing aliases whose handler always throws (with the payload in the message)
     */
    private function consumer(IMessageSource $source, array $failing = [], int $maxDeliveries = 3, ?\Closure $onHandled = null, ?\Closure $onError = null): MessageConsumer
    {
        $handlers = (new MessageHandlerRegistry())->register('crm.lead_created.v1', function (Message $message) use ($failing, $onHandled): void {
            $alias = (string) $message->payload['alias'];
            if (in_array($alias, $failing, true)) {
                throw new RuntimeException('cannot handle ' . json_encode($message->payload));
            }
            $this->effects->record($alias);
            if ($onHandled !== null) {
                $onHandled();
            }
        });

        return new MessageConsumer('billing', $source, $handlers, $this->inbox, new InMemoryTransactionManager([$this->inbox, $this->effects]), $maxDeliveries, onHandlerError: $onError);
    }

    private static function assertSecret(RelayReport|ConsumeReport $report): void
    {
        self::assertStringNotContainsString(self::SECRET, json_encode($report->toArray(), JSON_THROW_ON_ERROR));
    }
}
