<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Contract;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecification\Messaging\MessagePublisher;
use Antevemus\ASpecification\Messaging\Outbox\InMemoryMessageOutbox;
use Antevemus\ASpecification\Messaging\OutboxRelay;
use Antevemus\ASpecification\StateMachine\Persistence\InMemory\InMemoryTransactionManager;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\EffectLog;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\MessagingBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The contract battery of the messaging hook, written once and run over two benches: the in-memory pair of the
 * core (InMemoryContractTest, the reference) and this package against a real RabbitMQ (RabbitMqContractTest).
 * Every scenario goes the whole way the application goes: MessagePublisher into the core's InMemoryMessageOutbox,
 * OutboxRelay through the transport of the bench, MessageConsumer over the source of the bench with the core's
 * InMemoryProcessedMessageStore and InMemoryTransactionManager. Each one compares the transcript of the run (one
 * line per delivery: the alias of the message, the outcome, the delivery count), the effects of the handlers and
 * the dead letters with literals written here, the same for both benches.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Contract
 */
abstract class MessagingContractTestCase extends TestCase
{
    protected const NAME = 'crm.lead_created.v1';

    protected MessagingBench $bench;

    protected InMemoryMessageOutbox $outbox;

    protected MessagePublisher $publisher;

    protected InMemoryProcessedMessageStore $inbox;

    protected EffectLog $effects;

    /** @var array<string, int> alias => failures left before its handler succeeds */
    protected array $failuresLeft = [];

    /** @var list<Message> every message a handler was called with, failed calls included */
    protected array $handled = [];

    abstract protected function createBench(): MessagingBench;

    protected function setUp(): void
    {
        $this->bench = $this->createBench();
        $this->outbox = new InMemoryMessageOutbox();
        $this->publisher = new MessagePublisher($this->outbox);
        $this->inbox = new InMemoryProcessedMessageStore();
        $this->effects = new EffectLog();
    }

    protected function tearDown(): void
    {
        $this->bench->tearDown();
    }

    // -- the scenarios --------------------------------------------------------------------------------------

    public function testARoundTripDeliversTheMessageAsPublished(): void
    {
        $source = $this->bench->source('billing');
        $message = TestMessages::make('A', key: 'lead:1', tenant: 'acme', payload: ['name' => 'Ana Lúcia', 'score' => 9.0, 'tags' => ['vip', 'sp'], 'none' => null, 'nested' => ['a' => ['b' => true]]], headers: ['trace-id' => 't-1']);
        $this->publisher->publish($message);

        $relayed = $this->relay()->relayPending(100);
        $report = $this->consumer($source)->consume(100, 0);

        self::assertSame(['A'], $this->aliases($relayed->deliveredIds()));
        self::assertSame(['A handled 1'], TestMessages::transcript($report));
        self::assertSame($message->toArray(), $this->handled[0]->toArray(), 'the message comes out as it went in: id, name, instant, key, tenant, headers and payload');
        self::assertSame(['A'], $this->effects->all());
        self::assertSame([], TestMessages::transcript($this->consumer($source)->consume(100, 0)), 'an acknowledged delivery does not come again');
    }

    public function testEveryConsumerGetsEveryMessage(): void
    {
        $billing = $this->bench->source('billing');
        $agents = $this->bench->source('agents');
        $this->publisher->publish(TestMessages::make('A'), TestMessages::make('B'));
        $this->relay()->relayPending(100);

        self::assertSame(['A handled 1', 'B handled 1'], TestMessages::transcript($this->consumer($billing, consumer: 'billing')->consume(100, 0)));
        self::assertSame(['A handled 1', 'B handled 1'], TestMessages::transcript($this->consumer($agents, consumer: 'agents')->consume(100, 0)));
    }

    public function testMessagesOfAKeyKeepTheirOrderAcrossBatches(): void
    {
        $source = $this->bench->source('billing');
        $this->publisher->publish(
            TestMessages::make('A1', key: 'a'),
            TestMessages::make('B1', key: 'b'),
            TestMessages::make('A2', key: 'a'),
            TestMessages::make('C1'),
            TestMessages::make('B2', key: 'b'),
            TestMessages::make('A3', key: 'a'),
        );

        $relayed = $this->relay(batchSize: 2)->relayPending(100);
        $report = $this->consumer($source)->consume(100, 0);

        // The relay sends the heads of the keys, oldest first, two at a time: [A1, B1], [A2, C1], [B2, A3].
        self::assertSame(3, $relayed->batchCount());
        self::assertSame(['A1', 'B1', 'A2', 'C1', 'B2', 'A3'], $this->aliases($relayed->deliveredIds()));
        self::assertSame(['A1 handled 1', 'B1 handled 1', 'A2 handled 1', 'C1 handled 1', 'B2 handled 1', 'A3 handled 1'], TestMessages::transcript($report));
    }

    public function testARequeuedDeliveryComesBackBeforeTheLaterOnesOfItsKey(): void
    {
        $source = $this->bench->source('billing');
        $this->failuresLeft = ['A1' => 1];
        $this->publisher->publish(TestMessages::make('A1', key: 'a'), TestMessages::make('A2', key: 'a'), TestMessages::make('A3', key: 'a'));
        $this->relay()->relayPending(100);
        $this->publisher->publish(TestMessages::make('B1', key: 'b'));
        $this->relay()->relayPending(100);
        $consumer = $this->consumer($source);

        // The broker holds A1, A2, A3, B1; the first run takes two of them, so A3 and B1 stay behind.
        $first = $consumer->consume(2, 0);
        $second = $consumer->consume(100, 0);

        self::assertSame(['A1 retried 1', 'A2 held_back 1'], TestMessages::transcript($first));
        self::assertSame([
            'A handler of consumer "billing" failed for message "crm.lead_created.v1" (id A1, ordering key "a") (RuntimeException) on delivery 1 of at most 3.',
            'An earlier message of ordering key "a" was retried in this run: message "crm.lead_created.v1" (id A2, ordering key "a") held back.',
        ], TestMessages::reasons($first));
        self::assertSame(['A1 handled 2', 'A2 handled 2', 'A3 handled 1', 'B1 handled 1'], TestMessages::transcript($second));
        self::assertSame(['A1', 'A2', 'A3', 'B1'], $this->effects->all());
    }

    public function testAMessageThatKeepsFailingIsDeadLetteredAfterMaxDeliveries(): void
    {
        $source = $this->bench->source('billing');
        $this->failuresLeft = ['X' => PHP_INT_MAX];
        $this->publisher->publish(TestMessages::make('X', key: 'x'), TestMessages::make('Y', key: 'x'));
        $this->relay()->relayPending(100);
        $consumer = $this->consumer($source);

        $runs = [];
        for ($i = 0; $i < 4; $i++) {
            $runs[] = TestMessages::transcript($consumer->consume(100, 0));
        }

        self::assertSame([
            ['X retried 1', 'Y held_back 1'],
            ['X retried 2', 'Y held_back 2'],
            ['X dead_lettered 3', 'Y handled 3'],
            [],
        ], $runs);
        self::assertSame(['Y'], $this->effects->all());
        self::assertSame([[
            'messageId' => 'X',
            'reason' => 'A handler of consumer "billing" failed for message "crm.lead_created.v1" (id X, ordering key "x") (RuntimeException) on delivery 3 of at most 3.',
            'deliveryCount' => 3,
        ]], $this->deadLetters($source));
    }

    public function testABodyThatIsNotAMessageIsDeadLetteredNeverDropped(): void
    {
        $source = $this->bench->source('billing');
        $this->bench->storeRaw('not json at all');
        $this->bench->storeRaw('{"v":2,"id":"x"}');
        $this->publisher->publish(TestMessages::make('A'));
        $this->relay()->relayPending(100);

        $report = $this->consumer($source)->consume(100, 0);

        self::assertSame(['invalid dead_lettered 1', 'invalid dead_lettered 1', 'A handled 1'], TestMessages::transcript($report));
        $reasons = [
            'The delivery is not a message of format 1: A message document of 15 bytes is not valid JSON, or its payload is nested deeper than 64 levels.',
            'The delivery is not a message of format 1: A message document must declare "v": 1.',
        ];
        self::assertSame([...$reasons, null], TestMessages::reasons($report));
        self::assertSame([
            ['messageId' => null, 'reason' => $reasons[0], 'deliveryCount' => 1],
            ['messageId' => null, 'reason' => $reasons[1], 'deliveryCount' => 1],
        ], $this->deadLetters($source));
    }

    public function testAMessageWithoutAHandlerIsAcknowledgedWithAWarning(): void
    {
        $source = $this->bench->source('billing');
        $this->publisher->publish(TestMessages::make('A', name: 'livechat.message_received.v1'), TestMessages::make('B'));
        $this->relay()->relayPending(100);
        $consumer = $this->consumer($source);

        $report = $consumer->consume(100, 0);

        self::assertSame(['A unknown_name 1', 'B handled 1'], TestMessages::transcript($report));
        self::assertSame(['Consumer "billing" has no handler for message "livechat.message_received.v1" (id A): acknowledged without handling.'], array_map(TestMessages::aliased(...), $report->warnings()));
        self::assertSame([], TestMessages::transcript($consumer->consume(100, 0)));
    }

    public function testAConsumerOfOneTenantAcknowledgesTheOthersWithoutHandling(): void
    {
        $source = $this->bench->source('billing');
        $this->publisher->publish(TestMessages::make('A', tenant: 'acme'), TestMessages::make('B', tenant: 'globex'), TestMessages::make('C'));
        $this->relay()->relayPending(100);
        $consumer = $this->consumer($source, tenant: 'acme');

        $report = $consumer->consume(100, 0);

        self::assertSame(['A handled 1', 'B other_tenant 1', 'C other_tenant 1'], TestMessages::transcript($report));
        self::assertSame([
            null,
            'Consumer "billing" handles tenant "acme" only; message "crm.lead_created.v1" (id B) is of tenant "globex": acknowledged without handling.',
            'Consumer "billing" handles tenant "acme" only; message "crm.lead_created.v1" (id C) is of no tenant: acknowledged without handling.',
        ], TestMessages::reasons($report));
        self::assertSame(['A'], $this->effects->all());
        self::assertSame([], TestMessages::transcript($consumer->consume(100, 0)));
    }

    public function testTheLargestPayloadTravels(): void
    {
        $source = $this->bench->source('billing');
        // {"alias":"L","text":"..."} of exactly 65 535 bytes, the text in two-byte characters (and one ASCII when odd).
        $room = Message::MAX_PAYLOAD_BYTES - strlen('{"alias":"L","text":""}');
        $message = TestMessages::make('L', payload: ['text' => str_repeat('é', intdiv($room, 2)) . str_repeat('a', $room % 2)]);
        self::assertSame(Message::MAX_PAYLOAD_BYTES, $message->payloadSize());
        $this->publisher->publish($message);

        $this->relay()->relayPending(100);
        $report = $this->consumer($source)->consume(100, 0);

        self::assertSame(['L handled 1'], TestMessages::transcript($report));
        self::assertSame($message->payload, $this->handled[0]->payload);
    }

    public function testABatchOfMixedKeysKeepsEveryKeyInOrder(): void
    {
        $source = $this->bench->source('billing');
        $published = [];
        for ($n = 1; $n <= 4; $n++) {
            foreach (['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'] as $key) {
                $published[] = TestMessages::make(strtoupper($key) . $n, key: $key);
            }
            $published[] = TestMessages::make('U' . $n);
        }
        $this->publisher->publish(...$published);

        $relayed = $this->relay()->relayPending(100);
        $report = $this->consumer($source)->consume(100, 0);

        self::assertSame(4, $relayed->batchCount(), 'a round per position of the keys: every head of the outbox in one batch');
        self::assertSame(36, $relayed->deliveredCount());
        $byKey = [];
        foreach (TestMessages::transcript($report) as $line) {
            [$alias, $outcome] = explode(' ', $line);
            self::assertSame('handled', $outcome);
            $byKey[$alias[0]][] = (int) substr($alias, 1);
        }
        self::assertSame(array_fill_keys(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'U'], [1, 2, 3, 4]), $byKey);
        self::assertSame(['A1', 'B1', 'C1', 'D1', 'E1', 'F1', 'G1', 'H1', 'U1', 'U2', 'U3', 'U4', 'A2'], array_slice($this->effects->all(), 0, 13));
    }

    public function testADuplicateIsAbsorbedByTheInbox(): void
    {
        $source = $this->bench->source('billing');
        $message = TestMessages::make('A', key: 'a');
        $this->publisher->publish($message);
        $this->relay()->relayPending(100);
        // The broker gets the message again: a relay that died between the confirm and the mark, a publisher
        // that retried.
        $again = $this->bench->transport()->send([$message]);
        $this->bench->transport()->send([$message]);

        $report = $this->consumer($source)->consume(100, 0);

        self::assertTrue($again->isConfirmed($message->messageId));
        self::assertSame(['A handled 1', 'A duplicate 1', 'A duplicate 1'], TestMessages::transcript($report));
        self::assertSame(['A'], $this->effects->all());
    }

    public function testTheDeliveriesOfADeadConsumerGoToItsSuccessor(): void
    {
        $source = $this->bench->source('billing');
        $this->publisher->publish(TestMessages::make('A1', key: 'a'), TestMessages::make('A2', key: 'a'), TestMessages::make('B1', key: 'b'));
        $this->relay()->relayPending(100);
        // The consumer takes two deliveries and dies before it answers them.
        self::assertCount(2, $source->receive(2, 0));

        $successor = $this->bench->successor($source);
        $report = $this->consumer($successor)->consume(100, 0);

        // The broker holds A1, B1, A2 (the relay sent the heads of a and b first); the dead consumer had A1 and B1.
        self::assertSame(['A1 handled 2', 'B1 handled 2', 'A2 handled 1'], TestMessages::transcript($report));
        self::assertSame(['A1', 'B1', 'A2'], $this->effects->all());
    }

    // -- the application around the bench -------------------------------------------------------------------

    protected function relay(int $batchSize = 100): OutboxRelay
    {
        return new OutboxRelay($this->outbox, $this->bench->transport(), batchSize: $batchSize);
    }

    protected function consumer(IMessageSource $source, ?string $tenant = null, int $maxDeliveries = 3, string $consumer = 'billing'): MessageConsumer
    {
        $handlers = (new MessageHandlerRegistry())->register(self::NAME, function (Message $message): void {
            $alias = (string) $message->payload['alias'];
            $this->handled[] = $message;
            $this->effects->record($alias);
            if (($this->failuresLeft[$alias] ?? 0) > 0) {
                $this->failuresLeft[$alias]--;
                // The message of a handler's exception may quote the payload; the reasons never repeat it.
                throw new RuntimeException('could not handle ' . json_encode($message->payload));
            }
        });

        return new MessageConsumer($consumer, $source, $handlers, $this->inbox, new InMemoryTransactionManager([$this->inbox, $this->effects]), $maxDeliveries, $tenant);
    }

    /**
     * @return list<array{messageId: ?string, reason: string, deliveryCount: int}>
     */
    protected function deadLetters(IMessageSource $source): array
    {
        return array_map(
            static fn(array $dead): array => ['messageId' => $dead['messageId'] === null ? null : TestMessages::aliased($dead['messageId']), 'reason' => TestMessages::aliased($dead['reason']), 'deliveryCount' => $dead['deliveryCount']],
            $this->bench->deadLetters($source)
        );
    }

    /**
     * @param list<string> $ids
     * @return list<string>
     */
    protected function aliases(array $ids): array
    {
        return array_map(TestMessages::aliased(...), $ids);
    }
}
