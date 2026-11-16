<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Property;

use Antevemus\ASpecification\Messaging\ConsumeOutcome;
use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecification\Messaging\MessagePublisher;
use Antevemus\ASpecification\Messaging\Outbox\InMemoryMessageOutbox;
use Antevemus\ASpecification\Messaging\OutboxRelay;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecification\Messaging\TransportSendResult;
use Antevemus\ASpecification\StateMachine\Persistence\InMemory\InMemoryTransactionManager;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\EffectLog;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\InMemoryBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\MessagingBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\RabbitMqBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Random\Engine\Mt19937;
use Random\Randomizer;
use RuntimeException;

/**
 * Random interleavings of publish, relay and consume, with fixed seeds, over the real broker (and, as the
 * reference, over the in-memory pair of the core): STEPS steps per seed, each one a publication of 1 to 3
 * messages on 3 ordering keys or none, a relay run (limit 1 to 5, batches of 1 to 3) whose process dies after
 * the broker confirmed a batch and before the outbox is marked, in 15 % of the runs, or a consume run (1 to 4
 * deliveries) whose process dies before an acknowledgement, at 10 % of the acknowledgements (its successor takes over what
 * it held). A message fails its first 0 to 2 deliveries, or always (a poison message, 10 %); maxDeliveries is 3.
 * At the end the outbox is relayed and the queue consumed until both are empty, and the run must show:
 *
 * - no confirmed message lost: every message published was handled or dead-lettered;
 * - no effect twice: every message handled at most once (the inbox absorbs the copies);
 * - the order of every key kept: the messages of a key handled in the order of publication, leaving out the
 *   ones dead-lettered (declared in the reports, RN-04 of the hook);
 * - the dead letters reported: the dead letter queue holds exactly the deliveries the reports dead-lettered,
 *   and every poison message is among them.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Property
 */
final class InterleavingPropertyTest extends TestCase
{
    public const SEEDS = [11, 23, 2026];

    public const STEPS = 150;

    private const KEYS = ['k1', 'k2', 'k3', null];

    private const MAX_DELIVERIES = 3;

    private ?MessagingBench $bench = null;

    /** @var array<string, array<int, array<string, int>>> what each run did, for the record */
    public static array $summary = [];

    /**
     * @return array<string, array{string, int}>
     */
    public static function runs(): array
    {
        $runs = [];
        foreach (['memory', 'rabbitmq'] as $bench) {
            foreach (self::SEEDS as $seed) {
                $runs["$bench seed $seed"] = [$bench, $seed];
            }
        }

        return $runs;
    }

    protected function tearDown(): void
    {
        $this->bench?->tearDown();
    }

    #[DataProvider('runs')]
    public function testNoConfirmedMessageIsLostNoEffectRunsTwiceAndEveryKeyKeepsItsOrder(string $bench, int $seed): void
    {
        if ($bench === 'rabbitmq') {
            TestBroker::require($this);
        }
        $this->bench = $bench === 'rabbitmq' ? new RabbitMqBench() : new InMemoryBench();
        $random = new Randomizer(new Mt19937($seed));
        $outbox = new InMemoryMessageOutbox();
        $publisher = new MessagePublisher($outbox);
        $inbox = new InMemoryProcessedMessageStore();
        $effects = new EffectLog();
        $source = $this->bench->source('billing');
        $dying = new DyingTransport($this->bench->transport());

        /** @var array<string, ?string> $published alias => key, in the order of publication */
        $published = [];
        /** @var array<string, int> $failuresLeft */
        $failuresLeft = [];
        $poison = [];
        $deadReported = [];
        $handlers = (new MessageHandlerRegistry())->register('crm.lead_created.v1', static function (Message $message) use (&$failuresLeft, $effects): void {
            $alias = (string) $message->payload['alias'];
            $effects->record($alias);
            if ($failuresLeft[$alias] > 0) {
                $failuresLeft[$alias]--;
                throw new RuntimeException('failed');
            }
        });
        $consumerDeaths = 0;
        $consume = function (int $max, int $dieBeforeAckPercent) use (&$source, &$deadReported, &$consumerDeaths, $random, $handlers, $inbox, $effects): int {
            $dyingSource = new DyingSource($source, $random, $dieBeforeAckPercent);
            $consumer = new MessageConsumer('billing', $dyingSource, $handlers, $inbox, new InMemoryTransactionManager([$inbox, $effects]), self::MAX_DELIVERIES);
            try {
                $report = $consumer->consume($max, 0);
            } catch (ProcessKilled) {
                $consumerDeaths++;
                $source = $this->bench->successor($source);
                $deadReported = [...$deadReported, ...$dyingSource->deadLettered];

                return 1;
            }
            foreach ($report->of(ConsumeOutcome::DEAD_LETTERED) as $entry) {
                $deadReported[] = (string) $entry->messageId;
            }

            return $report->receivedCount();
        };

        $serial = 0;
        for ($step = 0; $step < self::STEPS; $step++) {
            $roll = $random->getInt(1, 100);
            if ($roll <= 35) {
                $messages = [];
                for ($n = $random->getInt(1, 3); $n > 0; $n--) {
                    $alias = 'M' . (++$serial);
                    $key = self::KEYS[$random->getInt(0, 3)];
                    $published[$alias] = $key;
                    $isPoison = $random->getInt(1, 10) === 1;
                    $failuresLeft[$alias] = $isPoison ? PHP_INT_MAX : [0, 0, 0, 1, 2][$random->getInt(0, 4)];
                    if ($isPoison) {
                        $poison[] = $alias;
                    }
                    $messages[] = TestMessages::make($alias, key: $key);
                }
                $publisher->publish(...$messages);
            } elseif ($roll <= 65) {
                $dying->dieAfterConfirm = $random->getInt(1, 100) <= 15;
                (new OutboxRelay($outbox, $dying, batchSize: $random->getInt(1, 3)))->relayPending($random->getInt(1, 5));
            } else {
                $consume($random->getInt(1, 4), 10);
            }
        }

        // Drain: relay everything, then consume until nothing comes.
        $dying->dieAfterConfirm = false;
        while ($outbox->fetchPending(1) !== []) {
            (new OutboxRelay($outbox, $dying))->relayPending(100);
        }
        $quiet = 0;
        while ($quiet < 2) {
            $quiet = $consume(100, 0) === 0 ? $quiet + 1 : 0;
        }

        $handled = $effects->all();
        $deadLetters = array_map(static fn(array $d): string => (string) $d['messageId'], $this->bench->deadLetters($source));
        $deadAliases = array_values(array_unique(array_map(TestMessages::aliased(...), $deadReported)));

        self::assertGreaterThan(10, count($published), 'the seed publishes enough');
        self::assertSame(array_values(array_unique($handled)), $handled, 'no effect twice');
        foreach (array_keys($published) as $alias) {
            self::assertTrue(in_array($alias, $handled, true) || in_array($alias, $deadAliases, true), "$alias was handled or dead-lettered");
        }
        foreach (array_unique(array_filter(self::KEYS)) as $key) {
            $expected = array_values(array_filter(array_keys($published, $key, true), static fn(string $a): bool => !in_array($a, $deadAliases, true)));
            $actual = array_values(array_filter($handled, static fn(string $a): bool => $published[$a] === $key && !in_array($a, $deadAliases, true)));
            self::assertSame($expected, $actual, "the order of key $key");
        }
        $sortedReported = array_map(TestMessages::aliased(...), $deadReported);
        $sortedLetters = array_map(TestMessages::aliased(...), $deadLetters);
        sort($sortedReported);
        sort($sortedLetters);
        self::assertSame($sortedReported, $sortedLetters, 'the dead letter queue holds what the reports dead-lettered');
        self::assertSame([], array_diff($poison, $deadAliases), 'every poison message is a dead letter');
        self::assertSame([], array_values(array_intersect($poison, $handled)), 'no poison message has an effect');
        self::$summary[$bench][$seed] = ['published' => count($published), 'handled' => count($handled), 'dead' => count($deadLetters), 'poison' => count($poison), 'relayDeaths' => $dying->deaths, 'consumerDeaths' => $consumerDeaths];
        self::assertGreaterThan(0, $dying->deaths, 'the seed kills a relay');
        self::assertGreaterThan(0, $consumerDeaths, 'the seed kills a consumer');
        self::assertNotSame([], $deadLetters, 'the seed dead-letters');
    }
}

/** The process of the relay dies after the broker confirmed a batch, before the outbox is marked. */
final class DyingTransport implements IMessageTransport
{
    public bool $dieAfterConfirm = false;

    public int $deaths = 0;

    public function __construct(private readonly IMessageTransport $inner)
    {
    }

    public function send(array $batch): TransportSendResult
    {
        $result = $this->inner->send($batch);
        if ($this->dieAfterConfirm) {
            $this->deaths++;
            throw new RuntimeException('process killed after the confirm');
        }

        return $result;
    }
}

/** Thrown by DyingSource: the consumer's process died. */
final class ProcessKilled extends RuntimeException
{
}

/** The process of the consumer dies before an acknowledgement (after the commit), with a probability. */
final class DyingSource implements IMessageSource
{
    /** @var list<string> */
    public array $deadLettered = [];

    public function __construct(private readonly IMessageSource $inner, private readonly Randomizer $random, private readonly int $dieBeforeAckPercent)
    {
    }

    public function receive(int $max, int $waitMs): array
    {
        return $this->inner->receive($max, $waitMs);
    }

    public function ack(ReceivedMessage $message): void
    {
        if ($this->dieBeforeAckPercent > 0 && $this->random->getInt(1, 100) <= $this->dieBeforeAckPercent) {
            throw new ProcessKilled('killed before the acknowledgement');
        }
        $this->inner->ack($message);
    }

    public function reject(ReceivedMessage $message, bool $requeue, string $reason): void
    {
        $this->inner->reject($message, $requeue, $reason);
        if (!$requeue) {
            $this->deadLettered[] = $message->isValid() ? $message->message()->messageId : 'invalid';
        }
    }
}
