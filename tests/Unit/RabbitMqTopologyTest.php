<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Unit;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * RabbitMqTopology without a broker: the names, their rules, and the exact declarations it makes on a channel.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Unit
 */
final class RabbitMqTopologyTest extends TestCase
{
    /** @var list<list<mixed>> */
    private array $calls = [];

    public function testTheNames(): void
    {
        $default = new RabbitMqTopology();
        $custom = new RabbitMqTopology('shop.events', 'shop.graveyard');

        self::assertSame('aspec.messages', $default->exchange());
        self::assertSame('aspec.messages.dead', $default->deadLetterExchange());
        self::assertSame('agents.dead', $default->deadLetterQueueOf('agents'));
        self::assertSame('shop.events', $custom->exchange());
        self::assertSame('shop.graveyard', $custom->deadLetterExchange());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function badNames(): array
    {
        return [
            'empty' => [''],
            'reserved' => ['amq.topic'],
            'a space' => ['my queue'],
            'a slash' => ['a/b'],
            'unicode' => ['fila-ç'],
            '201 characters' => [str_repeat('q', 201)],
        ];
    }

    #[DataProvider('badNames')]
    public function testABadNameIsRefusedEverywhere(string $name): void
    {
        foreach ([fn() => new RabbitMqTopology($name), fn() => new RabbitMqTopology('ok', $name), fn() => (new RabbitMqTopology())->deadLetterQueueOf($name)] as $i => $build) {
            try {
                $build();
                self::fail("case $i: no exception");
            } catch (InvalidArgumentException $e) {
                self::assertMatchesRegularExpression('/^The (exchange|dead letter exchange|queue) name must be 1 to 200 characters of \[A-Za-z0-9_.:-\], not starting with "amq\."\.$/', $e->getMessage());
            }
        }
    }

    public function testTheLongestNameAndEveryAllowedCharacterAreAccepted(): void
    {
        self::assertSame(str_repeat('q', 200) . '.dead', (new RabbitMqTopology())->deadLetterQueueOf(str_repeat('q', 200)));
        self::assertSame('Aa0_.:-x.dead', (new RabbitMqTopology())->deadLetterQueueOf('Aa0_.:-x'));
        self::assertSame('amqx', (new RabbitMqTopology('amqx'))->exchange());
    }

    public function testTheTwoExchangesMustDiffer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The exchange of the messages and the one of the dead letters must be different.');
        new RabbitMqTopology('same', 'same');
    }

    public function testTheDeclarationsOfAConsumerQueue(): void
    {
        (new RabbitMqTopology('shop'))->declareQueue($this->channel(), 'agents', ['crm.#', '*.lead_created.v1', 'crm.#']);

        self::assertSame([
            ['exchange_declare', 'shop', 'topic', false, true, false],
            ['exchange_declare', 'shop.dead', 'direct', false, true, false],
            ['queue_declare', 'agents.dead', false, true, false, false, false, ['x-queue-type' => 'quorum']],
            ['queue_bind', 'agents.dead', 'shop.dead', 'agents'],
            ['queue_declare', 'agents', false, true, false, false, false, ['x-queue-type' => 'quorum', 'x-dead-letter-exchange' => 'shop.dead', 'x-dead-letter-routing-key' => 'agents']],
            ['queue_bind', 'agents', 'shop', 'crm.#'],
            ['queue_bind', 'agents', 'shop', '*.lead_created.v1'],
        ], $this->calls);
    }

    /**
     * @return array<string, array{list<mixed>}>
     */
    public static function badPatterns(): array
    {
        return [
            'none' => [[]],
            'empty' => [['']],
            'two dots' => [['crm..x']],
            'a capital' => [['Crm.x']],
            'a wildcard inside a word' => [['crm.#x']],
            'a leading dot' => [['.crm']],
            'a trailing dot' => [['crm.']],
            'a trailing newline' => [["crm.x\n"]],
            'not a string' => [[5]],
        ];
    }

    /**
     * @param list<mixed> $patterns
     */
    #[DataProvider('badPatterns')]
    public function testABadPatternIsRefusedBeforeAnythingIsDeclared(array $patterns): void
    {
        try {
            (new RabbitMqTopology())->declareQueue($this->channel(), 'agents', $patterns);
            self::fail('no exception');
        } catch (InvalidArgumentException $e) {
            self::assertContains($e->getMessage(), [
                'Queue "agents" needs at least one routing-key pattern.',
                'A routing-key pattern of queue "agents" is dot-separated words of [a-z0-9_], "*" or "#".',
            ]);
        }
        self::assertSame([], $this->calls);
    }

    public function testABadQueueIsRefusedBeforeAnythingIsDeclared(): void
    {
        try {
            (new RabbitMqTopology())->declareQueue($this->channel(), 'amq.agents', ['#']);
            self::fail('no exception');
        } catch (InvalidArgumentException) {
            self::assertSame([], $this->calls);
        }
    }

    public function testTheAcceptedPatterns(): void
    {
        (new RabbitMqTopology('x'))->declareQueue($this->channel(), 'q', ['#', '*', 'crm.*.v1', 'a_1.b2.#', '*.#.*']);

        self::assertSame(['#', '*', 'crm.*.v1', 'a_1.b2.#', '*.#.*'], array_column(array_values(array_filter($this->calls, static fn(array $c): bool => $c[0] === 'queue_bind' && $c[1] === 'q')), 3));
    }

    public function testDeclareClosesItsConnectionEvenWhenADeclarationFails(): void
    {
        $channel = $this->channel();
        $channel->method('queue_declare')->willThrowException(new RuntimeException('PRECONDITION_FAILED'));
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('channel')->willReturn($channel);
        $connection->expects(self::once())->method('close');

        $this->expectExceptionMessage('PRECONDITION_FAILED');
        (new RabbitMqTopology('x'))->declare(RabbitMqConnector::fromFactory(static fn(): AbstractConnection => $connection), ['q' => ['#']]);
    }

    public function testDeclareWithoutQueuesDeclaresTheExchangesOnly(): void
    {
        $channel = $this->channel();
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('channel')->willReturn($channel);
        $connection->expects(self::once())->method('close');

        (new RabbitMqTopology('x'))->declare(RabbitMqConnector::fromFactory(static fn(): AbstractConnection => $connection));

        self::assertSame([['exchange_declare', 'x', 'topic', false, true, false], ['exchange_declare', 'x.dead', 'direct', false, true, false]], $this->calls);
    }

    public function testDeclareTakesTheQueueNamesAsStrings(): void
    {
        $channel = $this->channel();
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('channel')->willReturn($channel);

        (new RabbitMqTopology('x'))->declare(RabbitMqConnector::fromFactory(static fn(): AbstractConnection => $connection), [42 => ['#']]);

        self::assertContains(['queue_bind', '42', 'x', '#'], $this->calls);
    }

    /** A channel that records the declarations made on it. */
    private function channel(): AMQPChannel
    {
        $channel = $this->createMock(AMQPChannel::class);
        foreach (['exchange_declare', 'queue_declare', 'queue_bind'] as $method) {
            $channel->method($method)->willReturnCallback(function (mixed ...$arguments) use ($method): null {
                $arguments = array_map(static fn(mixed $a): mixed => $a instanceof AMQPTable ? $a->getNativeData() : $a, $arguments);
                $this->calls[] = [$method, ...self::trim($method, $arguments)];

                return null;
            });
        }

        return $channel;
    }

    /**
     * The arguments the topology passes (the mock also receives the defaults of the ones it leaves out).
     *
     * @param list<mixed> $arguments
     * @return list<mixed>
     */
    private static function trim(string $method, array $arguments): array
    {
        return array_slice($arguments, 0, ['exchange_declare' => 5, 'queue_declare' => 7, 'queue_bind' => 3][$method]);
    }
}
