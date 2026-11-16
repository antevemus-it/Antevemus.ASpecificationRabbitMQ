<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ;

use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;

/**
 * RabbitMqTopology - The Exchanges and Queues of the Messaging Hook, Declared Idempotently
 *
 * The names RabbitMqTransport publishes to and RabbitMqSource reads from, and their declaration:
 *
 * - **The exchange** (`aspec.messages` by default): durable, of type `topic`; the routing key of a message is
 *   its name (`crm.lead_created.v1`), so a consumer binds by name or by pattern (`crm.#`, `*.lead_created.v1`).
 * - **A queue per consumer**: durable, a **quorum queue** (`x-queue-type: quorum`), bound to the exchange by
 *   one or more routing-key patterns. A quorum queue is replicated by Raft and counts the deliveries of each
 *   message (`x-delivery-count`), which the source needs to give MessageConsumer a delivery count that survives
 *   a dead consumer; a classic queue has only the `redelivered` flag.
 * - **The dead letters**: a durable `direct` exchange (`<exchange>.dead` by default) and, per consumer queue,
 *   a durable quorum queue `<queue>.dead` bound to it by the name of the consumer queue. The consumer queue
 *   names that exchange as its `x-dead-letter-exchange` (and itself as `x-dead-letter-routing-key`), so a
 *   message the broker dead-letters on its own (a policy with a delivery limit or a TTL) lands there too.
 *
 * Every declaration is idempotent: declaring what exists with the same arguments changes nothing, so a worker
 * may call declare() on every start. Declaring an existing queue with other arguments (a classic queue of the
 * same name) is refused by the broker (PRECONDITION_FAILED) and rethrown. Bindings are only added: a pattern
 * left out of a later declaration stays bound until it is removed by hand.
 *
 * <code>
 * $topology = new RabbitMqTopology();
 * $topology->declare($connector, ['agents' => ['underwriting.#', 'crm.lead_created.v1']]);
 * $topology->deadLetterQueueOf('agents'); // 'agents.dead'
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RabbitMqTopology
{
    /** Default name of the exchange the messages are published to. */
    public const DEFAULT_EXCHANGE = 'aspec.messages';

    /** Suffix of the default dead letter exchange and of each dead letter queue. */
    public const DEAD_SUFFIX = '.dead';

    /** An exchange or queue name: printable ASCII without spaces, not of the reserved `amq.` family. */
    public const NAME_PATTERN = '/^(?!amq\.)[A-Za-z0-9_.:-]{1,200}$/D';

    /** A binding pattern: dot-separated words of a message name, `*` (one word) or `#` (zero or more). */
    public const PATTERN_PATTERN = '/^(?:[a-z0-9_]+|\*|#)(?:\.(?:[a-z0-9_]+|\*|#))*$/D';

    private readonly string $deadLetterExchange;

    /**
     * @param string $exchange The topic exchange of the messages
     * @param string|null $deadLetterExchange The direct exchange of the dead letters (null = `<exchange>.dead`)
     * @throws InvalidArgumentException When a name breaks NAME_PATTERN, or both names are the same
     */
    public function __construct(private readonly string $exchange = self::DEFAULT_EXCHANGE, ?string $deadLetterExchange = null)
    {
        $this->deadLetterExchange = $deadLetterExchange ?? $exchange . self::DEAD_SUFFIX;
        self::assertName($exchange, 'exchange');
        self::assertName($this->deadLetterExchange, 'dead letter exchange');
        if ($exchange === $this->deadLetterExchange) {
            throw new InvalidArgumentException('The exchange of the messages and the one of the dead letters must be different.');
        }
    }

    /** The topic exchange the messages are published to. */
    public function exchange(): string
    {
        return $this->exchange;
    }

    /** The direct exchange of the dead letters. */
    public function deadLetterExchange(): string
    {
        return $this->deadLetterExchange;
    }

    /**
     * The dead letter queue of a consumer queue: `<queue>.dead`.
     *
     * @throws InvalidArgumentException When the queue name breaks NAME_PATTERN
     */
    public function deadLetterQueueOf(string $queue): string
    {
        self::assertName($queue, 'queue');

        return $queue . self::DEAD_SUFFIX;
    }

    /**
     * Declares the two exchanges on a channel.
     */
    public function declareExchanges(AMQPChannel $channel): void
    {
        $channel->exchange_declare($this->exchange, 'topic', false, true, false);
        $channel->exchange_declare($this->deadLetterExchange, 'direct', false, true, false);
    }

    /**
     * Declares the two exchanges, a consumer queue bound by the patterns, and its dead letter queue.
     *
     * @param list<string> $patterns Routing-key patterns (at least one)
     * @throws InvalidArgumentException When the queue or a pattern is invalid, or there is no pattern
     */
    public function declareQueue(AMQPChannel $channel, string $queue, array $patterns): void
    {
        $dead = $this->deadLetterQueueOf($queue);
        if ($patterns === []) {
            throw new InvalidArgumentException(sprintf('Queue "%s" needs at least one routing-key pattern.', $queue));
        }
        foreach ($patterns as $pattern) {
            if (!is_string($pattern) || preg_match(self::PATTERN_PATTERN, $pattern) !== 1) {
                throw new InvalidArgumentException(sprintf('A routing-key pattern of queue "%s" is dot-separated words of [a-z0-9_], "*" or "#".', $queue));
            }
        }
        $this->declareExchanges($channel);
        $channel->queue_declare($dead, false, true, false, false, false, new AMQPTable(['x-queue-type' => 'quorum']));
        $channel->queue_bind($dead, $this->deadLetterExchange, $queue);
        $channel->queue_declare($queue, false, true, false, false, false, new AMQPTable([
            'x-queue-type' => 'quorum',
            'x-dead-letter-exchange' => $this->deadLetterExchange,
            'x-dead-letter-routing-key' => $queue,
        ]));
        foreach (array_unique($patterns) as $pattern) {
            $channel->queue_bind($queue, $this->exchange, $pattern);
        }
    }

    /**
     * Opens a connection, declares the exchanges and every queue given, and closes it.
     *
     * @param array<string, list<string>> $queues Patterns by consumer queue (none = the exchanges only)
     * @throws InvalidArgumentException When a queue or a pattern is invalid
     * @throws Throwable What php-amqplib throws when the broker cannot be reached or refuses a declaration
     */
    public function declare(RabbitMqConnector $connector, array $queues = []): void
    {
        $connection = $connector->connect();
        try {
            $channel = $connection->channel();
            $this->declareExchanges($channel);
            foreach ($queues as $queue => $patterns) {
                $this->declareQueue($channel, (string) $queue, $patterns);
            }
        } finally {
            $connection->close();
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    private static function assertName(string $name, string $what): void
    {
        if (preg_match(self::NAME_PATTERN, $name) !== 1) {
            throw new InvalidArgumentException(sprintf('The %s name must be 1 to 200 characters of [A-Za-z0-9_.:-], not starting with "amq.".', $what));
        }
    }
}
