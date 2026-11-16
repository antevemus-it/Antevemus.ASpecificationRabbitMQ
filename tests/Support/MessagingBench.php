<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;

/**
 * A broker under test, as the contract battery sees it: the pair of the core in memory (the reference) or the
 * pair of this package against a real RabbitMQ. The battery runs the same scenarios over both and expects the
 * same transcript.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
interface MessagingBench
{
    /** The transport the relay sends with. */
    public function transport(): IMessageTransport;

    /** A new consumer: every message sent after this call reaches it. */
    public function source(string $consumer): IMessageSource;

    /** The consumer behind $source died without answering what it held: the source of the one that replaces it. */
    public function successor(IMessageSource $source): IMessageSource;

    /** Stores a body that is not the wire form of a message (another producer), for every consumer. */
    public function storeRaw(string $body): void;

    /**
     * The dead letters of a consumer so far, in the order they were dead-lettered.
     *
     * @return list<array{messageId: ?string, reason: string, deliveryCount: int}>
     */
    public function deadLetters(IMessageSource $source): array;

    /** Removes what the bench created. */
    public function tearDown(): void;
}
