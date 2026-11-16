<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Internal;

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;

/**
 * Amqp - What the Transport and the Source Share
 *
 * The names of the headers this package writes, the reading of a header of a delivery, the closing of a
 * connection that may already be broken, and the text of a failure of php-amqplib in a reason: the short class
 * name and the message of the exception, which names the broker, the exchange or the queue and never a body,
 * truncated to MAX_REASON_LENGTH characters (the limit of TransportSendResult and of a dead letter reason).
 *
 * @internal
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ
 * @subpackage Internal
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class Amqp
{
    /** Header with the tenant of the message (absent when it has none). */
    public const HEADER_TENANT = 'aspec-tenant';

    /** Header with the ordering key of the message (absent when it has none). */
    public const HEADER_ORDERING_KEY = 'aspec-ordering-key';

    /** Header of a dead letter: why the consumer rejected it. */
    public const HEADER_DEAD_REASON = 'aspec-dead-reason';

    /** Header of a dead letter: the delivery count when it was rejected. */
    public const HEADER_DEAD_DELIVERY_COUNT = 'aspec-dead-delivery-count';

    /** Header of a dead letter: the consumer queue it was rejected from. */
    public const HEADER_DEAD_QUEUE = 'aspec-dead-queue';

    /** Header the broker sets on a redelivery from a quorum queue: deliveries before this one. */
    public const HEADER_DELIVERY_COUNT = 'x-delivery-count';

    /** Longest reason recorded. */
    public const MAX_REASON_LENGTH = 2000;

    private function __construct()
    {
    }

    /**
     * The headers of a delivery (the `application_headers` table), empty when it has none.
     *
     * @return array<string, mixed>
     */
    public static function headersOf(AMQPMessage $message): array
    {
        $headers = $message->has('application_headers') ? $message->get('application_headers') : null;

        return $headers instanceof AMQPTable ? $headers->getNativeData() : [];
    }

    /** A failure of php-amqplib in a reason: `ShortClass: message`. */
    public static function describe(Throwable $error): string
    {
        $class = $error::class;
        $separator = strrpos($class, '\\');
        $short = $separator === false ? $class : substr($class, $separator + 1);
        $message = trim($error->getMessage());

        return $message === '' ? $short : $short . ': ' . $message;
    }

    /** True for a number above zero; false for zero, a negative number and NAN. */
    public static function isPositive(float $number): bool
    {
        return $number > 0.0;
    }

    /** A reason cut to MAX_REASON_LENGTH characters. */
    public static function truncate(string $reason): string
    {
        return mb_substr($reason, 0, self::MAX_REASON_LENGTH);
    }

    /** Closes a connection, ignoring what a broken one throws. */
    public static function closeQuietly(AbstractConnection $connection): void
    {
        try {
            $connection->close();
        } catch (Throwable) {
            // A connection already broken has nothing left to close.
        }
    }
}
