<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecification\Messaging\ConsumeEntry;
use Antevemus\ASpecification\Messaging\ConsumeReport;
use Antevemus\ASpecification\Messaging\Message;
use DateTimeImmutable;

/**
 * Messages with an alias: the id of each one is derived from its alias (a valid ULID), and a transcript names
 * each message by its alias, so the expected transcripts of the battery are short and the same for every bench.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class TestMessages
{
    private const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

    /** @var array<string, string> id => alias */
    private static array $aliases = [];

    /**
     * A message whose payload carries its alias (`alias`), and the rest of the payload given.
     *
     * @param array<string, mixed> $payload
     * @param array<string, string> $headers
     */
    public static function make(string $alias, string $name = 'crm.lead_created.v1', ?string $key = null, ?string $tenant = null, array $payload = [], array $headers = []): Message
    {
        return new Message($name, ['alias' => $alias] + $payload, $key, $tenant, $headers, self::idOf($alias), new DateTimeImmutable('2026-11-15T12:00:00.123456Z'));
    }

    /** The ULID of an alias. */
    public static function idOf(string $alias): string
    {
        $hash = hash('sha256', $alias);
        $id = '01JDRMQ';
        for ($i = 0; $i < 19; $i++) {
            $id .= self::ALPHABET[hexdec($hash[$i * 2] . $hash[$i * 2 + 1]) % 32];
        }
        self::$aliases[$id] = $alias;

        return $id;
    }

    /** A text with every known id replaced by its alias. */
    public static function aliased(?string $text): string
    {
        return strtr((string) $text, self::$aliases);
    }

    /**
     * One line per delivery: `<alias> <outcome> <delivery count>` (`invalid` for an invalid delivery).
     *
     * @return list<string>
     */
    public static function transcript(ConsumeReport $report): array
    {
        return array_map(
            static fn(ConsumeEntry $e): string => sprintf('%s %s %d', $e->messageId === null ? 'invalid' : self::aliased($e->messageId), $e->outcome->value, $e->deliveryCount),
            $report->entries()
        );
    }

    /**
     * The reasons of a report, aliased.
     *
     * @return list<?string>
     */
    public static function reasons(ConsumeReport $report): array
    {
        return array_map(static fn(ConsumeEntry $e): ?string => $e->reason === null ? null : self::aliased($e->reason), $report->entries());
    }
}
