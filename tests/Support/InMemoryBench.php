<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecification\Messaging\IMessageSource;
use Antevemus\ASpecification\Messaging\IMessageTransport;
use Antevemus\ASpecification\Messaging\InMemory\InMemoryMessageSource;
use Antevemus\ASpecification\Messaging\InMemory\InMemoryMessageTransport;
use LogicException;

/**
 * The reference: InMemoryMessageTransport and InMemoryMessageSource of the core, unchanged.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class InMemoryBench implements MessagingBench
{
    private readonly InMemoryMessageTransport $transport;

    public function __construct()
    {
        $this->transport = new InMemoryMessageTransport();
    }

    public function transport(): IMessageTransport
    {
        return $this->transport;
    }

    public function source(string $consumer): IMessageSource
    {
        return new InMemoryMessageSource($this->transport);
    }

    public function successor(IMessageSource $source): IMessageSource
    {
        $source instanceof InMemoryMessageSource || throw new LogicException('not a source of this bench');
        $source->recoverInFlight();

        return $source;
    }

    public function storeRaw(string $body): void
    {
        $this->transport->storeRaw($body);
    }

    public function deadLetters(IMessageSource $source): array
    {
        $source instanceof InMemoryMessageSource || throw new LogicException('not a source of this bench');

        return array_map(
            static fn(array $dead): array => ['messageId' => $dead['messageId'], 'reason' => $dead['reason'], 'deliveryCount' => $dead['deliveryCount']],
            $source->deadLetters()
        );
    }

    public function tearDown(): void
    {
    }
}
