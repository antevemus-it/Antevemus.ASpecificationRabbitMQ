<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Support;

use Antevemus\ASpecification\StateMachine\Persistence\InMemory\IInMemoryTransactional;

/**
 * The effects of the handlers of a test, rolled back with the inbox by the InMemoryTransactionManager of the core:
 * an effect recorded by a handler that then failed disappears, as a row of a rolled back transaction does.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Support
 */
final class EffectLog implements IInMemoryTransactional
{
    /** @var list<string> */
    private array $effects = [];

    public function record(string $effect): void
    {
        $this->effects[] = $effect;
    }

    /** @return list<string> */
    public function all(): array
    {
        return $this->effects;
    }

    public function exportState(): array
    {
        return ['effects' => $this->effects];
    }

    public function importState(array $state): void
    {
        $this->effects = $state['effects'];
    }
}
