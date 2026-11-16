<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Contract;

use Antevemus\ASpecificationRabbitMQ\Tests\Support\InMemoryBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\MessagingBench;

/**
 * The contract battery over the reference: the in-memory transport and source of the core.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Contract
 */
final class InMemoryContractTest extends MessagingContractTestCase
{
    protected function createBench(): MessagingBench
    {
        return new InMemoryBench();
    }
}
