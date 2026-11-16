<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Contract;

use Antevemus\ASpecificationRabbitMQ\Tests\Support\MessagingBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\RabbitMqBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;

/**
 * The contract battery over this package against a real RabbitMQ (skipped when the test broker is not
 * reachable; see phpunit.xml.dist).
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Contract
 */
final class RabbitMqContractTest extends MessagingContractTestCase
{
    protected function createBench(): MessagingBench
    {
        TestBroker::require($this);

        return new RabbitMqBench();
    }

    protected function tearDown(): void
    {
        if (isset($this->bench)) {
            parent::tearDown();
        }
    }
}
