<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Contract;

use Antevemus\ASpecification\Messaging\Testing\IMessagingBench;
use Antevemus\ASpecification\Messaging\Testing\MessagingContractTestCase;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\RabbitMqBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;

/**
 * The contract battery of the core (MessagingContractTestCase of antevemus/aspecification 2.15: every scenario
 * of the messaging hook, and the property sequence with its fixed seeds) over this package against a real
 * RabbitMQ. Skipped when the test broker is not reachable, which the CI turns into a failure with
 * --fail-on-skipped (see phpunit.xml.dist). The core runs the same battery against its in-memory reference.
 *
 * @version    1.0.1
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Contract
 */
final class RabbitMqContractTest extends MessagingContractTestCase
{
    protected function createBench(): IMessagingBench
    {
        TestBroker::require($this);

        return new RabbitMqBench();
    }
}
