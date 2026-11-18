<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Broker;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\RabbitMqBench;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;
use PHPUnit\Framework\TestCase;

/**
 * A test against the real broker: skipped when it is not reachable; a bench of its own (a unique topology,
 * removed at the end).
 *
 * @version    1.0.1
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Broker
 */
abstract class BrokerTestCase extends TestCase
{
    protected RabbitMqBench $bench;

    protected TestBroker $broker;

    protected RabbitMqTopology $topology;

    protected RabbitMqConnector $connector;

    protected function setUp(): void
    {
        TestBroker::require($this);
        $this->bench = new RabbitMqBench();
        $this->broker = $this->bench->broker;
        $this->topology = $this->bench->topology;
        $this->connector = TestBroker::connector();
    }

    protected function tearDown(): void
    {
        if (isset($this->bench)) {
            $this->bench->close();
        }
    }

    /** A new consumer queue bound by the patterns, removed at the end. */
    protected function queue(array $patterns = ['#']): string
    {
        return $this->broker->declareQueue($this->topology, TestBroker::uniqueName('aspec.q'), $patterns);
    }
}
