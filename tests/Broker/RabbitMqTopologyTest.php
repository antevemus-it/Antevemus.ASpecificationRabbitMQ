<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Broker;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * RabbitMqTopology against the broker: idempotent declarations, routing by pattern, the dead letter arguments of
 * a consumer queue, a conflicting queue, and the URL of a connector.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Broker
 */
final class RabbitMqTopologyTest extends BrokerTestCase
{
    public function testDeclaringTwiceChangesNothingAndPatternsRoute(): void
    {
        $topology = $this->broker->topology();
        $crm = TestBroker::uniqueName('aspec.crm');
        $leads = TestBroker::uniqueName('aspec.leads');
        $this->broker->track($crm, "$crm.dead", $leads, "$leads.dead");
        $queues = [$crm => ['crm.#', 'crm.#'], $leads => ['*.lead_created.v1']];
        $opened = [];
        $connector = RabbitMqConnector::fromFactory(static function () use (&$opened): AbstractConnection {
            return $opened[] = TestBroker::connector()->connect();
        });

        $topology->declare($connector, $queues);
        $topology->declare($connector, $queues);
        $result = (new RabbitMqTransport($this->connector, $topology))->send([
            TestMessages::make('A', name: 'crm.lead_created.v1'),
            TestMessages::make('B', name: 'crm.deal_won.v1'),
            TestMessages::make('C', name: 'web.lead_created.v1'),
        ]);

        self::assertCount(2, $opened);
        self::assertSame([false, false], array_map(static fn(AbstractConnection $c): bool => $c->isConnected(), $opened), 'declare() closes its connection');
        self::assertCount(3, $result->confirmedIds());
        self::assertSame(['A', 'B'], self::aliases($this->broker->drain($crm)));
        self::assertSame(['A', 'C'], self::aliases($this->broker->drain($leads)));
        self::assertSame(0, $this->broker->readyCount("$crm.dead"));
    }

    public function testDeclareWithoutQueuesDeclaresTheExchanges(): void
    {
        $topology = $this->broker->topology();

        $topology->declare($this->connector);

        $channel = $this->broker->channel();
        self::assertNull($channel->exchange_declare($topology->exchange(), 'topic', true));
        self::assertNull($channel->exchange_declare($topology->deadLetterExchange(), 'direct', true));
    }

    public function testAMessageTheBrokerDeadLettersOnItsOwnLandsInTheDeadLetterQueue(): void
    {
        $queue = $this->queue();
        (new RabbitMqTransport($this->connector, $this->topology))->send([TestMessages::make('A')]);
        $channel = $this->broker->channel();

        // The broker's own dead-lettering (here a nack without requeue; a delivery limit or a TTL do the same).
        $delivery = $channel->basic_get($queue);
        $channel->basic_nack($delivery->getDeliveryTag(), false, false);
        $this->broker->waitForReady($this->topology->deadLetterQueueOf($queue), 1);

        self::assertSame(['A'], self::aliases($this->broker->drain($this->topology->deadLetterQueueOf($queue))));
    }

    public function testAQueueOfTheSameNameWithOtherArgumentsIsRefused(): void
    {
        $queue = TestBroker::uniqueName('aspec.classic');
        $this->broker->track($queue, "$queue.dead");
        $this->broker->channel()->queue_declare($queue, false, true, false, false);

        $this->expectException(AMQPProtocolChannelException::class);
        $this->expectExceptionMessageMatches('/PRECONDITION_FAILED/');
        $this->topology->declare($this->connector, [$queue => ['#']]);
    }

    public function testAConnectorFromAUrlLogsIn(): void
    {
        $connection = RabbitMqConnector::fromUrl(TestBroker::url())->connect();
        self::assertTrue($connection->isConnected());
        $connection->close();

        $user = rawurlencode((string) (getenv('RABBITMQ_USER') ?: 'guest'));
        $wrongUser = str_replace('://' . $user . ':', '://nobody:', TestBroker::url());
        $wrongPassword = (string) preg_replace('/:[^:@\/]*@/', ':wrong-password@', TestBroker::url(), 1);
        foreach ([$wrongUser, $wrongPassword] as $url) {
            try {
                RabbitMqConnector::fromUrl($url)->connect();
                self::fail('logged in with wrong credentials');
            } catch (\Throwable $e) {
                self::assertStringContainsString('ACCESS_REFUSED', $e->getMessage());
            }
        }
    }

    /**
     * @param list<AMQPMessage> $messages
     * @return list<string>
     */
    private static function aliases(array $messages): array
    {
        return array_map(static fn(AMQPMessage $m): string => json_decode($m->getBody(), true)['payload']['alias'], $messages);
    }
}
