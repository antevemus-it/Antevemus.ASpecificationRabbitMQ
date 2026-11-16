<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Broker;

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * RabbitMqTransport against the broker: what a published message looks like in the queue, the publisher
 * confirms, the mandatory flag, the nacks, a missing exchange and a reconnection.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Broker
 */
final class RabbitMqTransportTest extends BrokerTestCase
{
    public function testAConfirmedMessageIsStoredWithItsPropertiesAndHeaders(): void
    {
        $queue = $this->queue();
        $keyed = TestMessages::make('A', key: 'quote:123', tenant: 'acme', payload: ['premium' => 1250.0]);
        $plain = TestMessages::make('B', name: 'livechat.message_received.v1');

        $result = (new RabbitMqTransport($this->connector, $this->topology))->send([$keyed, $plain]);

        self::assertSame([$keyed->messageId, $plain->messageId], $result->confirmedIds());
        self::assertSame($keyed->messageId, $result->brokerIdOf($keyed->messageId), 'the broker id is the message id');
        self::assertSame([], $result->failures());
        [$a, $b] = $this->broker->drain($queue);
        self::assertSame($keyed->toJson(), $a->getBody());
        self::assertSame('crm.lead_created.v1', $a->getRoutingKey());
        self::assertSame($this->topology->exchange(), $a->getExchange());
        self::assertSame([
            'application_headers' => ['aspec-ordering-key' => 'quote:123', 'aspec-tenant' => 'acme'],
            'content_type' => 'application/json',
            'delivery_mode' => 2,
            'message_id' => $keyed->messageId,
            'timestamp' => $keyed->occurredAt->getTimestamp(),
            'type' => 'crm.lead_created.v1',
        ], self::properties($a));
        self::assertSame('livechat.message_received.v1', $b->getRoutingKey());
        self::assertSame([], $b->get('application_headers')->getNativeData(), 'no tenant, no key: no header');
        self::assertSame($plain->messageId, $b->get('message_id'));
    }

    public function testAnUnroutableMessageFailsAndTheRoutedOnesOfTheBatchAreConfirmed(): void
    {
        $this->queue(['crm.#']);
        $routed = TestMessages::make('A');
        $lost = TestMessages::make('B', name: 'billing.invoice_overdue.v1', key: 'invoice:7');

        $result = (new RabbitMqTransport($this->connector, $this->topology))->send([$routed, $lost]);

        self::assertSame([$routed->messageId], $result->confirmedIds());
        self::assertSame([$lost->messageId => sprintf(
            'RabbitMQ returned message "billing.invoice_overdue.v1" (id %s, ordering key "invoice:7") as unroutable (312 NO_ROUTE): no queue is bound to routing key "billing.invoice_overdue.v1" of exchange "%s".',
            $lost->messageId,
            $this->topology->exchange()
        )], $result->failures());
    }

    public function testAMessageTheQueueRefusesIsNacked(): void
    {
        // A (classic) queue that holds one message and refuses the next (reject-publish): the broker nacks it.
        $queue = TestBroker::uniqueName('aspec.full');
        $this->broker->track($queue);
        $this->broker->channel()->queue_declare($queue, false, true, false, false, false, new AMQPTable(['x-queue-type' => 'classic', 'x-max-length' => 1, 'x-overflow' => 'reject-publish']));
        $this->broker->channel()->queue_bind($queue, $this->topology->exchange(), '#');
        $first = TestMessages::make('A', key: 'a');
        $second = TestMessages::make('B', key: 'b');

        $result = (new RabbitMqTransport($this->connector, $this->topology))->send([$first, $second]);

        self::assertSame([$first->messageId], $result->confirmedIds());
        self::assertSame([$second->messageId => sprintf(
            'RabbitMQ refused message "crm.lead_created.v1" (id %s, ordering key "b") (basic.nack): a queue it is routed to rejected it (x-overflow reject-publish) or could not store it.',
            $second->messageId
        )], $result->failures());
    }

    public function testAMissingExchangeFailsTheBatchAndTheNextSendReconnects(): void
    {
        $topology = new RabbitMqTopology(TestBroker::uniqueName('aspec.missing'));
        $this->broker->trackExchange($topology->exchange(), $topology->deadLetterExchange());
        $opened = 0;
        $connector = RabbitMqConnector::fromFactory(static function () use (&$opened): AbstractConnection {
            $opened++;

            return TestBroker::connector()->connect();
        }, 'amqp://test');
        $transport = new RabbitMqTransport($connector, $topology);
        $a = TestMessages::make('A');

        $failed = $transport->send([$a]);
        $queue = TestBroker::uniqueName('aspec.q');
        $this->broker->track($queue, $topology->deadLetterQueueOf($queue));
        $topology->declare(TestBroker::connector(), [$queue => ['#']]);
        $confirmed = $transport->send([$a]);
        $again = $transport->send([TestMessages::make('B')]);

        self::assertSame([], $failed->confirmedIds());
        self::assertStringStartsWith(sprintf('RabbitMQ at amqp://test did not confirm message "crm.lead_created.v1" (id %s) (AMQPProtocolChannelException: NOT_FOUND - no exchange \'%s\'', $a->messageId, $topology->exchange()), (string) $failed->reasonOf($a->messageId));
        self::assertSame([$a->messageId], $confirmed->confirmedIds());
        self::assertSame(1, count($again->confirmedIds()));
        self::assertSame(2, $opened, 'the failed connection was closed and a new one opened, then kept');
    }

    public function testAnEmptyBatchTalksToNobody(): void
    {
        $connector = RabbitMqConnector::fromFactory(static fn(): AbstractConnection => throw new \LogicException('connected'));

        $result = (new RabbitMqTransport($connector))->send([]);

        self::assertSame([], $result->confirmedIds());
        self::assertSame([], $result->failures());
    }

    public function testCloseOpensANewConnectionOnTheNextSend(): void
    {
        $this->queue();
        $opened = 0;
        $connector = RabbitMqConnector::fromFactory(static function () use (&$opened): AbstractConnection {
            $opened++;

            return TestBroker::connector()->connect();
        });
        $transport = new RabbitMqTransport($connector, $this->topology);

        $transport->send([TestMessages::make('A')]);
        $transport->send([TestMessages::make('B')]);
        $transport->close();
        $transport->close();
        $result = $transport->send([TestMessages::make('C')]);

        self::assertSame([TestMessages::idOf('C')], $result->confirmedIds());
        self::assertSame(2, $opened);
    }

    /**
     * @return array<string, mixed>
     */
    private static function properties(\PhpAmqpLib\Message\AMQPMessage $message): array
    {
        $properties = $message->get_properties();
        $properties['application_headers'] = $properties['application_headers']->getNativeData();
        ksort($properties);

        return $properties;
    }
}
