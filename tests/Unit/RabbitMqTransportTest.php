<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ\Tests\Unit;

use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\ScriptedBroker;
use Antevemus\ASpecificationRabbitMQ\Tests\Support\TestMessages;
use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;

/**
 * RabbitMqTransport without a broker: the answers of a scripted broker mapped to the result, the publish
 * itself, the connection kept and replaced, and an unreachable broker.
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ\Tests
 * @subpackage Unit
 */
final class RabbitMqTransportTest extends TestCase
{
    private ScriptedBroker $broker;

    protected function setUp(): void
    {
        $this->broker = new ScriptedBroker(fn(): array => [$this->createMock(AbstractConnection::class), $this->createMock(AMQPChannel::class)]);
    }

    public function testTheConfirmTimeoutMustBePositive(): void
    {
        foreach ([0.0, -1.0, NAN] as $timeout) {
            try {
                new RabbitMqTransport(new RabbitMqConnector(), confirmTimeout: $timeout);
                self::fail("$timeout: no exception");
            } catch (InvalidArgumentException $e) {
                self::assertStringStartsWith('The confirm timeout must be a positive number of seconds, ', $e->getMessage());
            }
        }
        self::assertInstanceOf(RabbitMqTransport::class, new RabbitMqTransport(new RabbitMqConnector(), confirmTimeout: 0.001));
    }

    public function testEachAnswerOfTheBrokerBecomesTheResultOfItsMessage(): void
    {
        $a = TestMessages::make('A', key: 'a');
        $b = TestMessages::make('B', key: 'b');
        $c = TestMessages::make('C', name: 'billing.invoice_overdue.v1');
        $this->broker->answer = static fn(AMQPMessage $m): string => ['A' => 'ack', 'B' => 'nack', 'C' => 'return'][json_decode($m->getBody(), true)['payload']['alias']];

        $result = (new RabbitMqTransport($this->broker->connector(), new RabbitMqTopology('shop'), 2.5))->send([$a, $b, $c]);

        self::assertSame([$a->messageId], $result->confirmedIds());
        self::assertSame($a->messageId, $result->brokerIdOf($a->messageId));
        self::assertSame([
            $b->messageId => sprintf('RabbitMQ refused message "crm.lead_created.v1" (id %s, ordering key "b") (basic.nack): a queue it is routed to rejected it (x-overflow reject-publish) or could not store it.', $b->messageId),
            $c->messageId => sprintf('RabbitMQ returned message "billing.invoice_overdue.v1" (id %s) as unroutable (312 NO_ROUTE): no queue is bound to routing key "billing.invoice_overdue.v1" of exchange "shop".', $c->messageId),
        ], $result->failures());
        self::assertSame([['confirm_select'], ['wait', 2.5]], $this->broker->calls, 'confirm mode, then one wait for the whole batch, with the timeout');
    }

    public function testWhatIsPublished(): void
    {
        $keyed = TestMessages::make('A', key: 'quote:1', tenant: 'acme');
        $plain = TestMessages::make('B', name: 'livechat.message_received.v1');

        (new RabbitMqTransport($this->broker->connector(), new RabbitMqTopology('shop')))->send([$keyed, $plain]);

        [[$first, $exchange, $routingKey, $mandatory], [$second, , $secondKey]] = $this->broker->published;
        self::assertSame(['shop', 'crm.lead_created.v1', true], [$exchange, $routingKey, $mandatory]);
        self::assertSame('livechat.message_received.v1', $secondKey);
        self::assertSame($keyed->toJson(), $first->getBody());
        self::assertSame('application/json', $first->get('content_type'));
        self::assertSame(AMQPMessage::DELIVERY_MODE_PERSISTENT, $first->get('delivery_mode'));
        self::assertSame($keyed->messageId, $first->get('message_id'));
        self::assertSame('crm.lead_created.v1', $first->get('type'));
        self::assertSame($keyed->occurredAt->getTimestamp(), $first->get('timestamp'));
        self::assertSame(['aspec-tenant' => 'acme', 'aspec-ordering-key' => 'quote:1'], $first->get('application_headers')->getNativeData());
        self::assertSame([], $second->get('application_headers')->getNativeData());
    }

    public function testAMessageWithoutAnAnswerFailsWithTheErrorOfTheWait(): void
    {
        $a = TestMessages::make('A');
        $b = TestMessages::make('B');
        $this->broker->answer = static fn(AMQPMessage $m): string => $m->get('message_id') === $a->messageId ? 'ack' : 'silent';
        $this->broker->waitError = new AMQPTimeoutException('The connection timed out after 5 sec while awaiting incoming data', 5);
        $transport = new RabbitMqTransport($this->broker->connector());

        $result = $transport->send([$a, $b]);

        self::assertSame([$a->messageId], $result->confirmedIds());
        self::assertSame([$b->messageId => sprintf('RabbitMQ at amqp://scripted did not confirm message "crm.lead_created.v1" (id %s) (AMQPTimeoutException: The connection timed out after 5 sec while awaiting incoming data).', $b->messageId)], $result->failures());
        self::assertSame(1, $this->broker->closes, 'the connection whose confirms are uncertain is closed');
    }

    public function testTheConnectionIsKeptUntilItFails(): void
    {
        $transport = new RabbitMqTransport($this->broker->connector());

        $transport->send([TestMessages::make('A')]);
        $transport->send([TestMessages::make('B')]);
        $this->broker->answer = static fn(): string => 'silent';
        $this->broker->waitError = new AMQPTimeoutException('late');
        $transport->send([TestMessages::make('C')]);
        $this->broker->answer = static fn(): string => 'ack';
        $last = $transport->send([TestMessages::make('D')]);

        self::assertSame(2, $this->broker->connections);
        self::assertSame(1, $this->broker->closes);
        self::assertSame([TestMessages::idOf('D')], $last->confirmedIds(), 'the answers of an earlier send are forgotten');
    }

    public function testTheAnswersOfAnEarlierSendAreForgotten(): void
    {
        $transport = new RabbitMqTransport($this->broker->connector());
        $a = TestMessages::make('A');
        $transport->send([$a]);
        $this->broker->answer = static fn(): string => 'nack';
        $nacked = $transport->send([$a]);
        $this->broker->answer = static fn(): string => 'return';
        $returned = $transport->send([$a]);
        $this->broker->answer = static fn(): string => 'ack';
        $acked = $transport->send([$a]);

        self::assertSame([], $nacked->confirmedIds());
        self::assertStringContainsString('basic.nack', (string) $nacked->reasonOf($a->messageId));
        self::assertStringContainsString('NO_ROUTE', (string) $returned->reasonOf($a->messageId));
        self::assertSame([$a->messageId], $acked->confirmedIds());
    }

    public function testABrokerThatCannotBeReachedFailsEveryMessageWithoutThrowing(): void
    {
        $this->broker->connectError = new \RuntimeException('Connection refused');
        $a = TestMessages::make('A');

        $result = (new RabbitMqTransport($this->broker->connector()))->send([$a]);

        self::assertSame([$a->messageId => sprintf('RabbitMQ at amqp://scripted did not confirm message "crm.lead_created.v1" (id %s) (RuntimeException: Connection refused).', $a->messageId)], $result->failures());
        self::assertSame(0, $this->broker->closes);
    }

    public function testARealConnectionRefusedFailsEveryMessage(): void
    {
        $a = TestMessages::make('A');
        $b = TestMessages::make('B');

        $result = (new RabbitMqTransport(new RabbitMqConnector('127.0.0.1', 1)))->send([$a, $b]);

        self::assertSame([], $result->confirmedIds());
        self::assertStringStartsWith(sprintf('RabbitMQ at amqp://127.0.0.1:1/ did not confirm message "crm.lead_created.v1" (id %s) (AMQPIOException: ', $b->messageId), (string) $result->reasonOf($b->messageId));
    }

    public function testAnEmptyBatchConnectsToNothing(): void
    {
        $result = (new RabbitMqTransport($this->broker->connector()))->send([]);

        self::assertSame([], $result->confirmedIds());
        self::assertSame([], $result->failures());
        self::assertSame(0, $this->broker->connections);
    }

    public function testCloseWithoutAConnectionDoesNothing(): void
    {
        $transport = new RabbitMqTransport($this->broker->connector());
        $transport->close();
        $transport->send([TestMessages::make('A')]);
        $transport->close();

        self::assertSame(1, $this->broker->closes);
        self::assertSame(1, $this->broker->connections);
    }
}
