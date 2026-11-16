# API Reference

Four classes, all `final`, in `Antevemus\ASpecificationRabbitMQ`. The transport and the source implement the two interfaces of the core's messaging hook (`IMessageTransport`, `IMessageSource`) and are used through the core's `OutboxRelay` and `MessageConsumer`; nothing else of the core is replaced.

## RabbitMqConnector

Opens connections to one broker (a factory: the transport and the source each open their own, and open a new one after a failure).

| Member | What |
| :--- | :--- |
| `new RabbitMqConnector(string $host = '127.0.0.1', int $port = 5672, string $user = 'guest', string $password = 'guest', string $vhost = '/', float $connectionTimeout = 3.0, float $readWriteTimeout = 10.0, int $heartbeat = 0)` | An `AMQPStreamConnection` of php-amqplib per `connect()`. The port is 1 to 65535, the timeouts positive, the heartbeat zero or more and the read/write timeout at least twice it (`InvalidArgumentException` otherwise). |
| `RabbitMqConnector::fromUrl(string $url)` | From `amqp://user:password@host:port/vhost`, percent-decoded (`%2F` is `/`); port 5672, user and password `guest` and vhost `/` when absent. `amqps` and other schemes, a query or a fragment are refused, without repeating the URL. |
| `RabbitMqConnector::fromFactory(Closure $factory, string $description = 'amqp (custom connection)')` | Runs the closure (`fn(): AbstractConnection`) for each connection: TLS (`AMQPSSLConnection`), a list of hosts, any option of php-amqplib. |
| `connect(): AbstractConnection` | A new connection (what php-amqplib throws when the broker cannot be reached or refuses the login). |
| `describe(): string` | The broker as a reason names it: `amqp://host:port/vhost`, never the user or the password. |

The password is marked `#[SensitiveParameter]` (also the URL of `fromUrl()`), so a stack trace does not show it.

<!-- tested: connector -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;

$connector = RabbitMqConnector::fromUrl('amqp://app:s3cret@rabbit.internal:5673/shop');
$connector->describe(); // 'amqp://rabbit.internal:5673/shop'
RabbitMqConnector::fromUrl('amqps://app:s3cret@rabbit.internal'); // throws InvalidArgumentException: A RabbitMQ URL is amqp://user:password@host:port/vhost (the URL given is not shown, it may hold a password).

// A broker that cannot be reached: every message fails, with a reason that names it, never its payload.
$message = new Message('crm.lead_created.v1', ['cpf' => '529.982.247-25']);
$result = (new RabbitMqTransport(new RabbitMqConnector('127.0.0.1', 1)))->send([$message]);
$result->isConfirmed($message->messageId); // false
str_contains((string) $result->reasonOf($message->messageId), '529.982.247-25'); // false
```

## RabbitMqTopology

| Member | What |
| :--- | :--- |
| `new RabbitMqTopology(string $exchange = 'aspec.messages', ?string $deadLetterExchange = null)` | The topic exchange of the messages and the direct exchange of the dead letters (`<exchange>.dead` by default). A name is 1 to 200 characters of `[A-Za-z0-9_.:-]`, not starting with `amq.`; the two must differ. |
| `exchange()`, `deadLetterExchange()` | The two names. |
| `deadLetterQueueOf(string $queue): string` | `<queue>.dead`. |
| `declare(RabbitMqConnector $connector, array $queues = [])` | Opens a connection, declares the exchanges and each queue of `[queue => [pattern, ...]]` with its dead letter queue, and closes it. Idempotent. |
| `declareExchanges(AMQPChannel $channel)`, `declareQueue(AMQPChannel $channel, string $queue, array $patterns)` | The same on a channel of the application. |
| `RabbitMqTopology::DEFAULT_EXCHANGE`, `DEAD_SUFFIX`, `NAME_PATTERN`, `PATTERN_PATTERN` | The default exchange, the `.dead` suffix, the rule of a name and the rule of a pattern (dot-separated words of `[a-z0-9_]`, `*` or `#`). |

## RabbitMqTransport

| Member | What |
| :--- | :--- |
| `new RabbitMqTransport(RabbitMqConnector $connector, RabbitMqTopology $topology = new RabbitMqTopology(), float $confirmTimeout = 5.0)` | The `IMessageTransport` (see [Topology and publisher confirms](../concepts/topology-and-confirms.md)). The topology is not declared by the transport: declare it on start. |
| `send(array $batch): TransportSendResult` | Publishes the batch with confirms and `mandatory`; confirmed messages carry their message id as the broker id; returned, nacked and unanswered ones fail with a reason. Never throws. |
| `close()` | Closes the connection; the next send opens a new one. |

## RabbitMqSource

| Member | What |
| :--- | :--- |
| `new RabbitMqSource(RabbitMqConnector $connector, string $queue, RabbitMqTopology $topology = new RabbitMqTopology(), int $pollIntervalMs = 25, float $confirmTimeout = 5.0)` | The `IMessageSource` of one consumer queue (see [Delivery, ordering and dead letters](../concepts/delivery-and-ordering.md)). The topology gives the dead letter exchange; the timeout is the wait for the confirm of a dead letter. |
| `receive(int $max, int $waitMs): array` | Up to `$max` deliveries (`ReceivedMessage`), the ones given back first; an invalid body is an invalid delivery. The broker id is `<connection>.<delivery tag>` (`1.7`). |
| `ack()`, `reject(…, bool $requeue, string $reason)` | `basic.ack`; a requeue kept by the source; a dead letter with the reason, then `basic.ack`. |
| `heldCount(): int` | Deliveries held: handed out and not answered, plus the ones given back. |
| `close()` | Closes the connection: the broker gives everything held back, at the head of the queue. |
| `queue(): string`, `RabbitMqSource::DEFAULT_POLL_INTERVAL_MS` | The queue; the default interval (25). |

Every exception of the source is a `MessagingException` of the core (`Antevemus\ASpecification\Messaging\Exceptions`), whose message names the queue, the broker and the cause, never a body. The arguments out of their range are an `InvalidArgumentException`.
