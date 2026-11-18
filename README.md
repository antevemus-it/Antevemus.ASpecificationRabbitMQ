# Antevemus ASpecification RabbitMQ

[![PHP](https://img.shields.io/badge/PHP-8.2%2B-777bb4.svg)](https://www.php.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/actions/workflows/tests.yml/badge.svg)](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/actions/workflows/tests.yml)

[Português (Brasil)](README.pt-BR.md)

**The RabbitMQ satellite of [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) for PHP 8.2+.** A transport and a source for the broker-agnostic messaging hook of the core (`Antevemus\ASpecification\Messaging`): the relay sends the outbox to a topic exchange with publisher confirms and the mandatory flag, and each consumer reads its own durable quorum queue with manual acknowledgements, the delivery count of the broker, requeues that keep the order of an ordering key, and a dead letter exchange whose letters carry the reason, never the payload. Pure PHP, on php-amqplib: the `amqp` extension is not needed.

Install with `composer require antevemus/aspecification-rabbitmq`.

## Quick Start

<!-- tested: quick-start -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Inbox\InMemoryProcessedMessageStore;
use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\MessageConsumer;
use Antevemus\ASpecification\Messaging\MessageHandlerRegistry;
use Antevemus\ASpecification\Messaging\MessagePublisher;
use Antevemus\ASpecification\Messaging\Outbox\InMemoryMessageOutbox;
use Antevemus\ASpecification\Messaging\OutboxRelay;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;

$connector = RabbitMqConnector::fromUrl(getenv('RABBITMQ_URL') ?: 'amqp://guest:guest@127.0.0.1:5672/%2F');
$topology = new RabbitMqTopology('quickstart.messages');
$topology->declare($connector, ['quickstart.agents' => ['underwriting.#']]); // idempotent: on every start

// Publish to the outbox (in production a PdoMessageOutbox, inside the transaction of the write), then relay.
$outbox = new InMemoryMessageOutbox();
(new MessagePublisher($outbox))->publish(new Message('underwriting.quote_submitted.v1', ['quote_id' => 123], orderingKey: 'quote:123', tenant: 'acme'));
$relay = new OutboxRelay($outbox, new RabbitMqTransport($connector, $topology));
$relay->relayPending(100)->deliveredCount(); // 1

// Consume the queue of the consumer "agents": the handler runs, then the delivery is acknowledged.
$tasks = [];
$handlers = (new MessageHandlerRegistry())->register('underwriting.quote_submitted.v1', function (Message $message) use (&$tasks): void {
    $tasks[] = $message->payload['quote_id'];
});
$consumer = new MessageConsumer('agents', new RabbitMqSource($connector, 'quickstart.agents', $topology), $handlers, new InMemoryProcessedMessageStore());
$consumer->consume(100, waitMs: 1000);
$tasks; // [123]
```

## Documentation

The official documentation lives in [docs/en](docs/en/index.md) (and in Portuguese, in [docs/pt-BR](docs/pt-BR/index.md)); every PHP block of it runs in the test suite, against a real RabbitMQ.

| Page | What it covers |
| :--- | :--- |
| [Installation and quick start](docs/en/getting-started/index.md) | Composer, the requirements and the Quick Start. |
| [Topology and publisher confirms](docs/en/concepts/topology-and-confirms.md) | The topic exchange, the quorum queues and their patterns, the dead letter exchange, the confirms and the mandatory flag. |
| [Delivery, ordering and dead letters](docs/en/concepts/delivery-and-ordering.md) | At least once plus the inbox, the pull, the order of a key across requeues and crashes, the dead letters. |
| [API reference](docs/en/api/reference.md) | `RabbitMqConnector`, `RabbitMqTopology`, `RabbitMqTransport` and `RabbitMqSource`. |
| [Tests](docs/en/quality/tests.md) | The contract battery of the core run against RabbitMQ (how a satellite wires it: [Writing a satellite](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/en/messaging/writing-a-satellite.md) of the core), the tests of what is RabbitMQ's own, and the mutation testing. |
| [Roadmap](docs/en/project/roadmap.md) | What shipped and what may come. |

## License

MIT, see [LICENSE](LICENSE). Provenance and the license of php-amqplib (LGPL-2.1-or-later, a dependency installed by Composer, not bundled) in [NOTICE.md](NOTICE.md); changes in [CHANGELOG.md](CHANGELOG.md).
