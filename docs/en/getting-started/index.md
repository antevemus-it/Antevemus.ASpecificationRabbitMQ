# Getting Started

## 📦 Installation

Install via Composer:

```bash
composer require antevemus/aspecification-rabbitmq
```

Requirements: PHP 8.2 or later with `ext-sockets` and `ext-mbstring`, and a RabbitMQ 3.13 or later (the queues are quorum queues). Installed with it: [`antevemus/aspecification`](https://github.com/antevemus-it/Antevemus.ASpecification) `^2.15` (the core, from the same author, which has the messaging hook) and [`php-amqplib/php-amqplib`](https://github.com/php-amqplib/php-amqplib) `^3.7` (a pure PHP AMQP 0-9-1 client; the `amqp` PECL extension is not needed).

What it is for: carrying the messages of the core's messaging hook between the modules of an application through RabbitMQ. The application publishes to the outbox of the core, inside its own transaction; the relay of the core sends the outbox with `RabbitMqTransport`; each consumer reads its own queue with `RabbitMqSource` through the core's `MessageConsumer`, which deduplicates by an inbox. Delivery is at least once, and each consumer runs a message once.

## 🚀 Quick Start

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

The relay counts the message as delivered only because RabbitMQ confirmed it (a publisher confirm: the quorum queue stored it). The consumer pulls it from `quickstart.agents`, a durable quorum queue bound to the exchange by `underwriting.#`, hands it to the handler, records it in the inbox and acknowledges it. In production the outbox and the inbox are the PDO stores of the core, and the handler writes through the same connection, so its effect and the inbox row commit together.

Next: [Topology and publisher confirms](../concepts/topology-and-confirms.md).
