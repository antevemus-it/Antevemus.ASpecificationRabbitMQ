# Primeiros Passos

## 📦 Instalação

Instale via Composer:

```bash
composer require antevemus/aspecification-rabbitmq
```

Requisitos: PHP 8.2 ou mais novo com `ext-sockets` e `ext-mbstring`, e um RabbitMQ 3.13 ou mais novo (as filas são quorum queues). Vêm junto: [`antevemus/aspecification`](https://github.com/antevemus-it/Antevemus.ASpecification) `^2.15` (o núcleo, do mesmo autor, que tem o gancho de mensageria) e [`php-amqplib/php-amqplib`](https://github.com/php-amqplib/php-amqplib) `^3.7` (um cliente AMQP 0-9-1 em PHP puro; a extensão PECL `amqp` não é necessária).

Para que serve: levar as mensagens do gancho de mensageria do núcleo entre os módulos de uma aplicação pelo RabbitMQ. A aplicação publica na outbox do núcleo, dentro da própria transação; o relay do núcleo envia a outbox com o `RabbitMqTransport`; cada consumidor lê a própria fila com o `RabbitMqSource`, pelo `MessageConsumer` do núcleo, que deduplica por uma inbox. A entrega é pelo menos uma vez, e cada consumidor roda uma mensagem uma vez.

## 🚀 Início Rápido

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
$topology->declare($connector, ['quickstart.agents' => ['underwriting.#']]); // idempotente: a cada partida

// Publica na outbox (em produção, uma PdoMessageOutbox, dentro da transação da escrita) e faz o relay.
$outbox = new InMemoryMessageOutbox();
(new MessagePublisher($outbox))->publish(new Message('underwriting.quote_submitted.v1', ['quote_id' => 123], orderingKey: 'quote:123', tenant: 'acme'));
$relay = new OutboxRelay($outbox, new RabbitMqTransport($connector, $topology));
$relay->relayPending(100)->deliveredCount(); // 1

// Consome a fila do consumidor "agents": o handler roda, depois a entrega é confirmada (ack).
$tasks = [];
$handlers = (new MessageHandlerRegistry())->register('underwriting.quote_submitted.v1', function (Message $message) use (&$tasks): void {
    $tasks[] = $message->payload['quote_id'];
});
$consumer = new MessageConsumer('agents', new RabbitMqSource($connector, 'quickstart.agents', $topology), $handlers, new InMemoryProcessedMessageStore());
$consumer->consume(100, waitMs: 1000);
$tasks; // [123]
```

O relay conta a mensagem como entregue só porque o RabbitMQ a confirmou (um publisher confirm: a quorum queue a gravou). O consumidor a puxa de `quickstart.agents`, uma quorum queue durável ligada à exchange por `underwriting.#`, entrega ao handler, registra na inbox e confirma. Em produção a outbox e a inbox são os stores PDO do núcleo, e o handler escreve pela mesma conexão, então o efeito dele e a linha da inbox entram no mesmo commit.

Próximo: [Topologia e publisher confirms](../concepts/topology-and-confirms.md).
