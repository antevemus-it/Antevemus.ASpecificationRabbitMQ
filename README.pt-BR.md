# Antevemus ASpecification RabbitMQ

[![PHP](https://img.shields.io/badge/PHP-8.2%2B-777bb4.svg)](https://www.php.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/actions/workflows/tests.yml/badge.svg)](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/actions/workflows/tests.yml)

[English](README.md)

**O satélite RabbitMQ da [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) para PHP 8.2+.** Um transporte e uma fonte para o gancho de mensageria agnóstico de broker do núcleo (`Antevemus\ASpecification\Messaging`): o relay envia a outbox a uma exchange `topic` com publisher confirms e a flag mandatory, e cada consumidor lê a própria quorum queue durável com ack manual, a contagem de entregas do broker, requeues que mantêm a ordem de uma chave de ordem, e uma exchange de mensagens mortas cujas mensagens levam o motivo, nunca o payload. PHP puro, sobre o php-amqplib: a extensão `amqp` não é necessária.

Instale com `composer require antevemus/aspecification-rabbitmq`.

## Início Rápido

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

## Documentação

A documentação oficial fica em [docs/pt-BR](docs/pt-BR/index.md) (e em inglês, em [docs/en](docs/en/index.md)); todo bloco PHP dela roda na suíte de testes, contra um RabbitMQ real.

| Página | O que cobre |
| :--- | :--- |
| [Instalação e início rápido](docs/pt-BR/getting-started/index.md) | Composer, os requisitos e o Início Rápido. |
| [Topologia e publisher confirms](docs/pt-BR/concepts/topology-and-confirms.md) | A exchange `topic`, as quorum queues e seus padrões, a exchange de mensagens mortas, os confirms e a flag mandatory. |
| [Entrega, ordem e mensagens mortas](docs/pt-BR/concepts/delivery-and-ordering.md) | Pelo menos uma vez mais a inbox, o pull, a ordem de uma chave através de requeues e quedas, as mensagens mortas. |
| [Referência da API](docs/pt-BR/api/reference.md) | `RabbitMqConnector`, `RabbitMqTopology`, `RabbitMqTransport` e `RabbitMqSource`. |
| [Testes](docs/pt-BR/quality/tests.md) | A bateria de contrato sobre o núcleo e sobre o RabbitMQ, as falhas, a propriedade e o teste de mutação. |
| [Roadmap](docs/pt-BR/project/roadmap.md) | O que foi entregue e o que pode vir. |

## Licença

MIT, veja [LICENSE](LICENSE). Procedência e a licença do php-amqplib (LGPL-2.1-or-later, uma dependência instalada pelo Composer, não embarcada) no [NOTICE.md](NOTICE.md); mudanças no [CHANGELOG.md](CHANGELOG.md).
