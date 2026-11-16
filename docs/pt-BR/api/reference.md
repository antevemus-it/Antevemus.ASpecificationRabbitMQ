# Referência da API

Quatro classes, todas `final`, em `Antevemus\ASpecificationRabbitMQ`. O transporte e a fonte implementam as duas interfaces do gancho de mensageria do núcleo (`IMessageTransport`, `IMessageSource`) e são usados pelo `OutboxRelay` e pelo `MessageConsumer` do núcleo; nada mais do núcleo é substituído.

## RabbitMqConnector

Abre conexões com um broker (uma fábrica: o transporte e a fonte abrem cada um a sua, e abrem outra depois de uma falha).

| Membro | O quê |
| :--- | :--- |
| `new RabbitMqConnector(string $host = '127.0.0.1', int $port = 5672, string $user = 'guest', string $password = 'guest', string $vhost = '/', float $connectionTimeout = 3.0, float $readWriteTimeout = 10.0, int $heartbeat = 0)` | Uma `AMQPStreamConnection` do php-amqplib por `connect()`. A porta vai de 1 a 65535, os timeouts são positivos, o heartbeat é zero ou mais e o timeout de leitura/escrita é pelo menos o dobro dele (senão, `InvalidArgumentException`). |
| `RabbitMqConnector::fromUrl(string $url)` | A partir de `amqp://usuario:senha@host:porta/vhost`, com percent-decoding (`%2F` é `/`); porta 5672, usuário e senha `guest` e vhost `/` quando ausentes. `amqps` e outros esquemas, uma query ou um fragmento são recusados, sem repetir a URL. |
| `RabbitMqConnector::fromFactory(Closure $factory, string $description = 'amqp (custom connection)')` | Roda a closure (`fn(): AbstractConnection`) a cada conexão: TLS (`AMQPSSLConnection`), uma lista de hosts, qualquer opção do php-amqplib. |
| `connect(): AbstractConnection` | Uma conexão nova (o que o php-amqplib lança quando o broker não é alcançado ou recusa o login). |
| `describe(): string` | O broker como um motivo o cita: `amqp://host:porta/vhost`, nunca o usuário nem a senha. |

A senha é marcada com `#[SensitiveParameter]` (também a URL do `fromUrl()`), então um stack trace não a mostra.

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

// Um broker inalcançável: toda mensagem falha, com um motivo que a cita, nunca o payload.
$message = new Message('crm.lead_created.v1', ['cpf' => '529.982.247-25']);
$result = (new RabbitMqTransport(new RabbitMqConnector('127.0.0.1', 1)))->send([$message]);
$result->isConfirmed($message->messageId); // false
str_contains((string) $result->reasonOf($message->messageId), '529.982.247-25'); // false
```

## RabbitMqTopology

| Membro | O quê |
| :--- | :--- |
| `new RabbitMqTopology(string $exchange = 'aspec.messages', ?string $deadLetterExchange = null)` | A exchange `topic` das mensagens e a exchange `direct` das mensagens mortas (`<exchange>.dead` por padrão). Um nome tem de 1 a 200 caracteres de `[A-Za-z0-9_.:-]`, sem começar por `amq.`; os dois precisam ser diferentes. |
| `exchange()`, `deadLetterExchange()` | Os dois nomes. |
| `deadLetterQueueOf(string $queue): string` | `<fila>.dead`. |
| `declare(RabbitMqConnector $connector, array $queues = [])` | Abre uma conexão, declara as exchanges e cada fila de `[fila => [padrão, ...]]` com a sua fila morta, e fecha. Idempotente. |
| `declareExchanges(AMQPChannel $channel)`, `declareQueue(AMQPChannel $channel, string $queue, array $patterns)` | O mesmo, num canal da aplicação. |
| `RabbitMqTopology::DEFAULT_EXCHANGE`, `DEAD_SUFFIX`, `NAME_PATTERN`, `PATTERN_PATTERN` | A exchange padrão, o sufixo `.dead`, a regra de um nome e a regra de um padrão (palavras de `[a-z0-9_]` separadas por ponto, `*` ou `#`). |

## RabbitMqTransport

| Membro | O quê |
| :--- | :--- |
| `new RabbitMqTransport(RabbitMqConnector $connector, RabbitMqTopology $topology = new RabbitMqTopology(), float $confirmTimeout = 5.0)` | O `IMessageTransport` (veja [Topologia e publisher confirms](../concepts/topology-and-confirms.md)). O transporte não declara a topologia: declare-a na partida. |
| `send(array $batch): TransportSendResult` | Publica o lote com confirms e `mandatory`; as mensagens confirmadas levam o id da mensagem como id do broker; as devolvidas, as com nack e as sem resposta falham com um motivo. Nunca lança exceção. |
| `close()` | Fecha a conexão; o envio seguinte abre outra. |

## RabbitMqSource

| Membro | O quê |
| :--- | :--- |
| `new RabbitMqSource(RabbitMqConnector $connector, string $queue, RabbitMqTopology $topology = new RabbitMqTopology(), int $pollIntervalMs = 25, float $confirmTimeout = 5.0)` | O `IMessageSource` de uma fila de consumidor (veja [Entrega, ordem e mensagens mortas](../concepts/delivery-and-ordering.md)). A topologia dá a exchange de mensagens mortas; o timeout é a espera pelo confirm de uma mensagem morta. |
| `receive(int $max, int $waitMs): array` | Até `$max` entregas (`ReceivedMessage`), as devolvidas primeiro; um corpo inválido é uma entrega inválida. O id do broker é `<conexão>.<delivery tag>` (`1.7`). |
| `ack()`, `reject(…, bool $requeue, string $reason)` | `basic.ack`; um requeue guardado pela fonte; uma mensagem morta com o motivo, depois `basic.ack`. |
| `heldCount(): int` | Entregas guardadas: entregues e não respondidas, mais as devolvidas. |
| `close()` | Fecha a conexão: o broker devolve tudo o que estava guardado ao início da fila. |
| `queue(): string`, `RabbitMqSource::DEFAULT_POLL_INTERVAL_MS` | A fila; o intervalo padrão (25). |

Toda exceção da fonte é uma `MessagingException` do núcleo (`Antevemus\ASpecification\Messaging\Exceptions`), cuja mensagem cita a fila, o broker e a causa, nunca um corpo. Argumentos fora da faixa são uma `InvalidArgumentException`.
