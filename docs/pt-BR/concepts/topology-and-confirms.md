# Topologia e Publisher Confirms

## As exchanges e as filas

O `RabbitMqTopology` dá nome ao que o transporte publica e ao que cada consumidor lê, e declara tudo:

| O quê | Nome | Tipo |
| :--- | :--- | :--- |
| A exchange das mensagens | `aspec.messages` (o primeiro argumento) | durável, `topic`; a routing key de uma mensagem é o nome dela |
| Uma fila por consumidor | o nome que você der | **quorum queue** durável, ligada por um ou mais padrões de routing key |
| A exchange de mensagens mortas | `<exchange>.dead` (ou o segundo argumento) | durável, `direct` |
| Uma fila morta por consumidor | `<fila>.dead` | quorum queue durável, ligada à exchange de mensagens mortas pelo nome da fila do consumidor |

Um padrão é o nome de uma mensagem com os curingas de uma exchange `topic`: `*` vale uma palavra, `#` qualquer número delas. `billing.#` pega toda mensagem do módulo de cobrança; `*.invoice_overdue.v1` pega esse fato de qualquer módulo; `crm.lead_created.v1` pega um nome só.

Toda fila de consumidor indica a exchange de mensagens mortas como `x-dead-letter-exchange`, com o próprio nome como routing key, então uma mensagem que o broker manda para a fila morta por conta própria (uma policy com limite de entregas ou TTL) cai na mesma `<fila>.dead` que as rejeitadas pelo consumidor.

**Por que quorum queues.** Uma quorum queue é replicada (Raft) e conta as entregas de cada mensagem: toda reentrega traz o header `x-delivery-count`. A fonte precisa desse número para dar ao consumidor uma contagem de entregas que sobrevive a um processo morto, e é isso que permite ao consumidor mandar para a fila morta uma mensagem que derruba o worker toda vez, em vez de ficar em laço nela. Uma fila clássica só tem a flag `redelivered`; a fonte a lê como "pelo menos a segunda entrega", mas não consegue contar além disso.

**Idempotente.** O `declare()` pode rodar a cada partida de um worker: declarar o que já existe com os mesmos argumentos não muda nada. Uma fila que já existe com outros argumentos (uma fila clássica de mesmo nome) é recusada pelo broker (`PRECONDITION_FAILED`). Ligações só são acrescentadas: um padrão que saiu de uma declaração posterior continua ligado até ser desligado à mão.

## Publisher confirms e a flag mandatory

O `RabbitMqTransport::send()` publica o lote que o relay entrega e espera, até `confirmTimeout` segundos (5 por padrão), a resposta do broker para cada mensagem. Só um confirm (`basic.ack`) torna uma mensagem confirmada: numa quorum queue o broker o manda depois que a maioria das réplicas gravou a mensagem. Toda mensagem é publicada com a flag `mandatory`, então uma mensagem para a qual nenhuma fila está ligada volta (`basic.return`, `312 NO_ROUTE`) e conta como falha, embora o broker a confirme depois: ela não foi gravada em lugar nenhum.

<!-- tested: topology -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;

$connector = RabbitMqConnector::fromUrl(getenv('RABBITMQ_URL') ?: 'amqp://guest:guest@127.0.0.1:5672/%2F');
$topology = new RabbitMqTopology('docs.messages');
$topology->exchange(); // 'docs.messages'
$topology->deadLetterExchange(); // 'docs.messages.dead'
$topology->deadLetterQueueOf('docs.billing'); // 'docs.billing.dead'

$topology->declare($connector, ['docs.billing' => ['billing.#', '*.invoice_overdue.v1']]);
$topology->declare($connector, ['docs.billing' => ['billing.#', '*.invoice_overdue.v1']]); // a segunda vez não muda nada

$result = (new RabbitMqTransport($connector, $topology))->send([
    $overdue = new Message('billing.invoice_overdue.v1', ['invoice_id' => 7], orderingKey: 'invoice:7'),
    $lead = new Message('crm.lead_created.v1', ['lead_id' => 9]),
]);
$result->confirmedIds() === [$overdue->messageId]; // true
$result->brokerIdOf($overdue->messageId) === $overdue->messageId; // true (o RabbitMQ não dá à mensagem um id próprio)
str_contains((string) $result->reasonOf($lead->messageId), 'as unroutable (312 NO_ROUTE)'); // true
```

- **A mensagem no broker.** A routing key é o nome; o corpo é o `Message::toJson()`, a forma de fio do núcleo, com `content_type` `application/json`; ela é persistente (`delivery_mode` 2); `message_id` é o id da mensagem, `type` o nome e `timestamp` o instante do fato; os headers `aspec-tenant` e `aspec-ordering-key` levam o tenant e a chave de ordem quando a mensagem os tem, para uma policy, uma exchange de headers ou uma pessoa que leia a fila.
- **Um nack** (`basic.nack`: uma fila cheia com `x-overflow: reject-publish`, um erro interno) faz a mensagem falhar.
- **A conexão** é aberta no primeiro envio e mantida. Uma falha da conexão ou do canal (o broker inalcançável, a exchange não declarada, a resposta que não veio a tempo) faz falhar toda mensagem ainda não confirmada, fecha a conexão, e o envio seguinte abre outra. O `send()` nunca lança exceção: o resultado diz quais mensagens falharam e por quê, e o relay as envia de novo na rodada seguinte.
- **A ordem.** O relay do núcleo põe no máximo uma mensagem de cada chave de ordem num lote e só envia a próxima mensagem de uma chave depois que o broker confirmou a anterior, então uma fila grava as mensagens de uma chave na ordem de publicação; o transporte não precisa de ordenação própria.

Próximo: [Entrega, ordem e mensagens mortas](delivery-and-ordering.md).
