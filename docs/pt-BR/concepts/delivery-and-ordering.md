# Entrega, Ordem e Mensagens Mortas

## Garantias de entrega

O desenho é **pelo menos uma vez, mais a inbox do consumidor**, de ponta a ponta:

- **Saída:** o relay do núcleo marca uma mensagem como entregue só depois que o broker a confirmou. Um processo que morre entre o confirm e a marcação envia a mensagem de novo na rodada seguinte, e a fila a recebe duas vezes.
- **Entrada:** uma entrega fica sem ack até o consumidor responder. Um consumidor que morre antes de responder deixa a entrega com o broker, que a entrega ao próximo consumidor da fila, com a contagem de entregas aumentada. Um consumidor que morre depois do commit da transação e antes do ack recebe a mensagem de novo também.
- **Uma vez por consumidor:** o `MessageConsumer` registra cada mensagem na inbox na mesma transação dos efeitos dos handlers, então as cópias de uma mensagem, venham de onde vierem, são confirmadas como duplicadas sem rodar nada.

## O pull e a espera

O `RabbitMqSource::receive($max, $waitMs)` puxa com `basic.get`, uma mensagem por vez, com ack manual, até ter `$max` entregas ou a fila ficar vazia. Quando a fila estava vazia, puxa de novo a cada `pollIntervalMs` (25 ms por padrão) até `$waitMs` acabar. A fonte nunca guarda uma mensagem que o consumidor não recebeu: não há buffer de prefetch. O `basic.qos` limita o que um broker empurra para uma assinatura `basic.consume`, que esta fonte não abre; o limite das entregas em voo é o `$max`, o número que o consumidor pediu. Uma entrega empurrada para um buffer e ainda não entregue voltaria do broker com a contagem aumentada, sem ter rodado, quando o processo morresse; com o pull, o que está em voo é exatamente o que o consumidor tem.

## Ordem e requeue

As mensagens de uma chave de ordem chegam a um consumidor na ordem de publicação, e uma entrega rejeitada com requeue (um handler que falhou, ou uma mensagem posterior da mesma chave retida atrás dela) precisa voltar **antes** de qualquer entrega posterior da chave. Uma quorum queue do RabbitMQ 3.13 põe uma mensagem rejeitada com requeue (`basic.nack` ou `basic.reject` com requeue) no **fim** da fila, atrás das mensagens posteriores da chave; por isso o `RabbitMqSource` não devolve ao broker uma entrega rejeitada com requeue: ele a guarda, ainda sem ack, e o `receive()` seguinte a entrega de novo antes de qualquer coisa que puxar, na ordem em que as entregas chegaram pela primeira vez, com a contagem de entregas aumentada em um.

<!-- tested: requeue-order -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecification\Messaging\ReceivedMessage;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;

$connector = RabbitMqConnector::fromUrl(getenv('RABBITMQ_URL') ?: 'amqp://guest:guest@127.0.0.1:5672/%2F');
$topology = new RabbitMqTopology('docs.orders');
$topology->declare($connector, ['docs.orders.billing' => ['billing.#']]);
$transport = new RabbitMqTransport($connector, $topology);
foreach (['reminder', 'second notice', 'collection'] as $step) {
    $transport->send([new Message('billing.invoice_overdue.v1', ['step' => $step], orderingKey: 'invoice:7')]);
}

$source = new RabbitMqSource($connector, 'docs.orders.billing', $topology);
$first = $source->receive(1, 0)[0];
$source->reject($first, true, 'The handler failed.'); // guardada pela fonte, não vai para o fim da fila
$deliveries = $source->receive(10, 0);
array_map(fn(ReceivedMessage $d): string => $d->message()->payload['step'], $deliveries); // ['reminder', 'second notice', 'collection']
array_map(fn(ReceivedMessage $d): int => $d->deliveryCount, $deliveries); // [2, 1, 1]
foreach ($deliveries as $delivery) {
    $source->ack($delivery);
}
```

- **Um processo morto.** O que a fonte guarda continua retirado do broker. Se o processo morre, o RabbitMQ devolve tudo o que a conexão tinha ao **início** da fila, em ordem, com o `x-delivery-count` aumentado, então a ordem de uma chave se mantém também através da queda. A contagem que o próximo consumidor vê é a do broker mais um: as reentregas que a própria fonte fez o broker não conhece (uma mensagem que falhou três vezes num processo que depois morreu chega como a segunda entrega).
- **A contagem de entregas** é `x-delivery-count + 1` (o header conta as entregas anteriores a esta; na primeira ele não vem), mais as vezes que a fonte entregou de novo.
- **Um consumidor por fila** mantém a ordem. Consumidores concorrentes numa fila a mantêm só por consumidor, como em qualquer broker.
- **Uma falha do broker** (a conexão perdida, a fila inexistente) fecha a conexão, esquece o que a fonte guardava (o broker devolve) e lança uma `MessagingException` que cita a fila e o broker; o `receive()` seguinte abre uma conexão nova. Responder a uma entrega de uma conexão perdida é recusado com uma `MessagingException`: o broker a entrega de novo.

## Mensagens mortas

O `reject($requeue = false)` é como o consumidor manda para a fila morta: um corpo inválido, um handler que recusou o payload, a última entrega permitida que falhou, uma contagem de entregas já acima do limite. A fonte publica o corpo e as propriedades da entrega na exchange de mensagens mortas, com a routing key da fila, para que a cópia caia em `<fila>.dead`, acrescenta três headers e só dá ack no original depois que o broker confirmou a cópia:

| Header | Valor |
| :--- | :--- |
| `aspec-dead-reason` | o motivo do consumidor (até 2 000 caracteres): cita a mensagem pelo nome, id e chave de ordem, nunca pelo payload |
| `aspec-dead-delivery-count` | a contagem de entregas no momento da rejeição |
| `aspec-dead-queue` | a fila do consumidor de onde veio |

<!-- tested: dead-letters -->
```php
<?php

declare(strict_types=1);

use Antevemus\ASpecification\Messaging\Message;
use Antevemus\ASpecificationRabbitMQ\RabbitMqConnector;
use Antevemus\ASpecificationRabbitMQ\RabbitMqSource;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTopology;
use Antevemus\ASpecificationRabbitMQ\RabbitMqTransport;

$connector = RabbitMqConnector::fromUrl(getenv('RABBITMQ_URL') ?: 'amqp://guest:guest@127.0.0.1:5672/%2F');
$topology = new RabbitMqTopology('docs.refusals');
$topology->declare($connector, ['docs.refusals.billing' => ['#']]);
(new RabbitMqTransport($connector, $topology))->send([$message = new Message('billing.invoice_overdue.v1', ['invoice_id' => 7], tenant: 'acme')]);

$source = new RabbitMqSource($connector, 'docs.refusals.billing', $topology);
$source->reject($source->receive(1, 0)[0], false, 'Refused by the billing rules.');

// Lendo a fila morta, como faria um operador ou um job de reparo (php-amqplib puro).
$letter = $connector->connect()->channel()->basic_get($topology->deadLetterQueueOf('docs.refusals.billing'), true);
$headers = $letter->get('application_headers')->getNativeData();
$headers['aspec-dead-reason']; // 'Refused by the billing rules.'
[$headers['aspec-dead-delivery-count'], $headers['aspec-dead-queue'], $headers['aspec-tenant']]; // [1, 'docs.refusals.billing', 'acme']
Message::fromJson($letter->getBody())->messageId === $message->messageId; // true
$source->receive(1, 0); // []
```

- **Sem confirm, sem ack.** Quando a cópia não é confirmada (nenhuma fila morta ligada para a fila, e ela volta; um nack; a resposta que não vem a tempo), a fonte não confirma nada, fecha a conexão (tudo o que guardava volta para a fila) e lança uma `MessagingException`. Um processo que morre entre o confirm da cópia e o ack deixa a entrega para voltar, e ela pode ir duas vezes para a fila morta.
- **As mensagens mortas do próprio broker** (um limite de entregas, um TTL definido por policy) também chegam a `<fila>.dead`, pelo `x-dead-letter-exchange` da fila, com o header `x-death` do RabbitMQ e sem os três headers acima.

Próximo: [Referência da API](../api/reference.md).
