# Delivery, Ordering and Dead Letters

## Delivery guarantees

The design is **at least once, plus the inbox of the consumer**, end to end:

- **Out:** the relay of the core marks a message delivered only after the broker confirmed it. A process that dies between the confirm and the mark sends the message again on its next run, and the queue gets it twice.
- **In:** a delivery stays unacknowledged until the consumer answers it. A consumer that dies before it answers leaves the delivery to the broker, which gives it to the next consumer of the queue, with its delivery count raised. A consumer that dies after its transaction committed and before the acknowledgement gets the message again too.
- **Once per consumer:** `MessageConsumer` records each message in its inbox in the same transaction as the effects of the handlers, so the copies of a message, whatever produced them, are acknowledged as duplicates without running anything.

## The pull and the wait

`RabbitMqSource::receive($max, $waitMs)` pulls with `basic.get`, one message at a time with manual acknowledgement, until it has `$max` deliveries or the queue is empty. When the queue was empty it pulls again every `pollIntervalMs` (25 ms by default) until `$waitMs` runs out. The source never holds a message the consumer was not handed: there is no prefetch buffer. `basic.qos` limits what a broker pushes to a `basic.consume` subscription, which this source does not open; the bound on the deliveries in flight is `$max`, the number the consumer asked for. A delivery pushed to a buffer and not yet handed out would come back from the broker with its count raised, without having run, when the process dies; with the pull, what is in flight is exactly what the consumer has.

## Ordering and requeue

The messages of an ordering key reach a consumer in the order of publication, and a delivery rejected with requeue (a handler that failed, or a later message of its key held back behind it) must come again **before** any later delivery of its key. A quorum queue of RabbitMQ 3.13 puts a message rejected with requeue (`basic.nack` or `basic.reject` with requeue) at the **tail** of the queue, behind the later messages of its key, so `RabbitMqSource` does not give a requeued delivery back to the broker: it keeps it, still unacknowledged, and the next `receive()` hands it out again before anything it pulls, in the order the deliveries were first received, with the delivery count raised by one.

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
$source->reject($first, true, 'The handler failed.'); // kept by the source, not sent to the tail of the queue
$deliveries = $source->receive(10, 0);
array_map(fn(ReceivedMessage $d): string => $d->message()->payload['step'], $deliveries); // ['reminder', 'second notice', 'collection']
array_map(fn(ReceivedMessage $d): int => $d->deliveryCount, $deliveries); // [2, 1, 1]
foreach ($deliveries as $delivery) {
    $source->ack($delivery);
}
```

- **A dead process.** What the source keeps is still checked out from the broker. If the process dies, RabbitMQ gives everything the connection held back at the **head** of the queue, in order, with `x-delivery-count` raised, so the order of a key holds across the crash as well. The count the next consumer sees is the broker's plus one: the redeliveries the source made itself are not known to the broker (a message that failed three times in a process that then died arrives as its second delivery).
- **The delivery count** is `x-delivery-count + 1` (the header counts the deliveries before this one; it is absent on the first), plus the times the source handed the delivery out again.
- **One consumer per queue** keeps the order. Competing consumers on one queue keep it only per consumer, as in any broker.
- **A failure of the broker** (the connection lost, the queue missing) closes the connection and forgets what the source held (the broker gives it back) and throws a `MessagingException` that names the queue and the broker; the next `receive()` opens a new connection. Answering a delivery of a lost connection is refused with a `MessagingException`: the broker delivers it again.

## Dead letters

`reject($requeue = false)` is how the consumer dead-letters: an invalid body, a handler that refused the payload, the last allowed delivery that failed, a delivery count already above the limit. The source publishes the body and the properties of the delivery to the dead letter exchange, with the routing key of the queue, so the copy lands in `<queue>.dead`, adds three headers and acknowledges the original only once the broker confirmed the copy:

| Header | Value |
| :--- | :--- |
| `aspec-dead-reason` | the reason of the consumer (at most 2 000 characters): it names the message by its name, id and ordering key, never by its payload |
| `aspec-dead-delivery-count` | the delivery count when it was rejected |
| `aspec-dead-queue` | the consumer queue it came from |

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

// Reading the dead letter queue, as an operator or a repair job would (plain php-amqplib).
$letter = $connector->connect()->channel()->basic_get($topology->deadLetterQueueOf('docs.refusals.billing'), true);
$headers = $letter->get('application_headers')->getNativeData();
$headers['aspec-dead-reason']; // 'Refused by the billing rules.'
[$headers['aspec-dead-delivery-count'], $headers['aspec-dead-queue'], $headers['aspec-tenant']]; // [1, 'docs.refusals.billing', 'acme']
Message::fromJson($letter->getBody())->messageId === $message->messageId; // true
$source->receive(1, 0); // []
```

- **Not confirmed, not acknowledged.** When the copy is not confirmed (no dead letter queue is bound for the queue, so it is returned; a nack; no answer in time), the source acknowledges nothing, closes the connection (everything it held goes back to the queue) and throws a `MessagingException`. A process that dies between the confirm of the copy and the acknowledgement leaves the delivery to come again, and it may be dead-lettered twice.
- **The broker's own dead letters** (a delivery limit, a TTL set by a policy) reach `<queue>.dead` too, through the `x-dead-letter-exchange` of the queue, with the `x-death` header of RabbitMQ and without the three headers above.

Next: [API reference](../api/reference.md).
