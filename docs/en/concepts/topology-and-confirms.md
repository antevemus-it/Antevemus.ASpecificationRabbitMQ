# Topology and Publisher Confirms

## The exchanges and the queues

`RabbitMqTopology` names what the transport publishes to and what each consumer reads from, and declares it:

| What | Name | Kind |
| :--- | :--- | :--- |
| The exchange of the messages | `aspec.messages` (the first argument) | durable, `topic`; the routing key of a message is its name |
| A queue per consumer | the name you give it | durable **quorum** queue, bound by one or more routing-key patterns |
| The dead letter exchange | `<exchange>.dead` (or the second argument) | durable, `direct` |
| A dead letter queue per consumer | `<queue>.dead` | durable quorum queue, bound to the dead letter exchange by the name of the consumer queue |

A pattern is the name of a message with the wildcards of a topic exchange: `*` stands for one word, `#` for any number of them. `billing.#` takes every message of the billing module; `*.invoice_overdue.v1` takes that fact from any module; `crm.lead_created.v1` takes one name.

Every consumer queue names the dead letter exchange as its `x-dead-letter-exchange`, with its own name as the routing key, so a message the broker dead-letters by itself (a policy with a delivery limit or a time to live) lands in the same `<queue>.dead` as the ones the consumer rejects.

**Why quorum queues.** A quorum queue is replicated (Raft) and counts the deliveries of each message: every redelivery carries the `x-delivery-count` header. The source needs that number to give the consumer a delivery count that survives a dead process, which is what lets the consumer send a message that keeps killing its worker to the dead letter queue instead of looping on it. A classic queue has only a `redelivered` flag; the source reads it as "at least the second delivery" but cannot count further.

**Idempotent.** `declare()` may run on every start of a worker: declaring what already exists with the same arguments changes nothing. A queue that already exists with other arguments (a classic queue of the same name) is refused by the broker (`PRECONDITION_FAILED`). Bindings are only added: a pattern dropped from a later declaration stays bound until it is unbound by hand.

## Publisher confirms and the mandatory flag

`RabbitMqTransport::send()` publishes the batch the relay gives it and waits, up to `confirmTimeout` seconds (5 by default), for the broker's answer to each message. Only a confirm (`basic.ack`) makes a message confirmed: for a quorum queue the broker sends it once a majority of the replicas wrote the message. Every message is published with the `mandatory` flag, so a message no queue is bound for comes back (`basic.return`, `312 NO_ROUTE`) and counts as failed, although the broker acknowledges it afterwards: it was stored nowhere.

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
$topology->declare($connector, ['docs.billing' => ['billing.#', '*.invoice_overdue.v1']]); // the second time changes nothing

$result = (new RabbitMqTransport($connector, $topology))->send([
    $overdue = new Message('billing.invoice_overdue.v1', ['invoice_id' => 7], orderingKey: 'invoice:7'),
    $lead = new Message('crm.lead_created.v1', ['lead_id' => 9]),
]);
$result->confirmedIds() === [$overdue->messageId]; // true
$result->brokerIdOf($overdue->messageId) === $overdue->messageId; // true (RabbitMQ gives a message no id of its own)
str_contains((string) $result->reasonOf($lead->messageId), 'as unroutable (312 NO_ROUTE)'); // true
```

- **The broker message.** The routing key is the name; the body is `Message::toJson()`, the wire form of the core, with `content_type` `application/json`; it is persistent (`delivery_mode` 2); `message_id` is the message id, `type` the name and `timestamp` the instant of the fact; the headers `aspec-tenant` and `aspec-ordering-key` carry the tenant and the ordering key when the message has them, for a policy, a headers exchange or a person reading the queue.
- **A nack** (`basic.nack`: a queue with `x-overflow: reject-publish` that is full, an internal error) fails the message.
- **The connection** is opened on the first send and kept. A failure of the connection or of the channel (the broker unreachable, the exchange not declared, no answer in time) fails every message not confirmed yet, closes the connection, and the next send opens a new one. `send()` never throws: the result says which messages failed and why, and the relay sends them again on its next run.
- **The order.** The relay of the core puts at most one message of each ordering key in a batch and sends the next message of a key only after the broker confirmed the previous one, so a queue stores the messages of a key in the order of publication; the transport needs no ordering of its own.

Next: [Delivery, ordering and dead letters](delivery-and-ordering.md).
