# Tests

The suite runs with PHPUnit in four groups, `Unit`, `Contract`, `Broker` and `Property`, in the CI of every push on PHP 8.2, 8.3 and 8.4, against a RabbitMQ 3.13 started as a service of the workflow:

```bash
docker run -d -p 5672:5672 rabbitmq:3.13
RABBITMQ_HOST=127.0.0.1 RABBITMQ_PORT=5672 vendor/bin/phpunit
```

The broker is read from `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` and `RABBITMQ_VHOST` (127.0.0.1, 5672, guest, guest, `/`). A test that needs it is skipped, with the reason, when it cannot be reached; the CI runs with `--fail-on-skipped`, so there none is skipped. Every test declares exchanges and queues of its own, under unique names, and removes them at the end.

## What is tested

- **The contract battery**, written once and run over two implementations: the in-memory transport and source of the core (the reference) and this package against the real broker. Each scenario goes the way of an application: the core's `MessagePublisher` into its `InMemoryMessageOutbox`, the core's `OutboxRelay` through the transport, the core's `MessageConsumer` over the source with its `InMemoryProcessedMessageStore` and a transaction manager; it compares the transcript (each delivery's message, outcome and delivery count), the effects of the handlers and the dead letters with literals that are the same for both. The scenarios: a round trip that keeps every field, every consumer gets every message, the order of each key across batches, a requeued delivery that comes back before the later ones of its key, a dead letter after `maxDeliveries` with its reason and count, bodies that are not messages dead-lettered and never dropped, a name without a handler, a consumer of one tenant, a payload of exactly 65 535 bytes, a batch of nine keys, duplicates absorbed by the inbox, and the deliveries of a dead consumer handed to its successor.
- **Failures against the broker**: a relay that dies between the confirm and the mark (the message is sent again, the effect runs once); a consumer whose socket is cut after its commit and before its acknowledgement (everything it held goes, in order and with the count raised, to the next consumer; the committed message is a duplicate); a broker that cannot be reached (the whole batch fails, nothing is marked delivered); a message no queue is bound for (failed by the mandatory flag, sent again once a queue is bound); a nack of a full queue; a missing exchange and the reconnection; and the secrecy of every reason, report, exception and dead letter header on the way, with a payload that carries a marker.
- **What the broker does**, measured so that the design rests on it: a quorum queue of RabbitMQ 3.13 puts a message rejected with requeue at the tail (the reason the source keeps a requeued delivery), gives the deliveries of a closed connection back at the head, in order, and counts the deliveries in `x-delivery-count`; a classic queue gives only the `redelivered` flag.
- **The source and the transport on their own**, against the broker and against a scripted one (mocked php-amqplib channels): the pull, its wait and its interval, the delivery count, the requeue kept in order, the dead letter and its confirm (acknowledged only after it), the properties and headers of a published message, each answer of the broker mapped to its result, the connection kept until it fails, the arguments and their messages.
- **A property** over random interleavings of publication, relay and consumption, with fixed seeds (3 seeds × 150 steps, over the core's pair and over RabbitMQ): relays that die after a confirm (15 % of the runs), consumers that die before an acknowledgement (10 % of the acknowledgements), handlers that fail up to twice or always (poison messages, 10 %), `maxDeliveries` 3. After draining, no confirmed message is lost, no effect runs twice, the messages of every key are handled in the order of publication (the dead-lettered ones aside), and the dead letter queue holds exactly what the reports dead-lettered.
- **The documentation**: every PHP block of `docs/en` and `docs/pt-BR`, and the Quick Start of each README, runs with the answers written in its comments (the blocks that talk to a broker against the test broker); both languages carry the same code and the same files.

## Mutation testing

Infection runs over the whole `src/` in a scheduled workflow, against a RabbitMQ service, with minimum scores one point below the last measure. Measured for 1.0.0: 274 mutants, 269 killed, 1 time-out, 4 escaped, all equivalent (constants of the clock of the wait off by one part in a million or a thousand, and a remaining time of exactly zero nanoseconds); MSI 98.54%, mutation code coverage 100%.

The suite has 168 tests (113 run and 55 skipped without a broker).
