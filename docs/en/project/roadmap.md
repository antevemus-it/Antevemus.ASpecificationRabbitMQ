# Roadmap

## Shipped

- **1.0.0:** `RabbitMqTransport` (publisher confirms, the mandatory flag, nacks and connection failures per message), `RabbitMqSource` (pull with manual acknowledgements, the delivery count of a quorum queue, requeues kept in order, dead letters with their reason through a dead letter exchange), `RabbitMqTopology` (a topic exchange, a quorum queue per consumer bound by patterns, a dead letter exchange and queue, idempotent) and `RabbitMqConnector` (host and credentials, an `amqp://` URL or a factory of the application).

## On demand

Not planned; each one comes when a consumer needs it:

- [ ] A push mode (`basic.consume` with `basic.qos`) for a consumer whose volume makes one round trip per message too slow, with a buffer that keeps the order of a key across requeues.
- [ ] A topology over streams of RabbitMQ (`x-queue-type: stream`), for consumers that replay.
- [ ] The native requeue of the broker, for a RabbitMQ whose quorum queues put a requeued message back at its place.

The milestone view is in [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/ROADMAP.md).
