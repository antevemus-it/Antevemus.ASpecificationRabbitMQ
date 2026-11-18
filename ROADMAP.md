# Roadmap

The RabbitMQ satellite of ASpecification. The core keeps the messaging hook broker-agnostic and ships no broker client; this package carries the RabbitMQ transport and source.

## Milestones

- [x] **1.0.0:** `RabbitMqTransport` (publisher confirms mandatory, the mandatory flag with returns counted as failures, nacks, connection failures per message), `RabbitMqSource` (pull with manual acknowledgements, `x-delivery-count` of a quorum queue, requeues kept in the source so that the order of an ordering key holds, dead letters with their reason through a dead letter exchange), `RabbitMqTopology` (a durable topic exchange, a quorum queue per consumer bound by patterns, a dead letter exchange and queue, idempotent) and `RabbitMqConnector`.
- [x] **1.0.1:** the contract battery of the core (`antevemus/aspecification` ^2.15) in place of the private one of 1.0.0, through an `IMessagingBench` over RabbitMQ; the tests of what is RabbitMQ's own stay. The API does not change.

## On demand

Not planned; each one comes when a consumer needs it:

- [ ] A push mode (`basic.consume` with `basic.qos`) for consumers whose volume makes one round trip per message too slow, with a buffer that keeps the order of a key across requeues.
- [ ] A topology over RabbitMQ streams (`x-queue-type: stream`), for consumers that replay.
- [ ] The native requeue of the broker, for a RabbitMQ whose quorum queues put a requeued message back at its place.
