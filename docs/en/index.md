# Antevemus ASpecification RabbitMQ

The official documentation of the package, one page per topic. The same pages exist in Portuguese (Brazil), in `docs/pt-BR`, and every PHP block of these pages is run by the test suite with the answers written in its comments.

> **The RabbitMQ satellite of [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) for PHP 8.2+**  
> A transport and a source for the broker-agnostic messaging hook of the core: the outbox relayed to a topic exchange with publisher confirms and the mandatory flag, and each consumer reading its own quorum queue with manual acknowledgements, the delivery count of the broker, requeues kept in order and a dead letter exchange whose letters carry the reason.

## Documentation Map

### Getting started

- [Installation and quick start](getting-started/index.md): Composer, the requirements and the Quick Start.

### Concepts

- [Topology and publisher confirms](concepts/topology-and-confirms.md): The exchanges and the quorum queues, the patterns, the declarations, the confirms and the mandatory flag.
- [Delivery, ordering and dead letters](concepts/delivery-and-ordering.md): At least once plus the inbox, the pull, the order of a key across requeues and crashes, and the dead letters with their reason.

### API reference

- [API reference](api/reference.md): `RabbitMqConnector`, `RabbitMqTopology`, `RabbitMqTransport` and `RabbitMqSource`.

### Quality

- [Tests](quality/tests.md): The contract battery of the core run against RabbitMQ, the tests of what is RabbitMQ's own, and the mutation testing.

### Project

- [Roadmap](project/roadmap.md): The releases shipped and what may come next.

## Where Is Each Thing

| What | Where |
| :--- | :--- |
| Overview and the Quick Start | [README.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/README.md) |
| What changed in each version | [CHANGELOG.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/CHANGELOG.md) |
| Milestones shipped and planned | [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/ROADMAP.md) |
| License and provenance | [LICENSE](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/LICENSE), [NOTICE.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/NOTICE.md) |
