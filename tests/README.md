# Tests

```bash
composer install
docker run -d -p 5672:5672 rabbitmq:3.13      # or set RABBITMQ_HOST/RABBITMQ_PORT/RABBITMQ_USER/RABBITMQ_PASSWORD/RABBITMQ_VHOST
vendor/bin/phpunit
```

Without a reachable broker the tests that need one are skipped, with the reason (see `phpunit.xml.dist`).

| Folder | What |
| :--- | :--- |
| `Unit/` | No broker: the connector, the topology (declarations recorded on a mocked channel), the transport and the source on a scripted broker (`Support/ScriptedBroker`), the shared helpers, and the documentation (`DocumentationTreeTest`, `DocumentationBlocksTest`; the blocks that talk to a broker are skipped without one). |
| `Contract/` | The contract battery of the core (`MessagingContractTestCase` of `antevemus/aspecification` ^2.15, every scenario and the property sequence), run over RabbitMQ (`RabbitMqContractTest`); the core runs it against its in-memory reference. How a satellite wires it: [Writing a satellite](https://github.com/antevemus-it/Antevemus.ASpecification/blob/main/docs/en/messaging/writing-a-satellite.md), in the documentation of the core. |
| `Broker/` | Against the broker: the transport, the source, the topology, and the failures that are RabbitMQ's own (`FailureScenariosTest`: a socket cut before an acknowledgement, a broker that cannot be reached, the mandatory flag, the secrecy of the dead letter headers and of a connection error). |
| `Support/` | The bench of the battery over RabbitMQ (`RabbitMqBench`, an `IMessagingBench` of the core), the test broker (`TestBroker`), the scripted broker, the effects of the handlers (`EffectLog`) and messages with an alias (`TestMessages`). |

1.0.1: 164 tests (98 without a broker). Mutation testing: see `infection.json5`.
