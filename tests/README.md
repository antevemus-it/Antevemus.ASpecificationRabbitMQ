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
| `Contract/` | The contract battery (`MessagingContractTestCase`), run over the core's in-memory pair (`InMemoryContractTest`) and over RabbitMQ (`RabbitMqContractTest`), with the same expected transcripts. |
| `Broker/` | Against the broker: the transport, the source, the topology, and the failure scenarios with the secrecy sweep (`FailureScenariosTest`). |
| `Property/` | Random interleavings of publish, relay and consume with fixed seeds, over both pairs (`InterleavingPropertyTest`). |
| `Support/` | The benches (`MessagingBench`, `InMemoryBench`, `RabbitMqBench`), the test broker (`TestBroker`), the scripted broker, the effects of the handlers (`EffectLog`) and messages with an alias (`TestMessages`). |

1.0.0: 168 tests (113 without a broker). Mutation testing: see `infection.json5`.
