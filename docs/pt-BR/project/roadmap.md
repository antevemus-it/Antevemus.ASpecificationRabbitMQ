# Roadmap

## Entregue

- **1.0.0:** `RabbitMqTransport` (publisher confirms, a flag mandatory, nacks e falhas de conexão por mensagem), `RabbitMqSource` (pull com ack manual, a contagem de entregas de uma quorum queue, requeues guardados em ordem, mensagens mortas com o motivo por uma exchange de mensagens mortas), `RabbitMqTopology` (uma exchange `topic`, uma quorum queue por consumidor ligada por padrões, uma exchange e uma fila de mensagens mortas, idempotente) e `RabbitMqConnector` (host e credenciais, uma URL `amqp://` ou uma fábrica da aplicação).
- **1.0.1:** a suíte roda a bateria de contrato do núcleo (`antevemus/aspecification` ^2.15) contra o RabbitMQ, no lugar da bateria privada da 1.0.0; os testes do que é próprio do RabbitMQ ficam. A API não muda.

## Sob demanda

Não planejado; cada um vem quando um consumidor precisar:

- [ ] Um modo push (`basic.consume` com `basic.qos`) para um consumidor cujo volume torne lenta uma ida e volta por mensagem, com um buffer que mantenha a ordem de uma chave através de requeues.
- [ ] Uma topologia sobre streams do RabbitMQ (`x-queue-type: stream`), para consumidores que reprocessam.
- [ ] O requeue nativo do broker, para um RabbitMQ cujas quorum queues devolvam a mensagem ao seu lugar.

A visão por marcos está no [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/ROADMAP.md).
