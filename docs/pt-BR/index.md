# Antevemus ASpecification RabbitMQ

A documentação oficial do pacote, uma página por tema. As mesmas páginas existem em inglês, em `docs/en`, e todo bloco PHP destas páginas é rodado pela suíte de testes com as respostas escritas nos comentários.

> **O satélite RabbitMQ da [ASpecification](https://github.com/antevemus-it/Antevemus.ASpecification) para PHP 8.2+**  
> Um transporte e uma fonte para o gancho de mensageria agnóstico de broker do núcleo: a outbox enviada a uma exchange `topic` com publisher confirms e a flag mandatory, e cada consumidor lendo a própria quorum queue com ack manual, a contagem de entregas do broker, requeues guardados em ordem e uma exchange de mensagens mortas cujas mensagens levam o motivo.

## Mapa da Documentação

### Primeiros passos

- [Instalação e início rápido](getting-started/index.md): Composer, os requisitos e o Início Rápido.

### Conceitos

- [Topologia e publisher confirms](concepts/topology-and-confirms.md): As exchanges e as quorum queues, os padrões, as declarações, os confirms e a flag mandatory.
- [Entrega, ordem e mensagens mortas](concepts/delivery-and-ordering.md): Pelo menos uma vez mais a inbox, o pull, a ordem de uma chave através de requeues e quedas, e as mensagens mortas com o motivo.

### Referência da API

- [Referência da API](api/reference.md): `RabbitMqConnector`, `RabbitMqTopology`, `RabbitMqTransport` e `RabbitMqSource`.

### Qualidade

- [Testes](quality/tests.md): A bateria de contrato do núcleo rodada contra o RabbitMQ, os testes do que é próprio do RabbitMQ, e o teste de mutação.

### Projeto

- [Roadmap](project/roadmap.md): As versões publicadas e o que pode vir depois.

## Onde Está Cada Coisa

| O quê | Onde |
| :--- | :--- |
| Visão geral e o Início Rápido | [README.pt-BR.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/README.pt-BR.md) |
| O que mudou em cada versão | [CHANGELOG.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/CHANGELOG.md) |
| Marcos entregues e planejados | [ROADMAP.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/ROADMAP.md) |
| Licença e procedência | [LICENSE](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/LICENSE), [NOTICE.md](https://github.com/antevemus-it/Antevemus.ASpecificationRabbitMQ/blob/main/NOTICE.md) |
