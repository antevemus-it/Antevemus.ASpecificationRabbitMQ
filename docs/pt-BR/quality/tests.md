# Testes

A suíte roda com PHPUnit em quatro grupos, `Unit`, `Contract`, `Broker` e `Property`, na CI de todo push em PHP 8.2, 8.3 e 8.4, contra um RabbitMQ 3.13 iniciado como serviço do workflow:

```bash
docker run -d -p 5672:5672 rabbitmq:3.13
RABBITMQ_HOST=127.0.0.1 RABBITMQ_PORT=5672 vendor/bin/phpunit
```

O broker é lido de `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` e `RABBITMQ_VHOST` (127.0.0.1, 5672, guest, guest, `/`). Um teste que precisa dele é pulado, com o motivo, quando ele não é alcançado; a CI roda com `--fail-on-skipped`, então lá nenhum é pulado. Todo teste declara exchanges e filas próprias, com nomes únicos, e as remove no fim.

## O que é testado

- **A bateria de contrato**, escrita uma vez e rodada sobre duas implementações: o transporte e a fonte em memória do núcleo (a referência) e este pacote contra o broker real. Cada cenário percorre o caminho de uma aplicação: o `MessagePublisher` do núcleo na sua `InMemoryMessageOutbox`, o `OutboxRelay` do núcleo pelo transporte, o `MessageConsumer` do núcleo sobre a fonte, com a sua `InMemoryProcessedMessageStore` e um gerenciador de transação; compara a transcrição (a mensagem, o resultado e a contagem de entregas de cada entrega), os efeitos dos handlers e as mensagens mortas com literais que são os mesmos para as duas. Os cenários: uma ida e volta que preserva todo campo, todo consumidor recebe toda mensagem, a ordem de cada chave através de lotes, uma entrega devolvida com requeue que volta antes das posteriores da chave, uma mensagem morta depois de `maxDeliveries` com motivo e contagem, corpos que não são mensagens mandados para a fila morta e nunca descartados, um nome sem handler, um consumidor de um tenant só, um payload de exatamente 65 535 bytes, um lote de nove chaves, duplicatas absorvidas pela inbox, e as entregas de um consumidor morto passadas ao sucessor.
- **Falhas contra o broker**: um relay que morre entre o confirm e a marcação (a mensagem sai de novo, o efeito roda uma vez); um consumidor cujo socket é cortado depois do commit e antes do ack (tudo o que ele guardava vai, em ordem e com a contagem aumentada, ao próximo consumidor; a mensagem já gravada vira duplicata); um broker inalcançável (o lote inteiro falha, nada é marcado como entregue); uma mensagem para a qual nenhuma fila está ligada (falha pela flag mandatory, sai de novo quando uma fila é ligada); um nack de uma fila cheia; uma exchange inexistente e a reconexão; e o sigilo de todo motivo, relatório, exceção e header de mensagem morta no caminho, com um payload que leva um marcador.
- **O que o broker faz**, medido para que o desenho se apoie nisso: uma quorum queue do RabbitMQ 3.13 põe uma mensagem rejeitada com requeue no fim da fila (o motivo de a fonte guardar o requeue), devolve as entregas de uma conexão fechada ao início, em ordem, e conta as entregas em `x-delivery-count`; uma fila clássica só dá a flag `redelivered`.
- **A fonte e o transporte sozinhos**, contra o broker e contra um broker roteirizado (canais do php-amqplib simulados): o pull, a espera e o intervalo, a contagem de entregas, o requeue guardado em ordem, a mensagem morta e o seu confirm (ack só depois dele), as propriedades e os headers de uma mensagem publicada, cada resposta do broker mapeada no resultado, a conexão mantida até falhar, os argumentos e as suas mensagens.
- **Uma propriedade** sobre intercalações aleatórias de publicação, relay e consumo, com sementes fixas (3 sementes × 150 passos, sobre o par do núcleo e sobre o RabbitMQ): relays que morrem depois de um confirm (15 % das rodadas), consumidores que morrem antes de um ack (10 % dos acks), handlers que falham até duas vezes ou sempre (mensagens venenosas, 10 %), `maxDeliveries` 3. Depois de drenar, nenhuma mensagem confirmada se perde, nenhum efeito roda duas vezes, as mensagens de cada chave são tratadas na ordem de publicação (fora as que foram para a fila morta), e a fila morta tem exatamente o que os relatórios mandaram para ela.
- **A documentação**: todo bloco PHP de `docs/en` e `docs/pt-BR`, e o Início Rápido de cada README, roda com as respostas escritas nos comentários (os blocos que falam com um broker, contra o broker de teste); as duas línguas têm o mesmo código e os mesmos arquivos.

## Teste de mutação

O Infection roda sobre todo o `src/` num workflow agendado, contra um serviço RabbitMQ, com pisos um ponto abaixo da última medida. Medido na 1.0.0: 274 mutantes, 269 mortos, 1 por tempo esgotado, 4 escapados, todos equivalentes (constantes do relógio da espera alteradas em uma parte por milhão ou por mil, e um tempo restante de exatamente zero nanossegundo); MSI 98,54 %, cobertura de código por mutação 100 %.

A suíte tem 168 testes (113 rodam e 55 são pulados sem um broker).
