# Distributed Wagering Processor

Serviço NestJS/Bun para processar operações de apostas recebidas por HTTP ou SQS. A garantia de consistência está no PostgreSQL: cada wallet é serializada por `SELECT ... FOR UPDATE`, e saldo, transação, ledger, inbox e outbox são gravados em uma transação SQL.

## Requisitos

- Docker Compose v2 (para PostgreSQL 16 e LocalStack com SQS)
- Bun 1.x, caso queira executar comandos fora dos containers

## Subir o ambiente

```sh
docker compose up --build -d
docker compose ps
curl http://localhost:3000/health/ready
```

O serviço `migrate` aplica as migrations e cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events.fifo` antes de iniciar `app`. `GET /health/ready` responde 503 até PostgreSQL e SQS estarem disponíveis. As credenciais `test` e a senha do Compose são somente para ambiente local.

## Interfaces locais de demonstração

- [Painel visual](http://localhost:3000/demo): cria uma wallet, envia operações, mostra saldo e ledger, reconcilia e executa o cenário de duas apostas simultâneas de 80.00 sobre saldo 100.00.
- [Documentação interativa](http://localhost:3000/api): contratos HTTP, exemplos e execução manual dos endpoints. O documento OpenAPI também está em `/api-json`.

As duas interfaces usam os mesmos endpoints e o mesmo serviço de domínio. O painel é uma ferramenta local para demonstrar o comportamento do backend; não contém cálculos monetários nem uma segunda implementação das regras financeiras.

Para rodar três instâncias contra o mesmo banco e as mesmas filas:

```sh
docker compose up --build -d --scale worker=2
```

Isso inicia `app` e dois containers `worker`, todos executando a mesma aplicação; apenas `app` publica a porta HTTP no host.

## Exemplo HTTP

```sh
curl -X POST http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Copie o `id` retornado:

```sh
curl -X POST http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"WALLET_ID","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Endpoints adicionais:

| Método | Rota | Uso |
| --- | --- | --- |
| GET | `/wallets/:walletId` | Saldo e versão |
| GET | `/wallets/:walletId/ledger?cursor=...&limit=50` | Histórico com cursor opaco |
| GET | `/wagering/transactions/:transactionId` | Resultado por ID interno |
| GET | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Resultado por ID do provedor |
| POST | `/wallets/:walletId/reconciliation` | Compara saldo materializado e ledger |
| GET | `/health/live` e `/health/ready` | Saúde do processo e dependências |
| GET | `/metrics` | Métricas em texto Prometheus |

`POST /wagering/transactions` devolve 200 para operação processada, 202 para referência pendente, 422 para rejeição de negócio, 400 para contrato inválido, 409 para conflito de idempotência e 503 para falha transitória de infraestrutura. O replay recebe a resposta armazenada e `idempotentReplay: true`.

## Enviar pela fila

O corpo deve ter `messageId`, `type: "WagerTransactionRequested"`, `occurredAt` e `data` com o mesmo contrato do POST mais `idempotencyKey`. Exemplo com AWS CLI apontando para o LocalStack:

```sh
aws --endpoint-url=http://localhost:4566 sqs send-message \
  --queue-url=http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id=WALLET_ID \
  --message-deduplication-id=msg-123 \
  --message-body='{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-10-14T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"WALLET_ID","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}'
```

## Testes e migrations

```sh
bun install
bun run typecheck
bun run test
docker compose up -d postgres localstack
bun run migration:run
bun scripts/setup-sqs.ts
bun run test:integration
docker compose up --build -d
docker compose run --rm --no-deps -e TEST_BASE_URL=http://app:3000 app bun run test:http
```

Os testes de integração exigem PostgreSQL e LocalStack, mas `app` e `worker` devem estar parados durante sua execução: eles consomem as mesmas filas de teste. Se a aplicação já estiver ligada, execute `docker compose stop app worker` antes de `bun run test:integration`. Para executar os testes de integração na rede do Compose, use `docker compose run --rm --no-deps app bun run test:integration`. O teste HTTP exige `app` ligado.

Os testes de integração usam PostgreSQL e SQS reais. Cobrem 50 reenvios paralelos, disputa de saldo, três processos, referência fora de ordem, precisão do cursor do ledger, inbox/redelivery, outbox com dois publishers, DLQ, falha transitória e um processo encerrado após o commit e antes do ack. Os testes HTTP conferem os status, replay, reconciliação, documentação e cabeçalhos do painel. `bun run migration:revert` remove o schema do desafio; execute esse comando somente em um banco de testes descartável.

## Decisões

Veja [ARCHITECTURE.md](ARCHITECTURE.md) para invariantes, locks, idempotência, eventos, falhas, autenticação e limitações.
