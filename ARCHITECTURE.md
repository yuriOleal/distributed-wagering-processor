# Decisões de arquitetura

## Limite transacional

A wallet é a unidade de concorrência. A implementação usa TypeORM para o ciclo de vida do `DataSource`, `QueryRunner` e migrations; SQL explícito dentro do `QueryRunner` torna visíveis os locks e as constraints críticas. Cada operação abre uma transação, serializa a chave de idempotência por advisory lock transacional, bloqueia a linha da wallet com `FOR UPDATE` e grava transação, wallet, ledger, inbox e outbox antes do commit. Advisory locks têm namespace por chave, e nenhuma wallet compartilha um lock global. A ordem de aquisição é inbox, idempotency key, identidade externa e wallet.

`Money` usa `decimal.js` e decimal string com escala fixa de duas casas. Nem entrada, nem domínio, nem evento convertem dinheiro para `number`. PostgreSQL usa `numeric(20,2)`; o driver entrega esses valores como string. O modelo aceita moedas ISO de três letras e checa conflito de moeda, embora os exemplos usem BRL. O contrato rejeita arredondamento implícito: valores com mais de duas casas são inválidos.

O saldo inicial positivo cria uma transação `OPENING`, um lançamento `CREDIT` e eventos na mesma transação. Saldo inicial zero não cria lançamento. `version` começa em 1 e cresce somente quando há movimento de saldo; `LOSS` não altera versão ou ledger.

## Barreiras no banco

- `UNIQUE(player_id,currency)` impede wallet duplicada.
- `UNIQUE(idempotency_key)` e `UNIQUE(provider_id,external_transaction_id)` impedem duplicação persistente.
- `UNIQUE(wallet_ledger.transaction_id)` limita a um lançamento por transação financeira.
- `CHECK(balance >= 0)` e `CHECK` de aritmética em cada lançamento impedem saldo negativo e lançamento desequilibrado.
- Trigger proíbe `UPDATE` e `DELETE` no ledger.
- Constraint triggers adiadas conferem, no commit, saldo materializado contra a soma de créditos menos débitos do ledger. Divergência impede o commit.
- Índice único parcial impede duas reversões processadas do mesmo tipo sobre a mesma referência.

O lock da wallet serializa movimentos de saldo e impede lost update entre processos. Os índices e triggers continuam protegendo o estado se uma rota de escrita futura esquecer alguma verificação em código.

## Idempotência e estado

O header `Idempotency-Key` é obrigatório no HTTP; a mensagem SQS transporta a mesma chave. O SHA-256 é calculado sobre JSON canônico com chaves em ordem lexicográfica, apenas campos de negócio e dinheiro normalizado para duas casas. O header e `messageId` não entram no hash. Mesma chave e mesmo hash retornam o ID, estado e saldo armazenados, com `idempotentReplay: true`. Chave igual com outro payload retorna 409. O par `(providerId,externalTransactionId)` também é único, mesmo que a chave varie.

`PENDING → PROCESSED | REJECTED | PENDING_REFERENCE`; `PENDING_REFERENCE → PENDING_REFERENCE | PROCESSED | REJECTED`. `PROCESSED`, `REJECTED` e `FAILED` são terminais. `FAILED` fica reservado para erro permanente auditável; indisponibilidade temporária de PostgreSQL/SQS é retentada, sem transformar uma transação confirmada em `FAILED`.

`BET` debita ou rejeita com `INSUFFICIENT_FUNDS`. `WIN` credita e pode referenciar uma `BET`. `LOSS` registra o resultado sem lançamento. `REFUND` credita uma `BET` processada, com mesmo valor. `ROLLBACK` inverte `BET`, `WIN` ou `REFUND`; se a inversão exigir débito acima do saldo, rejeita com `REVERSAL_INSUFFICIENT_FUNDS`. Referências precisam coincidir em provedor, jogador, wallet, moeda e rodada. Cada referência pode ter no máximo uma reversão processada por tipo, conforme a regra do desafio. O mesmo alvo pode ter um `REFUND` e um `ROLLBACK` distintos; em produção eu exigiria uma regra adicional de negócio antes de permitir essa combinação.

Quando a referência ainda não existe ou não foi processada, a operação permanece `PENDING_REFERENCE`. Um worker a revisita com backoff exponencial de 2 a 300 segundos, por até 8 tentativas ou 24 horas; então rejeita com `REFERENCE_NOT_FOUND`. A chegada posterior da referência é segura: o worker volta a bloquear a wallet e verifica a situação atual antes de mudar o estado.

Taxonomia estável de rejeições: `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`, `SELF_REFERENCE`, `INVALID_REFUND_TARGET`, `INVALID_ROLLBACK_TARGET`, `INVALID_WIN_TARGET`, `REVERSAL_AMOUNT_MISMATCH`, `ALREADY_REVERSED`. Erros de contrato (`INVALID_MONEY`, `INVALID_PAYLOAD`, `INVALID_REFERENCE`, `INVALID_AMOUNT`) são 400, sem criar transação. Conflitos de chave são 409.

## SQS e outbox

O consumidor usa o mesmo caso de uso do HTTP e grava a inbox por `(consumer_name,message_id)` na mesma transação financeira. Só remove a mensagem SQS após o commit. Um crash depois do commit e antes do ack provoca redelivery, mas encontra inbox/transação persistidas e não movimenta saldo novamente. Erros transitórios alteram a visibilidade para retry; a redrive policy da fila manda para a DLQ após cinco recebimentos. Mensagens inválidas são enviadas à DLQ e removidas da fila original apenas após confirmação do envio à DLQ. Em `SIGTERM`, o processo para de buscar mensagens e espera o processamento em andamento.

Eventos concretos implementam o envelope versionado: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged`. O payload da outbox guarda somente dados JSON, incluindo valores monetários como string. Dois publishers usam `FOR UPDATE SKIP LOCKED`. Publicação pode ser repetida se o processo cair entre o envio ao SQS e a marcação `published_at`; `eventId` estável e deduplicação no consumidor externo são necessários. A FIFO fornece uma otimização de transporte; as garantias financeiras continuam no PostgreSQL.

## Operação, autenticação e limites

`/health/live` verifica o processo; `/health/ready` consulta PostgreSQL e SQS. Logs estruturados contêm IDs de correlação, mensagem, transação, wallet e provedor, sem imprimir payload financeiro. `/metrics` expõe status, duplicatas, retries, DLQ, conflitos de lock, atraso da outbox, latência e divergência de reconciliação. Os contadores são locais a cada processo; para totalizar três instâncias, o Prometheus deve coletar todas.

Autenticação não foi implementada, pois a seção 2 permite essa opção e não lhe atribui pontos. `NoopAuthGuard` marca o ponto de extensão. Em produção, eu o substituiria por um guard OIDC com validação de assinatura, issuer, audience e escopos; vincularia `providerId` ou `playerId` aos claims autorizados. Health permaneceria aberto. A fila é canal interno confiável, mas seus IDs ainda passam pelas verificações de domínio.

O projeto mantém o ledger de uma entrada por movimentação, como exigido; não implementa partidas dobradas. Paginação é por `(created_at,id)` descendente. O cursor é opaco por codificação base64url, sem assinatura; uma implantação exposta publicamente deveria assiná-lo. A reconciliação só informa divergências, sem correção automática. A aplicação não agrega métricas entre instâncias. Não há interface de administração para tratar manualmente mensagens na DLQ ou transações `FAILED`.
