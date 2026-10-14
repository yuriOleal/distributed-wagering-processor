# Decisões de arquitetura

## Limite transacional

A wallet é a unidade de concorrência. A implementação usa TypeORM para o ciclo de vida do `DataSource`, `QueryRunner` e migrations; SQL explícito dentro do `QueryRunner` torna visíveis os locks e as constraints críticas. Cada operação abre uma transação, serializa a chave de idempotência por advisory lock transacional, bloqueia a linha da wallet com `FOR UPDATE` e grava transação, wallet, ledger, inbox e outbox antes do commit. Advisory locks têm namespace por chave, e nenhuma wallet compartilha um lock global. A ordem de aquisição é inbox, idempotency key, identidade externa e wallet.

`Money` usa `decimal.js` e decimal string com escala fixa de duas casas. Nem entrada, nem domínio, nem evento convertem dinheiro para `number`. PostgreSQL usa `numeric(20,2)`; o driver entrega esses valores como string. O modelo aceita moedas ISO de três letras e checa conflito de moeda, embora os exemplos usem BRL. O contrato rejeita arredondamento implícito: valores com mais de duas casas são inválidos. Entradas acima de 18 dígitos inteiros são inválidas; um crédito que ultrapassaria a capacidade de `numeric(20,2)` é rejeitado com `AMOUNT_OUT_OF_RANGE` antes de chegar ao banco.

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

Taxonomia estável de rejeições: `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `AMOUNT_OUT_OF_RANGE`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`, `SELF_REFERENCE`, `INVALID_REFUND_TARGET`, `INVALID_ROLLBACK_TARGET`, `INVALID_WIN_TARGET`, `REVERSAL_AMOUNT_MISMATCH`, `ALREADY_REVERSED`. Erros de contrato (`INVALID_MONEY`, `INVALID_PAYLOAD`, `INVALID_REFERENCE`, `INVALID_AMOUNT`) são 400, sem criar transação. Conflitos de chave são 409.

## SQS e outbox

O consumidor usa o mesmo caso de uso do HTTP e grava a inbox por `(consumer_name,message_id)` na mesma transação financeira. Só remove a mensagem SQS após o commit. Um crash depois do commit e antes do ack provoca redelivery, mas encontra inbox/transação persistidas e não movimenta saldo novamente. Erros transitórios alteram a visibilidade para retry; a redrive policy da fila manda para a DLQ após cinco recebimentos. Mensagens inválidas são enviadas à DLQ e removidas da fila original apenas após confirmação do envio à DLQ. Em `SIGTERM`, o processo para de buscar mensagens e espera o processamento em andamento.

Eventos concretos implementam o envelope versionado: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged`. O payload da outbox guarda somente dados JSON, incluindo valores monetários como string. Dois publishers usam `FOR UPDATE SKIP LOCKED`. Publicação pode ser repetida se o processo cair entre o envio ao SQS e a marcação `published_at`; `eventId` estável e deduplicação no consumidor externo são necessários. A FIFO fornece uma otimização de transporte; as garantias financeiras continuam no PostgreSQL.

## Operação, autenticação e limites

`/health/live` verifica o processo; `/health/ready` consulta PostgreSQL e SQS. Logs estruturados contêm IDs de correlação, mensagem, transação, wallet e provedor, sem imprimir payload financeiro. `/metrics` expõe status, duplicatas, retries, DLQ, conflitos de lock, atraso da outbox, latência e divergência de reconciliação. Os contadores são locais a cada processo; para totalizar três instâncias, o Prometheus deve coletar todas.

Autenticação não foi implementada, pois a seção 2 permite essa opção e não lhe atribui pontos. `NoopAuthGuard` marca o ponto de extensão. Em produção, eu o substituiria por um guard OIDC com validação de assinatura, issuer, audience e escopos; vincularia `providerId` ou `playerId` aos claims autorizados. Health permaneceria aberto. A fila é canal interno confiável, mas seus IDs ainda passam pelas verificações de domínio.

O projeto mantém o ledger de uma entrada por movimentação, como exigido; não implementa partidas dobradas. Paginação é por `(created_at,id)` descendente. O cursor é opaco por codificação base64url, sem assinatura; uma implantação exposta publicamente deveria assiná-lo. A reconciliação só informa divergências, sem correção automática. A aplicação não agrega métricas entre instâncias. Não há interface de administração para tratar manualmente mensagens na DLQ ou transações `FAILED`.

O cursor preserva os microssegundos do `timestamptz` do PostgreSQL ao serializar a posição da página. O driver converte `created_at` em `Date` com precisão de milissegundos apenas para a exibição; usar essa conversão no cursor poderia pular lançamentos próximos.

## Verificação e segurança

O plano de verificação cobre testes unitários, integração com PostgreSQL e LocalStack reais, três processos, falha após commit, retry, DLQ, migrations reversíveis e contrato HTTP. Na execução local em Docker, os 24 testes unitários/de integração e os 2 testes HTTP passaram. O painel foi testado no navegador com abertura, aposta, replay, estorno, reconciliação e duas apostas concorrentes.

As portas do Compose são publicadas somente em `127.0.0.1`; o processo dentro do container escuta na interface da rede interna. A entrada financeira valida tipos, tamanho, formato e moeda antes do banco. O painel usa `textContent` para dados variáveis e recebe CSP com hashes dos scripts e estilos locais, além de proteção contra enquadramento e `nosniff`. Logs de falha registram códigos, sem mensagens de exceção que possam conter parâmetros SQL ou dados financeiros. O Dockerfile não recebe `.env`, histórico Git ou `node_modules` do host.

O guard sem autenticação é a escolha de escopo permitida pela seção 2 do desafio. `TODO(security)` no código marca autenticação OIDC e limitação de taxa antes de qualquer publicação pública. O Compose usa credenciais de demonstração; em produção elas precisam vir de gestão de segredos. A API não usa cookies ou sessões, portanto CSRF de sessão não se aplica neste ambiente local.

`npm audit --omit=dev` em 14/10/2026 apontou 10 avisos nas dependências transitivas da linha NestJS 10 / Swagger 7 (4 altos, 5 moderados, 1 baixo). As correções propostas pelo audit exigem migração de versão principal do framework. Esta versão fica restrita ao Compose local e suas portas de teste em `127.0.0.1`; antes de expor o serviço, atualizar o stack, repetir o audit e rever as rotas. TODO(security): eliminar os avisos de dependências na migração do framework.
