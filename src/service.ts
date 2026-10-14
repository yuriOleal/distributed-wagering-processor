import { createHash, randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { QueryRunner } from 'typeorm';
import { db, WalletRow } from './db';
import { metrics } from './metrics';
import { Direction, DomainError, EventContext, Kind, Money, MoneyProps,
  WagerTransaction, WagerTransactionPendingReference, WagerTransactionProcessed,
  WagerTransactionRejected, Wallet, WalletBalanceChanged, WalletLedgerEntry,
  IntegrationEvent, InboxMessage, OutboxMessage } from './domain';

export type WagerInput = {
  providerId: string; externalTransactionId: string; playerId: string; walletId: string;
  roundId: string; gameId: string; kind: Kind; money: MoneyProps;
  referenceExternalTransactionId?: string;
};
type Inbox = { consumerName: string; messageId: string; payloadHash: string };
type Result = { transactionId: string; status: string; balance: MoneyProps | null;
  failureCode?: string; idempotentReplay: boolean };
type TxRow = Record<string, any>;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TEXT = /^[\w:.-]{1,128}$/;
const isText = (value: unknown): value is string => typeof value === 'string' && TEXT.test(value);
const isUuid = (value: unknown): value is string => typeof value === 'string' && UUID.test(value);

export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.entries(value).filter(([, child]) => child !== undefined)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, child]) => `${JSON.stringify(key)}:${canonical(child)}`).join(',')}}`;
  }
  return JSON.stringify(value);
}
export function sha256(value: unknown): string {
  return createHash('sha256').update(canonical(value)).digest('hex');
}

function validateInput(raw: WagerInput): { input: WagerInput; money: Money } {
  if (!raw || !isText(raw.providerId) || !isText(raw.externalTransactionId) ||
    !isUuid(raw.playerId) || !isUuid(raw.walletId) ||
    !isText(raw.roundId) || !isText(raw.gameId) ||
    !['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'].includes(raw.kind) ||
    (raw.referenceExternalTransactionId !== undefined && !isText(raw.referenceExternalTransactionId)))
    throw new DomainError('INVALID_PAYLOAD');
  const money = Money.from(raw.money);
  if (!money.isPositive()) throw new DomainError('INVALID_AMOUNT');
  if ((raw.kind === 'REFUND' || raw.kind === 'ROLLBACK') && !raw.referenceExternalTransactionId)
    throw new DomainError('INVALID_REFERENCE');
  if (['BET', 'LOSS'].includes(raw.kind) && raw.referenceExternalTransactionId)
    throw new DomainError('INVALID_REFERENCE');
  const input: WagerInput = {
    providerId: raw.providerId,
    externalTransactionId: raw.externalTransactionId,
    playerId: raw.playerId,
    walletId: raw.walletId,
    roundId: raw.roundId,
    gameId: raw.gameId,
    kind: raw.kind,
    money: money.toJSON(),
    ...(raw.referenceExternalTransactionId ?
      { referenceExternalTransactionId: raw.referenceExternalTransactionId } : {}),
  };
  return { input, money };
}

function walletFrom(row: TxRow): Wallet {
  return Wallet.rehydrate({ id: row.id, playerId: row.player_id, currency: row.currency,
    balance: Money.from({ amount: row.balance, currency: row.currency }), version: row.version,
    createdAt: row.created_at, updatedAt: row.updated_at });
}
function transactionFrom(row: TxRow): WagerTransaction {
  return WagerTransaction.rehydrate({ id: row.id, providerId: row.provider_id,
    externalTransactionId: row.external_transaction_id, idempotencyKey: row.idempotency_key,
    payloadHash: row.payload_hash, walletId: row.wallet_id, playerId: row.player_id,
    roundId: row.round_id, gameId: row.game_id, kind: row.kind,
    money: Money.from({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId: row.reference_external_transaction_id ?? undefined,
    createdAt: row.created_at, status: row.status,
    referenceTransactionId: row.reference_transaction_id ?? undefined,
    failureCode: row.failure_code ?? undefined, processedAt: row.processed_at ?? undefined });
}
function resultOf(row: TxRow, replay: boolean): Result {
  return { transactionId: row.id, status: row.status,
    balance: row.result_balance === null || row.result_balance === undefined ? null :
      { amount: row.result_balance, currency: row.currency },
    ...(row.failure_code ? { failureCode: row.failure_code } : {}), idempotentReplay: replay };
}

@Injectable()
export class WagerService {
  private async withTx<T>(fn: (q: QueryRunner) => Promise<T>): Promise<T> {
    const q = db.createQueryRunner();
    await q.connect();
    try {
      await q.startTransaction();
      const result = await fn(q); await q.commitTransaction(); return result;
    }
    catch (error: any) {
      if (q.isTransactionActive) await q.rollbackTransaction();
      if (error?.code === '40P01' || error?.code === '55P03') metrics.lockConflicts++;
      throw error;
    }
    finally { await q.release(); }
  }
  private async enqueue(q: QueryRunner, event: IntegrationEvent<unknown>): Promise<void> {
    const message = OutboxMessage.enqueue(event);
    await q.query(`INSERT INTO outbox_messages(id, aggregate_id, event_type, payload)
      VALUES ($1,$2,$3,$4)`, [message.id, message.aggregateId, message.eventType,
        JSON.stringify(message.payload)]);
  }
  private async getWalletLocked(q: QueryRunner, id: string): Promise<Wallet> {
    const row = await q.manager.getRepository(WalletRow).findOne({ where: { id },
      lock: { mode: 'pessimistic_write' } });
    if (!row) throw new DomainError('WALLET_NOT_FOUND');
    return Wallet.rehydrate({ id: row.id, playerId: row.playerId, currency: row.currency,
      balance: Money.from({ amount: row.balance, currency: row.currency }),
      version: row.version, createdAt: row.createdAt, updatedAt: row.updatedAt });
  }
  async createWallet(playerId: string, initial: MoneyProps): Promise<unknown> {
    if (!isUuid(playerId)) throw new DomainError('INVALID_PAYLOAD');
    const money = Money.from(initial);
    if (money.isNegative()) throw new DomainError('INVALID_AMOUNT');
    return this.withTx(async q => {
      const wallet = Wallet.open({ id: randomUUID(), playerId, initialBalance: money });
      try {
        await q.query(`INSERT INTO wallets(id, player_id, currency, balance, version)
          VALUES ($1,$2,$3,$4,1)`, [wallet.id, playerId, money.currency, money.toString()]);
      } catch (e: any) {
        if (e.code === '23505') throw new DomainError('WALLET_ALREADY_EXISTS');
        throw e;
      }
      if (money.isPositive()) {
        const txId = randomUUID();
        await q.query(`INSERT INTO wager_transactions(id, provider_id, external_transaction_id,
          idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind,
          amount, currency, status, result_balance, processed_at)
          VALUES ($1,'internal',$2,$3,$4,$5,$6,'opening','opening','OPENING',$7,$8,'PROCESSED',$7,now())`,
          [txId, txId, `internal:${txId}`, sha256({ walletId: wallet.id, amount: money.toJSON() }),
            wallet.id, playerId, money.toString(), money.currency]);
        const entry = WalletLedgerEntry.create({ id: randomUUID(), walletId: wallet.id,
          transactionId: txId, direction: 'CREDIT', money,
          balanceBefore: Money.zero(money.currency), balanceAfter: money });
        await this.insertEntry(q, entry);
        await this.enqueue(q, WagerTransactionProcessed.from(txId, wallet.id, 'PROCESSED', { correlationId: txId }));
        await this.enqueue(q, WalletBalanceChanged.from(wallet, entry, { correlationId: txId }));
      }
      return { id: wallet.id, playerId, balance: money.toJSON(), version: 1 };
    });
  }
  private async insertEntry(q: QueryRunner, e: WalletLedgerEntry): Promise<void> {
    await q.query(`INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,
      balance_before,balance_after) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [e.id, e.walletId, e.transactionId, e.direction, e.money.toString(), e.money.currency,
        e.balanceBefore.toString(), e.balanceAfter.toString()]);
  }
  async submit(raw: WagerInput, key: string, inbox?: Inbox): Promise<Result> {
    const started = performance.now();
    try {
      const result = await this.submitTransaction(raw, key, inbox);
      if (result.idempotentReplay) metrics.duplicates++;
      else if (result.status in metrics.transactions)
        metrics.transactions[result.status as keyof typeof metrics.transactions]++;
      return result;
    } finally {
      metrics.processingCount++;
      metrics.processingSumMs += performance.now() - started;
    }
  }
  private async submitTransaction(raw: WagerInput, key: string, inbox?: Inbox): Promise<Result> {
    const { input, money } = validateInput(raw);
    if (!key || typeof key !== 'string' || key.length > 200) throw new DomainError('MISSING_IDEMPOTENCY_KEY');
    const hash = sha256(input);
    return this.withTx(async q => {
      // Both locks are scoped to a SQL transaction. Hash collisions only serialize unrelated requests.
      if (inbox) {
        await q.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [`inbox:${inbox.consumerName}:${inbox.messageId}`]);
        const [seen] = await q.query(`SELECT payload_hash FROM inbox_messages
          WHERE consumer_name=$1 AND message_id=$2`, [inbox.consumerName, inbox.messageId]);
        if (seen) {
          if (seen.payload_hash !== inbox.payloadHash) throw new DomainError('INBOX_MESSAGE_CONFLICT');
          const [previous] = await q.query('SELECT * FROM wager_transactions WHERE idempotency_key=$1', [key]);
          if (!previous || previous.payload_hash !== hash) throw new DomainError('INBOX_MESSAGE_CONFLICT');
          return resultOf(previous, true);
        }
      }
      await q.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [`idem:${key}`]);
      await q.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',
        [`external:${input.providerId}:${input.externalTransactionId}`]);
      const [byKey] = await q.query('SELECT * FROM wager_transactions WHERE idempotency_key=$1', [key]);
      const [byExternal] = await q.query(`SELECT * FROM wager_transactions
        WHERE provider_id=$1 AND external_transaction_id=$2`, [input.providerId, input.externalTransactionId]);
      const existing = byKey || byExternal;
      if (existing) {
        if (existing.idempotency_key !== key || existing.payload_hash !== hash)
          throw new DomainError('IDEMPOTENCY_CONFLICT');
        if (inbox) await this.recordInbox(q, inbox);
        return resultOf(existing, true);
      }
      const wallet = await this.getWalletLocked(q, input.walletId);
      if (wallet.playerId !== input.playerId) throw new DomainError('PLAYER_WALLET_MISMATCH');
      if (wallet.currency !== money.currency) throw new DomainError('CURRENCY_MISMATCH');
      const tx = WagerTransaction.create({ id: randomUUID(), providerId: input.providerId,
        externalTransactionId: input.externalTransactionId, idempotencyKey: key, payloadHash: hash,
        walletId: input.walletId, playerId: input.playerId, roundId: input.roundId,
        gameId: input.gameId, kind: input.kind, money,
        referenceExternalTransactionId: input.referenceExternalTransactionId });
      await q.query(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,
        idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,
        reference_external_transaction_id,status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING')`,
        [tx.id, tx.providerId, tx.externalTransactionId, key, hash, tx.walletId,
          tx.playerId, tx.roundId, tx.gameId, tx.kind, money.toString(), money.currency,
          tx.referenceExternalTransactionId ?? null]);
      const result = await this.process(q, tx, wallet, { correlationId: tx.id,
        causationId: inbox?.messageId });
      if (inbox) await this.recordInbox(q, inbox);
      return result;
    });
  }
  private async recordInbox(q: QueryRunner, inbox: Inbox): Promise<void> {
    const message = InboxMessage.receive(inbox);
    message.markProcessed(new Date());
    await q.query(`INSERT INTO inbox_messages(consumer_name,message_id,payload_hash)
      VALUES ($1,$2,$3)`, [message.consumerName, message.messageId, message.payloadHash]);
  }
  private async process(q: QueryRunner, tx: WagerTransaction, wallet: Wallet,
    ctx: EventContext, attempts = 0): Promise<Result> {
    let reference: WagerTransaction | undefined;
    if (tx.referenceExternalTransactionId) {
      if (tx.referenceExternalTransactionId === tx.externalTransactionId)
        return this.reject(q, tx, wallet, 'SELF_REFERENCE', ctx);
      const [row] = await q.query(`SELECT * FROM wager_transactions
        WHERE provider_id=$1 AND external_transaction_id=$2`,
        [tx.providerId, tx.referenceExternalTransactionId]);
      if (!row || row.status === 'PENDING' || row.status === 'PENDING_REFERENCE') {
        if (attempts >= 8 || Date.now() - tx.createdAt.getTime() >= 86400000)
          return this.reject(q, tx, wallet, 'REFERENCE_NOT_FOUND', ctx);
        tx.markPendingReference();
        await q.query(`UPDATE wager_transactions SET status='PENDING_REFERENCE',
          reference_attempts=$2, next_attempt_at=now() + ($3 || ' seconds')::interval,
          result_balance=$4 WHERE id=$1`, [tx.id, attempts + 1,
          Math.min(300, 2 ** (attempts + 1)), wallet.balance.toString()]);
        if (attempts === 0) await this.enqueue(q, WagerTransactionPendingReference.from(tx.id, wallet.id, ctx));
        return { transactionId: tx.id, status: 'PENDING_REFERENCE', balance: wallet.balance.toJSON(),
          idempotentReplay: false };
      }
      reference = transactionFrom(row);
      if (reference.status !== 'PROCESSED') return this.reject(q, tx, wallet, 'REFERENCE_NOT_PROCESSED', ctx);
      if (reference.walletId !== tx.walletId || reference.playerId !== tx.playerId ||
        reference.roundId !== tx.roundId || reference.money.currency !== tx.money.currency)
        return this.reject(q, tx, wallet, 'REFERENCE_MISMATCH', ctx);
      if (tx.kind === 'REFUND' && reference.kind !== 'BET')
        return this.reject(q, tx, wallet, 'INVALID_REFUND_TARGET', ctx);
      if (tx.kind === 'ROLLBACK' && !['BET', 'WIN', 'REFUND'].includes(reference.kind))
        return this.reject(q, tx, wallet, 'INVALID_ROLLBACK_TARGET', ctx);
      if (tx.kind === 'WIN' && reference.kind !== 'BET')
        return this.reject(q, tx, wallet, 'INVALID_WIN_TARGET', ctx);
      if ((tx.kind === 'REFUND' || tx.kind === 'ROLLBACK') && !tx.money.equals(reference.money))
        return this.reject(q, tx, wallet, 'REVERSAL_AMOUNT_MISMATCH', ctx);
      if (tx.kind === 'REFUND' || tx.kind === 'ROLLBACK') {
        const [prior] = await q.query(`SELECT id FROM wager_transactions
          WHERE kind=$1 AND reference_transaction_id=$2 AND status='PROCESSED'`,
          [tx.kind, reference.id]);
        if (prior) return this.reject(q, tx, wallet, 'ALREADY_REVERSED', ctx);
      }
    }
    if (tx.kind === 'LOSS') {
      tx.markProcessed(reference?.id, new Date());
      await q.query(`UPDATE wager_transactions SET status='PROCESSED',processed_at=now(),
        reference_transaction_id=$2,result_balance=$3 WHERE id=$1`,
        [tx.id, reference?.id ?? null, wallet.balance.toString()]);
      await this.enqueue(q, WagerTransactionProcessed.from(tx.id, wallet.id, tx.status, ctx));
      return { transactionId: tx.id, status: tx.status, balance: wallet.balance.toJSON(), idempotentReplay: false };
    }
    const direction: Direction = tx.ledgerDirectionFor(reference);
    let movement: { before: Money; after: Money };
    try {
      movement = direction === 'DEBIT'
        ? wallet.debit(tx.money, tx.kind === 'ROLLBACK' ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS')
        : wallet.credit(tx.money);
    } catch (error) {
      if (error instanceof DomainError) return this.reject(q, tx, wallet, error.code, ctx);
      throw error;
    }
    tx.markProcessed(reference?.id, new Date());
    await q.query(`UPDATE wallets SET balance=$2,version=$3,updated_at=now() WHERE id=$1`,
      [wallet.id, wallet.balance.toString(), wallet.version]);
    await q.query(`UPDATE wager_transactions SET status='PROCESSED',processed_at=now(),
      reference_transaction_id=$2,result_balance=$3 WHERE id=$1`,
      [tx.id, reference?.id ?? null, wallet.balance.toString()]);
    const entry = WalletLedgerEntry.create({ id: randomUUID(), walletId: wallet.id,
      transactionId: tx.id, direction, money: tx.money,
      balanceBefore: movement.before, balanceAfter: movement.after });
    await this.insertEntry(q, entry);
    await this.enqueue(q, WagerTransactionProcessed.from(tx.id, wallet.id, tx.status, ctx));
    await this.enqueue(q, WalletBalanceChanged.from(wallet, entry, ctx));
    return { transactionId: tx.id, status: tx.status, balance: wallet.balance.toJSON(), idempotentReplay: false };
  }
  private async reject(q: QueryRunner, tx: WagerTransaction, wallet: Wallet,
    code: string, ctx: EventContext): Promise<Result> {
    tx.reject(code);
    await q.query(`UPDATE wager_transactions SET status='REJECTED',failure_code=$2,
      result_balance=$3,next_attempt_at=NULL WHERE id=$1`, [tx.id, code, wallet.balance.toString()]);
    await this.enqueue(q, WagerTransactionRejected.from(tx.id, wallet.id, code, ctx));
    return { transactionId: tx.id, status: 'REJECTED', balance: wallet.balance.toJSON(),
      failureCode: code, idempotentReplay: false };
  }
  async retryReference(id: string): Promise<void> {
    const [candidate] = await db.query(`SELECT wallet_id FROM wager_transactions
      WHERE id=$1 AND status='PENDING_REFERENCE' AND next_attempt_at<=now()`, [id]);
    if (!candidate) return;
    await this.withTx(async q => {
      const wallet = await this.getWalletLocked(q, candidate.wallet_id);
      const [row] = await q.query('SELECT * FROM wager_transactions WHERE id=$1 FOR UPDATE', [id]);
      if (!row || row.status !== 'PENDING_REFERENCE' || new Date(row.next_attempt_at) > new Date()) return;
      const result = await this.process(q, transactionFrom(row), wallet, { correlationId: id }, row.reference_attempts);
      metrics.retries++;
      if (result.status !== 'PENDING_REFERENCE')
        metrics.transactions[result.status as keyof typeof metrics.transactions]++;
    });
  }
  async dueReferences(): Promise<string[]> {
    const rows = await db.query(`SELECT id FROM wager_transactions WHERE status='PENDING_REFERENCE'
      AND next_attempt_at<=now() ORDER BY next_attempt_at LIMIT 100`);
    return rows.map((r: { id: string }) => r.id);
  }
  async wallet(id: string): Promise<unknown> {
    if (!UUID.test(id)) throw new DomainError('INVALID_PAYLOAD');
    const [row] = await db.query('SELECT * FROM wallets WHERE id=$1', [id]);
    if (!row) throw new DomainError('WALLET_NOT_FOUND');
    return { id: row.id, playerId: row.player_id,
      balance: { amount: row.balance, currency: row.currency }, version: row.version };
  }
  async transaction(id: string): Promise<unknown> {
    if (!UUID.test(id)) throw new DomainError('INVALID_PAYLOAD');
    const [row] = await db.query('SELECT * FROM wager_transactions WHERE id=$1', [id]);
    if (!row) throw new DomainError('TRANSACTION_NOT_FOUND');
    return this.transactionView(row);
  }
  async providerTransaction(providerId: string, externalId: string): Promise<unknown> {
    const [row] = await db.query(`SELECT * FROM wager_transactions
      WHERE provider_id=$1 AND external_transaction_id=$2`, [providerId, externalId]);
    if (!row) throw new DomainError('TRANSACTION_NOT_FOUND');
    return this.transactionView(row);
  }
  private transactionView(row: TxRow): unknown {
    return { transactionId: row.id, providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id, kind: row.kind,
      money: { amount: row.amount, currency: row.currency }, status: row.status,
      balance: row.result_balance === null ? null : { amount: row.result_balance, currency: row.currency },
      failureCode: row.failure_code, referenceTransactionId: row.reference_transaction_id };
  }
  async ledger(walletId: string, cursor?: string, requestedLimit?: string): Promise<unknown> {
    if (!UUID.test(walletId)) throw new DomainError('INVALID_PAYLOAD');
    const limit = requestedLimit === undefined ? 50 : Number(requestedLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new DomainError('INVALID_PAYLOAD');
    let date: string | null = null; let id: string | null = null;
    if (cursor) {
      try {
        const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString());
        if (typeof decoded.date !== 'string' || !Number.isFinite(Date.parse(decoded.date)) ||
          !UUID.test(decoded.id)) throw new Error();
        date = decoded.date; id = decoded.id;
      } catch { throw new DomainError('INVALID_CURSOR'); }
    }
    // Keep PostgreSQL's microseconds: the pg Date conversion truncates to milliseconds.
    const rows = await db.query(`SELECT *,
      to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_created_at
      FROM wallet_ledger WHERE wallet_id=$1
      AND ($2::timestamptz IS NULL OR (created_at,id)<($2::timestamptz,$3::uuid))
      ORDER BY created_at DESC,id DESC LIMIT $4`, [walletId, date, id, limit + 1]);
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    return { entries: page.map((r: TxRow) => ({ id: r.id, transactionId: r.transaction_id,
      direction: r.direction, money: { amount: r.amount, currency: r.currency },
      balanceBefore: { amount: r.balance_before, currency: r.currency },
      balanceAfter: { amount: r.balance_after, currency: r.currency }, createdAt: r.created_at })),
      nextCursor: rows.length > limit && last ? Buffer.from(JSON.stringify({ date: last.cursor_created_at,
        id: last.id })).toString('base64url') : null };
  }
  async reconcile(walletId: string): Promise<unknown> {
    if (!UUID.test(walletId)) throw new DomainError('INVALID_PAYLOAD');
    const [row] = await db.query(`SELECT w.balance,w.currency,COUNT(l.id)::int AS count,
      COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0)::numeric(20,2) AS calculated
      FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id
      WHERE w.id=$1 GROUP BY w.id`, [walletId]);
    if (!row) throw new DomainError('WALLET_NOT_FOUND');
    const stored = Money.from({ amount: row.balance, currency: row.currency });
    const calculated = Money.from({ amount: row.calculated, currency: row.currency });
    const difference = stored.subtract(calculated);
    if (!difference.isZero()) {
      metrics.reconciliationDivergences++;
      console.error(JSON.stringify({ event: 'reconciliation_divergence', walletId }));
    }
    return { walletId, storedBalance: stored.toJSON(), calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(), consistent: difference.isZero(), checkedEntries: row.count };
  }
}
