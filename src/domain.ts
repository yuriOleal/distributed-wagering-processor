import Decimal from 'decimal.js';
import { randomUUID } from 'node:crypto';

export type MoneyProps = { amount: string; currency: string };
// PostgreSQL numeric(20,2) has at most 18 digits before the decimal point.
const AMOUNT = /^(0|[1-9]\d{0,17})\.\d{2}$/;
const CURRENCY = /^[A-Z]{3}$/;
const MAX_MONEY = new Decimal('999999999999999999.99');

export class DomainError extends Error {
  constructor(public readonly code: string, message = code) { super(message); }
}

export class Money {
  private constructor(private readonly value: Decimal, public readonly currency: string) {}
  private static checked(value: Decimal, currency: string): Money {
    if (value.abs().greaterThan(MAX_MONEY)) throw new DomainError('AMOUNT_OUT_OF_RANGE');
    return new Money(value, currency);
  }
  static from(p: MoneyProps): Money {
    if (!p || typeof p.amount !== 'string' || !AMOUNT.test(p.amount) ||
      typeof p.currency !== 'string' || !CURRENCY.test(p.currency))
      throw new DomainError('INVALID_MONEY');
    const value = new Decimal(p.amount);
    if (!value.isFinite() || value.decimalPlaces() > 2) throw new DomainError('INVALID_MONEY');
    return new Money(value, p.currency);
  }
  static zero(currency: string): Money { return Money.from({ amount: '0.00', currency }); }
  add(other: Money): Money { this.assertCurrency(other); return Money.checked(this.value.add(other.value), this.currency); }
  subtract(other: Money): Money { this.assertCurrency(other); return Money.checked(this.value.sub(other.value), this.currency); }
  negate(): Money { return new Money(this.value.neg(), this.currency); }
  isZero(): boolean { return this.value.isZero(); }
  isPositive(): boolean { return this.value.greaterThan(0); }
  isNegative(): boolean { return this.value.lessThan(0); }
  isLessThan(other: Money): boolean { this.assertCurrency(other); return this.value.lessThan(other.value); }
  equals(other: Money): boolean { return this.currency === other.currency && this.value.equals(other.value); }
  toJSON(): MoneyProps { return { amount: this.value.toFixed(2), currency: this.currency }; }
  toString(): string { return this.value.toFixed(2); }
  private assertCurrency(other: Money): void {
    if (this.currency !== other.currency) throw new DomainError('CURRENCY_MISMATCH');
  }
}

export class Wallet {
  private constructor(
    public readonly id: string, public readonly playerId: string, public readonly currency: string,
    private _balance: Money, private _version: number, public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}
  static open(p: { id: string; playerId: string; initialBalance: Money }): Wallet {
    if (p.initialBalance.isNegative()) throw new DomainError('NEGATIVE_BALANCE');
    return new Wallet(p.id, p.playerId, p.initialBalance.currency, p.initialBalance, 1, new Date(), new Date());
  }
  static rehydrate(p: { id: string; playerId: string; currency: string; balance: Money; version: number; createdAt: Date; updatedAt: Date }): Wallet {
    return new Wallet(p.id, p.playerId, p.currency, p.balance, p.version, p.createdAt, p.updatedAt);
  }
  get balance(): Money { return this._balance; }
  get version(): number { return this._version; }
  get updatedAt(): Date { return this._updatedAt; }
  debit(money: Money, failureCode = 'INSUFFICIENT_FUNDS'): { before: Money; after: Money } {
    this.assertMoney(money);
    if (this._balance.isLessThan(money)) throw new DomainError(failureCode);
    const before = this._balance;
    this._balance = before.subtract(money);
    this._version++;
    this._updatedAt = new Date();
    return { before, after: this._balance };
  }
  credit(money: Money): { before: Money; after: Money } {
    this.assertMoney(money);
    const before = this._balance;
    this._balance = before.add(money);
    this._version++;
    this._updatedAt = new Date();
    return { before, after: this._balance };
  }
  private assertMoney(money: Money): void {
    if (money.currency !== this.currency) throw new DomainError('CURRENCY_MISMATCH');
    if (!money.isPositive()) throw new DomainError('INVALID_AMOUNT');
  }
}

export type Kind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type Status = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export type Direction = 'DEBIT' | 'CREDIT';

export class WagerTransaction {
  private constructor(
    public readonly id: string, public readonly providerId: string,
    public readonly externalTransactionId: string, public readonly idempotencyKey: string,
    public readonly payloadHash: string, public readonly walletId: string,
    public readonly playerId: string, public readonly roundId: string,
    public readonly gameId: string, public readonly kind: Kind, public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date, private _status: Status,
    private _referenceTransactionId?: string, private _failureCode?: string,
    private _processedAt?: Date,
  ) {}
  static create(p: {
    id: string; providerId: string; externalTransactionId: string; idempotencyKey: string;
    payloadHash: string; walletId: string; playerId: string; roundId: string; gameId: string;
    kind: Kind; money: Money; referenceExternalTransactionId?: string;
  }): WagerTransaction {
    if (p.kind === 'OPENING') throw new DomainError('OPENING_INTERNAL_ONLY');
    if (((p.kind === 'REFUND' || p.kind === 'ROLLBACK') && !p.referenceExternalTransactionId) ||
      ((p.kind === 'BET' || p.kind === 'LOSS') && !!p.referenceExternalTransactionId))
      throw new DomainError('INVALID_REFERENCE');
    if (!p.money.isPositive()) throw new DomainError('INVALID_AMOUNT');
    return new WagerTransaction(p.id, p.providerId, p.externalTransactionId, p.idempotencyKey,
      p.payloadHash, p.walletId, p.playerId, p.roundId, p.gameId, p.kind, p.money,
      p.referenceExternalTransactionId, new Date(), 'PENDING');
  }
  static rehydrate(p: {
    id: string; providerId: string; externalTransactionId: string; idempotencyKey: string;
    payloadHash: string; walletId: string; playerId: string; roundId: string; gameId: string;
    kind: Kind; money: Money; referenceExternalTransactionId?: string; createdAt: Date;
    status: Status; referenceTransactionId?: string; failureCode?: string; processedAt?: Date;
  }): WagerTransaction {
    return new WagerTransaction(p.id, p.providerId, p.externalTransactionId, p.idempotencyKey,
      p.payloadHash, p.walletId, p.playerId, p.roundId, p.gameId, p.kind, p.money,
      p.referenceExternalTransactionId, p.createdAt, p.status, p.referenceTransactionId,
      p.failureCode, p.processedAt);
  }
  get status(): Status { return this._status; }
  get referenceTransactionId(): string | undefined { return this._referenceTransactionId; }
  get failureCode(): string | undefined { return this._failureCode; }
  get processedAt(): Date | undefined { return this._processedAt; }
  isTerminal(): boolean { return ['PROCESSED', 'REJECTED', 'FAILED'].includes(this._status); }
  affectsBalance(): boolean { return this.kind !== 'LOSS'; }
  requiresReference(): boolean { return this.kind === 'REFUND' || this.kind === 'ROLLBACK'; }
  matchesPayload(hash: string): boolean { return this.payloadHash === hash; }
  ledgerDirectionFor(reference?: WagerTransaction): Direction {
    if (this.kind === 'BET') return 'DEBIT';
    if (this.kind === 'WIN' || this.kind === 'REFUND' || this.kind === 'OPENING') return 'CREDIT';
    if (this.kind === 'ROLLBACK' && reference) return reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
    throw new DomainError('NO_LEDGER_DIRECTION');
  }
  markProcessed(referenceId: string | undefined, at: Date): void {
    this.assertActive(); this._status = 'PROCESSED'; this._referenceTransactionId = referenceId; this._processedAt = at;
  }
  markPendingReference(): void { this.assertActive(); this._status = 'PENDING_REFERENCE'; }
  reject(code: string): void { this.assertActive(); this._status = 'REJECTED'; this._failureCode = code; }
  fail(code: string): void { this.assertActive(); this._status = 'FAILED'; this._failureCode = code; }
  private assertActive(): void { if (this.isTerminal()) throw new DomainError('INVALID_TRANSACTION_STATE'); }
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string, public readonly walletId: string, public readonly transactionId: string,
    public readonly direction: Direction, public readonly money: Money,
    public readonly balanceBefore: Money, public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}
  static create(p: { id: string; walletId: string; transactionId: string; direction: Direction;
    money: Money; balanceBefore: Money; balanceAfter: Money }): WalletLedgerEntry {
    const entry = new WalletLedgerEntry(p.id, p.walletId, p.transactionId, p.direction,
      p.money, p.balanceBefore, p.balanceAfter, new Date());
    if (!entry.isBalanced()) throw new DomainError('UNBALANCED_LEDGER');
    return entry;
  }
  static rehydrate(p: { id: string; walletId: string; transactionId: string; direction: Direction;
    money: Money; balanceBefore: Money; balanceAfter: Money; createdAt: Date }): WalletLedgerEntry {
    return new WalletLedgerEntry(p.id, p.walletId, p.transactionId, p.direction,
      p.money, p.balanceBefore, p.balanceAfter, p.createdAt);
  }
  isBalanced(): boolean {
    const after = this.direction === 'CREDIT' ? this.balanceBefore.add(this.money) : this.balanceBefore.subtract(this.money);
    return after.equals(this.balanceAfter);
  }
}

export class InboxMessage {
  private constructor(public readonly messageId: string, public readonly consumerName: string,
    public readonly payloadHash: string, public readonly receivedAt: Date, private _processedAt?: Date) {}
  static receive(p: { messageId: string; consumerName: string; payloadHash: string }): InboxMessage {
    return new InboxMessage(p.messageId, p.consumerName, p.payloadHash, new Date());
  }
  static rehydrate(p: { messageId: string; consumerName: string; payloadHash: string;
    receivedAt: Date; processedAt?: Date }): InboxMessage {
    return new InboxMessage(p.messageId, p.consumerName, p.payloadHash, p.receivedAt, p.processedAt);
  }
  get processedAt(): Date | undefined { return this._processedAt; }
  isProcessed(): boolean { return !!this._processedAt; }
  markProcessed(at: Date): void { this._processedAt = at; }
}

export interface EventContext { correlationId: string; causationId?: string }
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  protected constructor(public readonly eventId: string, public readonly aggregateId: string,
    public readonly correlationId: string, public readonly causationId: string | undefined,
    public readonly occurredAt: Date, public readonly data: Readonly<T>) {}
  toJSON() {
    return { eventId: this.eventId, eventType: this.eventType, aggregateId: this.aggregateId,
      correlationId: this.correlationId, causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(), version: this.version, data: this.data };
  }
}
export class WagerTransactionProcessed extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WagerTransactionProcessed'; readonly version = 1;
  static from(id: string, walletId: string, status: Status, ctx: EventContext) {
    return new WagerTransactionProcessed(randomUUID(), walletId, ctx.correlationId, ctx.causationId,
      new Date(), { transactionId: id, status });
  }
}
export class WagerTransactionRejected extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WagerTransactionRejected'; readonly version = 1;
  static from(id: string, walletId: string, failureCode: string, ctx: EventContext) {
    return new WagerTransactionRejected(randomUUID(), walletId, ctx.correlationId, ctx.causationId,
      new Date(), { transactionId: id, failureCode });
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WagerTransactionPendingReference'; readonly version = 1;
  static from(id: string, walletId: string, ctx: EventContext) {
    return new WagerTransactionPendingReference(randomUUID(), walletId, ctx.correlationId, ctx.causationId,
      new Date(), { transactionId: id });
  }
}
export class WalletBalanceChanged extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WalletBalanceChanged'; readonly version = 1;
  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext) {
    return new WalletBalanceChanged(randomUUID(), wallet.id, ctx.correlationId, ctx.causationId,
      new Date(), { walletId: wallet.id, transactionId: entry.transactionId, direction: entry.direction,
        money: entry.money.toJSON(), balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(), walletVersion: wallet.version });
  }
}

export class OutboxMessage {
  private constructor(public readonly id: string, public readonly aggregateId: string,
    public readonly eventType: string, public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date, private _attempts: number, private _nextAttemptAt?: Date,
    private _publishedAt?: Date) {}
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(event.eventId, event.aggregateId, event.eventType, event.toJSON(), new Date(), 0, new Date());
  }
  static rehydrate(p: { id: string; aggregateId: string; eventType: string;
    payload: Record<string, unknown>; occurredAt: Date; attempts: number;
    nextAttemptAt?: Date; publishedAt?: Date }): OutboxMessage {
    return new OutboxMessage(p.id, p.aggregateId, p.eventType, p.payload,
      p.occurredAt, p.attempts, p.nextAttemptAt, p.publishedAt);
  }
  get attempts(): number { return this._attempts; }
  get nextAttemptAt(): Date | undefined { return this._nextAttemptAt; }
  get publishedAt(): Date | undefined { return this._publishedAt; }
  isPending(): boolean { return !this._publishedAt; }
  isDue(now: Date): boolean { return this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt <= now); }
  markPublished(at: Date): void { this._publishedAt = at; }
  scheduleRetry(now: Date): void {
    this._attempts++;
    this._nextAttemptAt = new Date(now.getTime() + Math.min(60000, 1000 * 2 ** Math.min(this._attempts, 6)));
  }
}
