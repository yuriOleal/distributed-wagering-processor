import { describe, expect, test } from 'bun:test';
import { DomainError, Money, Wallet, WalletLedgerEntry, WagerTransaction } from '../src/domain';
import { sha256, WagerService } from '../src/service';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const id = '00000000-0000-4000-8000-000000000001';

describe('Money', () => {
  test('zero is neither positive nor negative and cannot move a wallet', () => {
    const zero = brl('0.00');
    expect(zero.isPositive()).toBe(false);
    expect(zero.negate().isNegative()).toBe(false);
    const wallet = Wallet.open({ id, playerId: id, initialBalance: zero });
    expect(() => wallet.credit(zero)).toThrow(DomainError);
    expect(() => wallet.debit(zero)).toThrow(DomainError);
    expect(wallet.version).toBe(1);
  });
  test('currency must be a string, never an implicitly converted JSON value', () => {
    expect(() => Money.from({ amount: '1.00', currency: ['BRL'] as any })).toThrow(DomainError);
  });
  test('keeps two decimal places and exact arithmetic', () => {
    expect(brl('0.10').add(brl('0.20')).toJSON()).toEqual({ amount: '0.30', currency: 'BRL' });
    expect(brl('25.00').subtract(brl('0.01')).toString()).toBe('24.99');
  });
  test('rejects malformed, negative, excess scale and cross-currency operations', () => {
    for (const amount of ['', 'NaN', 'Infinity', '1e2', '-1.00', '1.001', ' 1.00',
      '1', '1.0',
      '1000000000000000000.00'])
      expect(() => brl(amount)).toThrow(DomainError);
    expect(() => brl('1.00').add(Money.from({ amount: '1.00', currency: 'USD' })))
      .toThrow(DomainError);
    expect(() => Money.from({ amount: '1.00', currency: 'brl' })).toThrow(DomainError);
    expect(() => brl('1.00').subtract(Money.from({ amount: '1.00', currency: 'USD' })))
      .toThrow(DomainError);
  });
  test('rejects credit overflow before PostgreSQL numeric(20,2) would fail', () => {
    expect(() => brl('999999999999999999.99').add(brl('0.01')))
      .toThrow(new DomainError('AMOUNT_OUT_OF_RANGE'));
  });
});

test('wallet prevents negative balance and increments version only on movement', () => {
  const wallet = Wallet.open({ id, playerId: id, initialBalance: brl('100.00') });
  expect(wallet.version).toBe(1);
  expect(() => wallet.debit(brl('101.00'))).toThrow(DomainError);
  expect(wallet.balance.toString()).toBe('100.00');
  expect(wallet.version).toBe(1);
  wallet.debit(brl('80.00'));
  expect(wallet.balance.toString()).toBe('20.00');
  expect(wallet.version).toBe(2);
  wallet.credit(brl('5.00'));
  expect(wallet.balance.toString()).toBe('25.00');
  expect(wallet.version).toBe(3);
  expect(() => wallet.credit(Money.from({ amount: '1.00', currency: 'USD' })))
    .toThrow(DomainError);
  expect(wallet.version).toBe(3);
});

test('ledger factory enforces balance arithmetic', () => {
  expect(() => WalletLedgerEntry.create({ id, walletId: id, transactionId: id,
    direction: 'DEBIT', money: brl('20.00'), balanceBefore: brl('100.00'),
    balanceAfter: brl('90.00') })).toThrow(DomainError);
  expect(WalletLedgerEntry.create({ id, walletId: id, transactionId: id,
    direction: 'DEBIT', money: brl('20.00'), balanceBefore: brl('100.00'),
    balanceAfter: brl('80.00') }).isBalanced()).toBe(true);
});

test('transaction transitions are terminal and rollback direction depends on reference', () => {
  const base = { id, providerId: 'p', externalTransactionId: 'e', idempotencyKey: 'k',
    payloadHash: 'h', walletId: id, playerId: id, roundId: 'r', gameId: 'g', money: brl('5.00') };
  const bet = WagerTransaction.create({ ...base, kind: 'BET' });
  bet.markProcessed(undefined, new Date());
  expect(() => bet.reject('LATE')).toThrow(DomainError);
  const rollback = WagerTransaction.create({ ...base, externalTransactionId: 'rollback',
    kind: 'ROLLBACK', referenceExternalTransactionId: 'e' });
  expect(rollback.ledgerDirectionFor(bet)).toBe('CREDIT');
  const win = WagerTransaction.create({ ...base, externalTransactionId: 'win', kind: 'WIN' });
  expect(rollback.ledgerDirectionFor(win)).toBe('DEBIT');
  const refund = WagerTransaction.create({ ...base, externalTransactionId: 'refund',
    kind: 'REFUND', referenceExternalTransactionId: 'e' });
  expect(refund.ledgerDirectionFor(bet)).toBe('CREDIT');
  expect(() => WagerTransaction.create({ ...base, kind: 'OPENING' })).toThrow(DomainError);
  expect(() => WagerTransaction.create({ ...base, kind: 'ROLLBACK' })).toThrow(DomainError);
  const loss = WagerTransaction.create({ ...base, kind: 'LOSS' });
  expect(loss.affectsBalance()).toBe(false);
  loss.markProcessed(undefined, new Date());
  expect(() => loss.markPendingReference()).toThrow(DomainError);
});

test('canonical payload hash ignores transport order but includes business changes', () => {
  expect(sha256({ a: 1, b: { c: 2, d: 3 } })).toBe(sha256({ b: { d: 3, c: 2 }, a: 1 }));
  expect(sha256({ amount: '1.00' })).not.toBe(sha256({ amount: '2.00' }));
});

test('JSON identifiers reject coercion before any database work', async () => {
  const service = new WagerService();
  const input = { providerId: 'provider', externalTransactionId: 'external', playerId: id,
    walletId: id, roundId: 'round', gameId: 'game', kind: 'BET' as const,
    money: { amount: '1.00', currency: 'BRL' } };
  for (const field of ['providerId', 'externalTransactionId', 'roundId', 'gameId']) {
    await expect(service.submit({ ...input, [field]: 123 }, 'key'))
      .rejects.toMatchObject({ code: 'INVALID_PAYLOAD' });
  }
  await expect(service.createWallet([id] as any, input.money))
    .rejects.toMatchObject({ code: 'INVALID_PAYLOAD' });
});
