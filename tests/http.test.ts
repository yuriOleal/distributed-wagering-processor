import { expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';

const base = process.env.TEST_BASE_URL ?? 'http://127.0.0.1:3000';
async function call(path: string, method = 'GET', body?: unknown, key?: string) {
  const response = await fetch(`${base}${path}`, { method,
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  return { status: response.status, data: await response.json() as any };
}

test('HTTP contract, concurrent balance, original replay, ledger and reconciliation', async () => {
  expect((await call('/health/ready')).status).toBe(200);
  const playerId = randomUUID();
  const createBody = { playerId, initialBalance: { amount: '100.00', currency: 'BRL' } };
  const created = await call('/wallets', 'POST', createBody);
  expect(created.status).toBe(201);
  expect((await call('/wallets', 'POST', createBody)).status).toBe(409);
  const walletId = created.data.id;
  const request = (amount: string) => ({ providerId: 'http-test', externalTransactionId: randomUUID(),
    walletId, playerId, roundId: 'round', gameId: 'game', kind: 'BET',
    money: { amount, currency: 'BRL' } });
  const bets = [request('80.00'), request('80.00')];
  const results = await Promise.all(bets.map(b => call('/wagering/transactions', 'POST', b, b.externalTransactionId)));
  expect(results.map(r => r.status).sort()).toEqual([200, 422]);
  const winnerIndex = results.findIndex(r => r.status === 200);
  const original = results[winnerIndex];
  expect(original.data.balance.amount).toBe('20.00');
  const credit = { ...request('10.00'), kind: 'WIN' };
  expect((await call('/wagering/transactions', 'POST', credit, credit.externalTransactionId)).status).toBe(200);
  const replay = await call('/wagering/transactions', 'POST', bets[winnerIndex], bets[winnerIndex].externalTransactionId);
  expect(replay.data.balance.amount).toBe('20.00');
  expect(replay.data.idempotentReplay).toBe(true);
  expect((await call(`/wallets/${walletId}`)).data.balance.amount).toBe('30.00');
  const conflict = { ...bets[winnerIndex], money: { amount: '1.00', currency: 'BRL' } };
  expect((await call('/wagering/transactions', 'POST', conflict, bets[winnerIndex].externalTransactionId)).status).toBe(409);
  expect((await call('/wagering/transactions', 'POST', request('1.00'))).status).toBe(400);
  expect((await call('/wagering/transactions', 'POST', { ...request('1.00'), providerId: 123 }, randomUUID())).status).toBe(400);
  const pending = { ...request('1.00'), kind: 'REFUND', referenceExternalTransactionId: randomUUID() };
  expect((await call('/wagering/transactions', 'POST', pending, pending.externalTransactionId)).status).toBe(202);
  const ledger = await call(`/wallets/${walletId}/ledger?limit=1`);
  expect(ledger.data.entries.length).toBe(1);
  expect(typeof ledger.data.nextCursor).toBe('string');
  expect((await call(`/wallets/${walletId}/ledger?cursor=bad`)).status).toBe(400);
  const reconciled = await call(`/wallets/${walletId}/reconciliation`, 'POST');
  expect(reconciled.data.consistent).toBe(true);
  expect(reconciled.data.checkedEntries).toBe(3);
  expect((await call(`/wagering/transactions/${original.data.transactionId}`)).data.status).toBe('PROCESSED');
}, 15000);

test('demo, OpenAPI, security headers and operational metrics are served', async () => {
  const demo = await fetch(`${base}/demo`);
  expect(demo.status).toBe(200);
  expect(demo.headers.get('x-content-type-options')).toBe('nosniff');
  expect(demo.headers.get('x-frame-options')).toBe('DENY');
  expect(demo.headers.get('content-security-policy')).toContain('sha256-');
  expect(await demo.text()).toContain('Wager Lab');
  const schema = await call('/api-json');
  expect(schema.data.paths['/wagering/transactions'].post).toBeDefined();
  expect((await fetch(`${base}/api`)).status).toBe(200);
  const metrics = await (await fetch(`${base}/metrics`)).text();
  expect(metrics).toContain('wager_processing_latency_ms_count');
  expect(metrics).toContain('wager_outbox_lag_seconds');
});
