import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { db } from '../src/db';
import { WagerService } from '../src/service';
import { Workers, sqs } from '../src/transport';
import { GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';

const service = new WagerService();
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => Promise<boolean>, timeout = 12000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await check()) return; await pause(100); }
  throw new Error('condition timed out');
}
const input = (walletId: string, playerId: string, external: string, amount = '25.00') => ({
  providerId: 'integration-test', externalTransactionId: external, walletId, playerId,
  roundId: 'round-1', gameId: 'game-1', kind: 'BET' as const,
  money: { amount, currency: 'BRL' },
});
async function wallet(initial = '100.00') {
  const playerId = randomUUID();
  const created = await service.createWallet(playerId, { amount: initial, currency: 'BRL' }) as { id: string };
  return { id: created.id, playerId };
}
async function balanceAndLedger(id: string) {
  const [row] = await db.query(`SELECT w.balance,COALESCE(SUM(CASE WHEN l.direction='CREDIT'
    THEN l.amount ELSE -l.amount END),0)::numeric(20,2) AS reconstructed
    FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id
    WHERE w.id=$1 GROUP BY w.id`, [id]);
  expect(row.balance).toBe(row.reconstructed);
  return row.balance as string;
}

beforeAll(async () => {
  await db.initialize();
  await db.runMigrations({ transaction: 'all' });
  const queue = process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
  await sqs.send(new GetQueueAttributesCommand({ QueueUrl: queue, AttributeNames: ['QueueArn'] }));
});
afterAll(async () => { await db.destroy(); });

test('schema, 50 redeliveries, hot-wallet race and ledger reconciliation', async () => {
  const w = await wallet();
  const eventId = randomUUID();
  const request = input(w.id, w.playerId, eventId, '80.00');
  const results = await Promise.all(Array.from({ length: 50 }, () =>
    service.submit(request, `integration-test:${eventId}`)));
  expect(results.filter(r => !r.idempotentReplay).length).toBe(1);
  expect(results.every(r => r.transactionId === results[0].transactionId)).toBe(true);
  expect(await balanceAndLedger(w.id)).toBe('20.00');
  const [debits] = await db.query(`SELECT COUNT(*)::int AS count FROM wallet_ledger
    WHERE wallet_id=$1 AND direction='DEBIT'`, [w.id]);
  expect(debits.count).toBe(1);
  await expect(service.submit({ ...request, money: { amount: '79.00', currency: 'BRL' } },
    `integration-test:${eventId}`)).rejects.toMatchObject({ code: 'IDEMPOTENCY_CONFLICT' });

  const rival = await wallet();
  const [a, b] = await Promise.all(['a', 'b'].map(suffix => service.submit(
    input(rival.id, rival.playerId, randomUUID(), '80.00'), `race:${suffix}:${randomUUID()}`)));
  expect([a.status, b.status].sort()).toEqual(['PROCESSED', 'REJECTED']);
  expect(await balanceAndLedger(rival.id)).toBe('20.00');
  const [rivalDebits] = await db.query(`SELECT COUNT(*)::int AS count FROM wallet_ledger
    WHERE wallet_id=$1 AND direction='DEBIT'`, [rival.id]);
  expect(rivalDebits.count).toBe(1);
  const [pending] = await db.query(`SELECT COUNT(*)::int AS count FROM outbox_messages
    WHERE aggregate_id=$1 AND published_at IS NULL`, [rival.id]);
  expect(pending.count).toBeGreaterThan(0);
});

test('out-of-order refund waits, then reverses exactly once', async () => {
  const w = await wallet();
  const betId = randomUUID();
  const refundId = randomUUID();
  const refund = await service.submit({ ...input(w.id, w.playerId, refundId),
    kind: 'REFUND', referenceExternalTransactionId: betId }, `refund:${refundId}`);
  expect(refund.status).toBe('PENDING_REFERENCE');
  await service.submit(input(w.id, w.playerId, betId), `bet:${betId}`);
  await db.query(`UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1`, [refund.transactionId]);
  await service.retryReference(refund.transactionId);
  expect((await service.transaction(refund.transactionId) as any).status).toBe('PROCESSED');
  expect(await balanceAndLedger(w.id)).toBe('100.00');
  const second = randomUUID();
  const rejected = await service.submit({ ...input(w.id, w.playerId, second),
    kind: 'REFUND', referenceExternalTransactionId: betId }, `refund:${second}`);
  expect(rejected.failureCode).toBe('ALREADY_REVERSED');
  expect(await balanceAndLedger(w.id)).toBe('100.00');
});

test('three independent Bun processes contend for one wallet', async () => {
  const w = await wallet('50.00');
  const children = Array.from({ length: 3 }, (_, i) => Bun.spawn([
    process.execPath, 'tests/submit-child.ts', w.id, w.playerId, `child-${randomUUID()}-${i}`],
    { cwd: process.cwd(), stdout: 'pipe', stderr: 'pipe' }));
  const statuses = await Promise.all(children.map(async child => {
    const output = await new Response(child.stdout).text();
    const errors = await new Response(child.stderr).text();
    expect(await child.exited).toBe(0);
    expect(errors).toBe('');
    return JSON.parse(output.trim()).status;
  }));
  expect(statuses.filter(s => s === 'PROCESSED').length).toBe(1);
  expect(await balanceAndLedger(w.id)).toBe('20.00');
});

test('SQS inbox, duplicate delivery, outbox restart and DLQ', async () => {
  const w = await wallet();
  const external = randomUUID();
  const messageId = randomUUID();
  const queue = process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
  const dead = process.env.WAGER_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
  const body = JSON.stringify({ messageId, type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(), data: { ...input(w.id, w.playerId, external),
      idempotencyKey: `sqs:${external}` } });
  // Submit once directly: this models a process killed after commit and before SQS ack.
  await service.submit(JSON.parse(body).data, `sqs:${external}`);
  const workersA = new Workers(service);
  const workersB = new Workers(service);
  workersA.onApplicationBootstrap(); workersB.onApplicationBootstrap();
  try {
    await sqs.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: body,
      MessageGroupId: w.id, MessageDeduplicationId: randomUUID() }));
    await until(async () => {
      const [row] = await db.query(`SELECT COUNT(*)::int AS count FROM inbox_messages
        WHERE consumer_name='wager-processor' AND message_id=$1`, [messageId]);
      return row.count === 1;
    });
    expect(await balanceAndLedger(w.id)).toBe('75.00');
    await until(async () => {
      const [row] = await db.query(`SELECT COUNT(*)::int AS count FROM outbox_messages
        WHERE aggregate_id=$1 AND published_at IS NULL`, [w.id]);
      return row.count === 0;
    });
    await sqs.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: '{invalid-json',
      MessageGroupId: 'invalid', MessageDeduplicationId: randomUUID() }));
    await until(async () => {
      const result = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dead,
        WaitTimeSeconds: 1, MaxNumberOfMessages: 1 }));
      return !!result.Messages?.some(m => m.Body === '{invalid-json');
    });
  } finally {
    await Promise.all([workersA.onApplicationShutdown(), workersB.onApplicationShutdown()]);
  }
});
