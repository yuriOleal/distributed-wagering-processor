import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { db } from '../src/db';
import { WagerService } from '../src/service';
import { Workers, sqs } from '../src/transport';
import { ChangeMessageVisibilityCommand, DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand,
  SendMessageCommand } from '@aws-sdk/client-sqs';
import { metrics } from '../src/metrics';

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
}, 30000);

test('zero opening creates no financial transaction or ledger entry', async () => {
  const w = await wallet('0.00');
  expect(await balanceAndLedger(w.id)).toBe('0.00');
  const [counts] = await db.query(`SELECT
    (SELECT COUNT(*)::int FROM wager_transactions WHERE wallet_id=$1) AS transactions,
    (SELECT COUNT(*)::int FROM wallet_ledger WHERE wallet_id=$1) AS entries`, [w.id]);
  expect(counts).toEqual({ transactions: 0, entries: 0 });
});

test('shared use case measures HTTP submissions and replays once', async () => {
  const w = await wallet();
  const request = input(w.id, w.playerId, randomUUID());
  const before = { processed: metrics.transactions.PROCESSED, duplicates: metrics.duplicates,
    latencyCount: metrics.processingCount };
  await service.submit(request, `metrics:${request.externalTransactionId}`);
  await service.submit(request, `metrics:${request.externalTransactionId}`);
  expect(metrics.transactions.PROCESSED - before.processed).toBe(1);
  expect(metrics.duplicates - before.duplicates).toBe(1);
  expect(metrics.processingCount - before.latencyCount).toBe(2);
  expect(await balanceAndLedger(w.id)).toBe('75.00');
});

test('ledger cursor preserves microseconds and never skips entries', async () => {
  const w = await wallet('0.00');
  await db.transaction(async manager => {
    for (let i = 1; i <= 3; i++) {
      const tx = randomUUID();
      await manager.query(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,
        idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,
        status,result_balance,processed_at) VALUES ($1::text::uuid,'cursor',$1,$1,$2,$3,$4,'r','g',
        'WIN','1.00','BRL','PROCESSED',$5,now())`, [tx, 'a'.repeat(64), w.id, w.playerId, `${i}.00`]);
      await manager.query(`INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,
        amount,currency,balance_before,balance_after,created_at)
        VALUES ($1,$2,$3,'CREDIT','1.00','BRL',$4,$5,$6)`,
        [randomUUID(), w.id, tx, `${i - 1}.00`, `${i}.00`, `2026-01-01T00:00:00.12300${i}Z`]);
    }
    await manager.query(`UPDATE wallets SET balance='3.00',version=4 WHERE id=$1`, [w.id]);
  });
  const ids: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await service.ledger(w.id, cursor, '1') as any;
    ids.push(...page.entries.map((entry: any) => entry.id));
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  expect(ids.length).toBe(3);
  expect(new Set(ids).size).toBe(3);
  expect(await balanceAndLedger(w.id)).toBe('3.00');
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

test('WIN, LOSS, and ROLLBACK keep ledger and business states consistent', async () => {
  const w = await wallet('100.00');
  const betId = randomUUID();
  const bet = await service.submit(input(w.id, w.playerId, betId, '30.00'), `bet:${betId}`);
  expect(bet.status).toBe('PROCESSED');

  const winId = randomUUID();
  const win = await service.submit({ ...input(w.id, w.playerId, winId, '15.00'),
    kind: 'WIN', referenceExternalTransactionId: betId }, `win:${winId}`);
  expect(win.status).toBe('PROCESSED');

  const lossId = randomUUID();
  const loss = await service.submit({ ...input(w.id, w.playerId, lossId, '5.00'),
    kind: 'LOSS' }, `loss:${lossId}`);
  expect(loss.status).toBe('PROCESSED');
  expect(await balanceAndLedger(w.id)).toBe('85.00');
  const [lossEntries] = await db.query(`SELECT COUNT(*)::int AS count FROM wallet_ledger
    WHERE transaction_id=$1`, [loss.transactionId]);
  expect(lossEntries.count).toBe(0);

  const rollbackId = randomUUID();
  const rollback = await service.submit({ ...input(w.id, w.playerId, rollbackId, '15.00'),
    kind: 'ROLLBACK', referenceExternalTransactionId: winId }, `rollback:${rollbackId}`);
  expect(rollback.status).toBe('PROCESSED');
  expect(await balanceAndLedger(w.id)).toBe('70.00');

  const duplicateId = randomUUID();
  const duplicate = await service.submit({ ...input(w.id, w.playerId, duplicateId, '15.00'),
    kind: 'ROLLBACK', referenceExternalTransactionId: winId }, `rollback:${duplicateId}`);
  expect(duplicate.failureCode).toBe('ALREADY_REVERSED');
  expect(await balanceAndLedger(w.id)).toBe('70.00');
});

test('a rejected reversal cannot debit below zero', async () => {
  const w = await wallet('10.00');
  const winId = randomUUID();
  await service.submit({ ...input(w.id, w.playerId, winId, '20.00'), kind: 'WIN' }, `win:${winId}`);
  const betId = randomUUID();
  await service.submit(input(w.id, w.playerId, betId, '25.00'), `bet:${betId}`);
  const rollbackId = randomUUID();
  const reversal = await service.submit({ ...input(w.id, w.playerId, rollbackId, '20.00'),
    kind: 'ROLLBACK', referenceExternalTransactionId: winId }, `rollback:${rollbackId}`);
  expect(reversal.status).toBe('REJECTED');
  expect(reversal.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
  expect(await balanceAndLedger(w.id)).toBe('5.00');
  const [entries] = await db.query(`SELECT COUNT(*)::int AS count FROM wallet_ledger
    WHERE transaction_id=$1`, [reversal.transactionId]);
  expect(entries.count).toBe(0);
});

test('an exhausted out-of-order reference is rejected and emits an event', async () => {
  const w = await wallet('100.00');
  const external = randomUUID();
  const pending = await service.submit({ ...input(w.id, w.playerId, external),
    kind: 'REFUND', referenceExternalTransactionId: randomUUID() }, `missing:${external}`);
  expect(pending.status).toBe('PENDING_REFERENCE');
  await db.query(`UPDATE wager_transactions SET reference_attempts=8,
    next_attempt_at=now() WHERE id=$1`, [pending.transactionId]);
  await service.retryReference(pending.transactionId);
  const rejected = await service.transaction(pending.transactionId) as {
    status: string; failureCode: string;
  };
  expect(rejected.status).toBe('REJECTED');
  expect(rejected.failureCode).toBe('REFERENCE_NOT_FOUND');
  const [events] = await db.query(`SELECT COUNT(*)::int AS count FROM outbox_messages
    WHERE aggregate_id=$1 AND event_type='WagerTransactionRejected'
    AND payload->'data'->>'transactionId'=$2`, [w.id, pending.transactionId]);
  expect(events.count).toBe(1);
  expect(await balanceAndLedger(w.id)).toBe('100.00');
});

test('deferred consistency check rolls back wallet, inbox, and outbox together', async () => {
  const w = await wallet('100.00');
  const messageId = randomUUID();
  const eventId = randomUUID();
  const runner = db.createQueryRunner();
  await runner.connect();
  await runner.startTransaction();
  try {
    await runner.query('UPDATE wallets SET balance=balance+1 WHERE id=$1', [w.id]);
    await runner.query(`INSERT INTO inbox_messages(consumer_name,message_id,payload_hash)
      VALUES ('atomicity-test',$1,$2)`, [messageId, 'a'.repeat(64)]);
    await runner.query(`INSERT INTO outbox_messages(id,aggregate_id,event_type,payload)
      VALUES ($1,$2,'AtomicityTest','{}'::jsonb)`, [eventId, w.id]);
    await expect(runner.commitTransaction()).rejects.toThrow();
  } finally {
    if (runner.isTransactionActive) await runner.rollbackTransaction();
    await runner.release();
  }
  expect(await balanceAndLedger(w.id)).toBe('100.00');
  const [rows] = await db.query(`SELECT
    (SELECT COUNT(*)::int FROM inbox_messages WHERE message_id=$1) AS inbox,
    (SELECT COUNT(*)::int FROM outbox_messages WHERE id=$2) AS outbox`, [messageId, eventId]);
  expect(rows).toEqual({ inbox: 0, outbox: 0 });
  await expect(db.query(`UPDATE wallet_ledger SET amount=amount+1 WHERE wallet_id=$1`, [w.id]))
    .rejects.toThrow();
  expect(await balanceAndLedger(w.id)).toBe('100.00');
});

test('different wallets accept simultaneous operations independently', async () => {
  const wallets = await Promise.all(Array.from({ length: 12 }, () => wallet('100.00')));
  const results = await Promise.all(wallets.map(w => {
    const external = randomUUID();
    return service.submit(input(w.id, w.playerId, external, '80.00'), `parallel:${external}`);
  }));
  expect(results.every(result => result.status === 'PROCESSED')).toBe(true);
  for (const w of wallets) expect(await balanceAndLedger(w.id)).toBe('20.00');
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

test('SQS inbox, duplicate delivery, outbox recovery and DLQ', async () => {
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
  const [pendingBeforeRestart] = await db.query(`SELECT COUNT(*)::int AS count
    FROM outbox_messages WHERE aggregate_id=$1 AND published_at IS NULL`, [w.id]);
  expect(pendingBeforeRestart.count).toBeGreaterThan(0);
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
      const found = result.Messages?.find(m => m.Body === '{invalid-json');
      if (!found) return false;
      await sqs.send(new DeleteMessageCommand({ QueueUrl: dead, ReceiptHandle: found.ReceiptHandle! }));
      return true;
    });
    const invalidWallet = JSON.stringify({ messageId: randomUUID(),
      type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: { ...input(w.id, w.playerId, randomUUID()), walletId: { invalid: true },
        idempotencyKey: `invalid:${randomUUID()}` } });
    await sqs.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: invalidWallet,
      MessageGroupId: 'invalid-wallet', MessageDeduplicationId: randomUUID() }));
    await until(async () => {
      const result = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dead,
        WaitTimeSeconds: 1, MaxNumberOfMessages: 1 }));
      const found = result.Messages?.find(m => m.Body === invalidWallet);
      if (!found) return false;
      await sqs.send(new DeleteMessageCommand({ QueueUrl: dead, ReceiptHandle: found.ReceiptHandle! }));
      return true;
    });
  } finally {
    await Promise.all([workersA.onApplicationShutdown(), workersB.onApplicationShutdown()]);
  }
}, 30000);

test('a process killed after commit is recovered by redelivery and concurrent publishers', async () => {
  const w = await wallet();
  const external = randomUUID();
  const messageId = randomUUID();
  const queue = process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
  const data = { ...input(w.id, w.playerId, external), idempotencyKey: `crash:${external}` };
  await sqs.send(new SendMessageCommand({ QueueUrl: queue, MessageGroupId: w.id,
    MessageDeduplicationId: messageId, MessageBody: JSON.stringify({ messageId,
      type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }) }));
  const child = Bun.spawn([process.execPath, 'tests/crash-child.ts'],
    { cwd: process.cwd(), stdout: 'pipe', stderr: 'pipe' });
  const killDeadline = setTimeout(() => child.kill('SIGKILL'), 10000);
  let committed: { receipt: string; result: { transactionId: string } };
  try {
    const reader = child.stdout.getReader();
    let output = '';
    while (!output.includes('\n')) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`Crash probe exited early: ${await new Response(child.stderr).text()}`);
      output += new TextDecoder().decode(chunk.value);
    }
    committed = JSON.parse(output.split('\n')[0]);
  } finally {
    child.kill('SIGKILL');
    await child.exited;
    clearTimeout(killDeadline);
  }
  expect(await balanceAndLedger(w.id)).toBe('75.00');
  const [pending] = await db.query(`SELECT COUNT(*)::int AS count FROM outbox_messages
    WHERE aggregate_id=$1 AND published_at IS NULL`, [w.id]);
  expect(pending.count).toBe(4);
  // Return the killed worker's message immediately instead of waiting its full visibility lease.
  await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: queue,
    ReceiptHandle: committed!.receipt, VisibilityTimeout: 0 }));
  const duplicatesBefore = metrics.duplicates;
  const workers = [new Workers(service), new Workers(service)];
  workers.forEach(worker => worker.onApplicationBootstrap());
  try {
    await until(async () => metrics.duplicates > duplicatesBefore);
    await until(async () => {
      const [row] = await db.query(`SELECT COUNT(*)::int AS count FROM outbox_messages
        WHERE aggregate_id=$1 AND published_at IS NULL`, [w.id]);
      return row.count === 0;
    });
  } finally { await Promise.all(workers.map(worker => worker.onApplicationShutdown())); }
  const [counts] = await db.query(`SELECT
    (SELECT COUNT(*)::int FROM inbox_messages WHERE message_id=$1) AS inbox,
    (SELECT COUNT(*)::int FROM wallet_ledger WHERE transaction_id=$2) AS ledger`,
    [messageId, committed!.result.transactionId]);
  expect(counts).toEqual({ inbox: 1, ledger: 1 });
  const replay = await service.submit(data, data.idempotencyKey);
  expect(replay.idempotentReplay).toBe(true);
  expect(replay.balance?.amount).toBe('75.00');
  expect(await balanceAndLedger(w.id)).toBe('75.00');
}, 30000);

test('a transient consumer failure retries the same message without duplicate effects', async () => {
  const w = await wallet();
  const external = randomUUID();
  let attempts = 0;
  class InterruptedService extends WagerService {
    override async submit(...args: Parameters<WagerService['submit']>) {
      if (++attempts === 1) throw new Error('Injected transient interruption before commit');
      return super.submit(...args);
    }
  }
  const queue = process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
  await sqs.send(new SendMessageCommand({ QueueUrl: queue, MessageGroupId: w.id,
    MessageDeduplicationId: randomUUID(), MessageBody: JSON.stringify({ messageId: randomUUID(),
      type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: { ...input(w.id, w.playerId, external), idempotencyKey: `retry:${external}` } }) }));
  const before = metrics.retries;
  const worker = new Workers(new InterruptedService());
  worker.onApplicationBootstrap();
  try {
    await until(async () => {
      const [row] = await db.query(`SELECT COUNT(*)::int AS count FROM wager_transactions
        WHERE provider_id='integration-test' AND external_transaction_id=$1`, [external]);
      return row.count === 1;
    });
  } finally { await worker.onApplicationShutdown(); }
  expect(attempts).toBe(2);
  expect(metrics.retries).toBeGreaterThan(before);
  expect(await balanceAndLedger(w.id)).toBe('75.00');
}, 30000);
