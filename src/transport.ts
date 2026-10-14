import { Injectable, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { ChangeMessageVisibilityCommand, DeleteMessageCommand, GetQueueAttributesCommand,
  ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { db } from './db';
import { DomainError } from './domain';
import { sha256, WagerInput, WagerService } from './service';
import { errorCode, log, metrics } from './metrics';

export const sqs = new SQSClient({
  region: process.env.AWS_REGION ?? 'us-east-1',
  endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
  credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
});
const wagerQueue = process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
const eventQueue = process.env.EVENT_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-events.fifo';
const dlq = process.env.WAGER_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

@Injectable()
export class Workers implements OnApplicationBootstrap, OnApplicationShutdown {
  private stopping = false;
  private loops: Promise<void>[] = [];
  constructor(private readonly wagers: WagerService) {}
  onApplicationBootstrap(): void {
    this.loops = [this.consumeLoop(), this.outboxLoop(), this.referenceLoop()];
  }
  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    await Promise.allSettled(this.loops);
  }
  private async consumeLoop(): Promise<void> {
    while (!this.stopping) {
      try {
        const response = await sqs.send(new ReceiveMessageCommand({ QueueUrl: wagerQueue,
          MaxNumberOfMessages: 10, WaitTimeSeconds: 5, VisibilityTimeout: 30,
          MessageSystemAttributeNames: ['ApproximateReceiveCount'] }));
        for (const message of response.Messages ?? []) {
          if (this.stopping) {
            // These messages were received but not started. Return them to the queue now.
            await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: wagerQueue,
              ReceiptHandle: message.ReceiptHandle!, VisibilityTimeout: 0 }));
          } else {
            await this.handleMessage(message.Body ?? '', message.ReceiptHandle!,
              Number(message.Attributes?.ApproximateReceiveCount ?? 1));
          }
        }
      } catch (error) {
        log('sqs_receive_failed', { error: errorCode(error) });
        await sleep(2000);
      }
    }
  }
  private async handleMessage(body: string, receipt: string, receiveCount: number): Promise<void> {
    let envelope: any;
    try {
      envelope = JSON.parse(body);
      if (!envelope || envelope.type !== 'WagerTransactionRequested' ||
        typeof envelope.messageId !== 'string' || envelope.messageId.length === 0 ||
        envelope.messageId.length > 128 || typeof envelope.occurredAt !== 'string' ||
        !Number.isFinite(Date.parse(envelope.occurredAt)) ||
        !envelope.data || typeof envelope.data !== 'object' || Array.isArray(envelope.data))
        throw new DomainError('INVALID_ENVELOPE');
      const data = envelope.data as WagerInput & { idempotencyKey: string };
      const result = await this.wagers.submit(data, data.idempotencyKey, {
        consumerName: 'wager-processor', messageId: envelope.messageId, payloadHash: sha256(envelope),
      });
      // Ack happens only after the SQL transaction commits.
      await sqs.send(new DeleteMessageCommand({ QueueUrl: wagerQueue, ReceiptHandle: receipt }));
      log('wager_consumed', { correlationId: result.transactionId,
        messageId: envelope.messageId, transactionId: result.transactionId,
        walletId: data.walletId, providerId: data.providerId, status: result.status });
    } catch (error) {
      if (error instanceof DomainError || error instanceof SyntaxError) {
        try {
          const walletId = envelope?.data?.walletId;
          await sqs.send(new SendMessageCommand({ QueueUrl: dlq, MessageBody: body,
            MessageGroupId: typeof walletId === 'string' && /^[0-9a-f-]{36}$/i.test(walletId)
              ? walletId : 'invalid',
            MessageDeduplicationId: randomUUID() }));
          await sqs.send(new DeleteMessageCommand({ QueueUrl: wagerQueue, ReceiptHandle: receipt }));
          metrics.dlq++;
          log('wager_dead_lettered', { messageId: envelope?.messageId, error: errorCode(error) });
        } catch (dlqError) { log('dlq_send_failed', { error: errorCode(dlqError) }); }
      } else {
        metrics.retries++;
        log('wager_retry', { messageId: envelope?.messageId, receiveCount, error: errorCode(error) });
        await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: wagerQueue,
          ReceiptHandle: receipt, VisibilityTimeout: Math.min(300, 2 ** Math.min(receiveCount, 8)) }));
      }
    }
  }
  private async outboxLoop(): Promise<void> {
    while (!this.stopping) {
      try {
        const q = db.createQueryRunner();
        await q.connect(); await q.startTransaction();
        try {
          const rows = await q.query(`SELECT * FROM outbox_messages
            WHERE published_at IS NULL AND next_attempt_at<=now()
            ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 20`);
          for (const row of rows) {
            try {
              await sqs.send(new SendMessageCommand({ QueueUrl: eventQueue,
                MessageBody: JSON.stringify(row.payload), MessageGroupId: row.aggregate_id,
                MessageDeduplicationId: row.id }));
              await q.query('UPDATE outbox_messages SET published_at=now() WHERE id=$1', [row.id]);
            } catch (error) {
              await q.query(`UPDATE outbox_messages SET attempts=attempts+1,
                next_attempt_at=now()+make_interval(secs=>LEAST(300,POWER(2,LEAST(attempts+1,8))::int))
                WHERE id=$1`, [row.id]);
              metrics.retries++;
              log('outbox_retry', { error: errorCode(error) });
            }
          }
          await q.commitTransaction();
        } catch (error) { await q.rollbackTransaction(); throw error; }
        finally { await q.release(); }
      } catch (error) { log('outbox_worker_failed', { error: errorCode(error) }); }
      await sleep(500);
    }
  }
  private async referenceLoop(): Promise<void> {
    while (!this.stopping) {
      try {
        for (const id of await this.wagers.dueReferences()) {
          if (this.stopping) break;
          await this.wagers.retryReference(id);
        }
      } catch (error) { log('reference_worker_failed', { error: errorCode(error) }); }
      await sleep(1000);
    }
  }
}

export async function readiness(): Promise<boolean> {
  try {
    await db.query('SELECT 1');
    await sqs.send(new GetQueueAttributesCommand({ QueueUrl: wagerQueue, AttributeNames: ['QueueArn'] }));
    return true;
  } catch { return false; }
}

export async function prometheus(): Promise<string> {
  const [lag] = await db.query(`SELECT COALESCE(EXTRACT(EPOCH FROM now()-MIN(occurred_at)),0) AS lag
    FROM outbox_messages WHERE published_at IS NULL`);
  const lines = Object.entries(metrics.transactions).map(([status, count]) =>
    `wager_transactions_total{status="${status}"} ${count}`);
  lines.push(`wager_duplicates_total ${metrics.duplicates}`,
    `wager_retries_total ${metrics.retries}`, `wager_dlq_total ${metrics.dlq}`,
    `wager_lock_conflicts_total ${metrics.lockConflicts}`, `wager_outbox_lag_seconds ${lag.lag}`);
  lines.push(`wager_reconciliation_divergences_total ${metrics.reconciliationDivergences}`,
    `wager_processing_latency_ms_count ${metrics.processingCount}`,
    `wager_processing_latency_ms_sum ${metrics.processingSumMs}`);
  return lines.join('\n') + '\n';
}
