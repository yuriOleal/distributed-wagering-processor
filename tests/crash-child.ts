import { ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { db } from '../src/db';
import { WagerService, sha256 } from '../src/service';
import { sqs } from '../src/transport';

await db.initialize();
const response = await sqs.send(new ReceiveMessageCommand({
  QueueUrl: process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo',
  MaxNumberOfMessages: 1, WaitTimeSeconds: 5, VisibilityTimeout: 30,
}));
const message = response.Messages?.[0];
if (!message?.Body) throw new Error('Crash probe did not receive a message');
const envelope = JSON.parse(message.Body);
const result = await new WagerService().submit(envelope.data, envelope.data.idempotencyKey, {
  consumerName: 'wager-processor', messageId: envelope.messageId, payloadHash: sha256(envelope),
});
// The parent kills this real process here, after SQL commit and before SQS ack.
console.log(JSON.stringify({ result, receipt: message.ReceiptHandle }));
await new Promise(() => {});
