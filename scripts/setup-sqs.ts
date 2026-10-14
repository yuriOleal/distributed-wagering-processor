import { CreateQueueCommand, GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';

const client = new SQSClient({
  region: process.env.AWS_REGION ?? 'us-east-1',
  endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
  credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
});
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

for (let attempt = 0; attempt < 30; attempt++) {
  try {
    const dlq = await client.send(new CreateQueueCommand({ QueueName: 'wager-transactions-dlq.fifo',
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' } }));
    await client.send(new CreateQueueCommand({ QueueName: 'wager-events.fifo',
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' } }));
    const attributes = await client.send(new GetQueueAttributesCommand({
      QueueUrl: dlq.QueueUrl!, AttributeNames: ['QueueArn'] }));
    await client.send(new CreateQueueCommand({ QueueName: 'wager-transactions.fifo',
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false',
        VisibilityTimeout: '30', RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: attributes.Attributes!.QueueArn, maxReceiveCount: '5',
        }) } }));
    console.log('SQS queues ready');
    process.exit(0);
  } catch (error) {
    if (attempt === 29) throw error;
    await pause(1000);
  }
}
