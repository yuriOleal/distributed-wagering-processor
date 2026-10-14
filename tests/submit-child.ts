import { db } from '../src/db';
import { WagerService } from '../src/service';

const [walletId, playerId, externalId] = process.argv.slice(2);
await db.initialize();
try {
  const result = await new WagerService().submit({ providerId: 'process-test',
    externalTransactionId: externalId, playerId, walletId, roundId: 'r', gameId: 'g',
    kind: 'BET', money: { amount: '30.00', currency: 'BRL' } },
  `process-test:${externalId}`);
  console.log(JSON.stringify(result));
} finally { await db.destroy(); }
