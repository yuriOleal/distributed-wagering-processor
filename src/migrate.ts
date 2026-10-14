import { db } from './db';

await db.initialize();
try {
  if (process.argv[2] === 'down') await db.undoLastMigration();
  else await db.runMigrations({ transaction: 'all' });
} finally {
  await db.destroy();
}
