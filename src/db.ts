import 'reflect-metadata';
import { Column, DataSource, Entity, Index, PrimaryColumn } from 'typeorm';
import { InitialSchema1710000000000 } from './migrations/1710000000000-InitialSchema';

@Entity('wallets')
@Index(['playerId', 'currency'], { unique: true })
export class WalletRow {
  @PrimaryColumn('uuid') id!: string;
  @Column('uuid', { name: 'player_id' }) playerId!: string;
  @Column('varchar', { length: 3 }) currency!: string;
  @Column('numeric', { precision: 20, scale: 2 }) balance!: string;
  @Column('integer') version!: number;
  @Column('timestamptz', { name: 'created_at' }) createdAt!: Date;
  @Column('timestamptz', { name: 'updated_at' }) updatedAt!: Date;
}

@Entity('wager_transactions')
@Index(['providerId', 'externalTransactionId'], { unique: true })
export class TransactionRow {
  @PrimaryColumn('uuid') id!: string;
  @Column('varchar', { name: 'provider_id' }) providerId!: string;
  @Column('varchar', { name: 'external_transaction_id' }) externalTransactionId!: string;
  @Column('varchar', { name: 'idempotency_key', unique: true }) idempotencyKey!: string;
  @Column('char', { name: 'payload_hash', length: 64 }) payloadHash!: string;
  @Column('uuid', { name: 'wallet_id' }) walletId!: string;
  @Column('uuid', { name: 'player_id' }) playerId!: string;
  @Column('varchar', { name: 'round_id' }) roundId!: string;
  @Column('varchar', { name: 'game_id' }) gameId!: string;
  @Column('varchar') kind!: string;
  @Column('numeric', { precision: 20, scale: 2 }) amount!: string;
  @Column('varchar', { length: 3 }) currency!: string;
  @Column('varchar', { name: 'reference_external_transaction_id', nullable: true }) referenceExternalTransactionId!: string | null;
  @Column('uuid', { name: 'reference_transaction_id', nullable: true }) referenceTransactionId!: string | null;
  @Column('varchar') status!: string;
  @Column('varchar', { name: 'failure_code', nullable: true }) failureCode!: string | null;
  @Column('numeric', { name: 'result_balance', precision: 20, scale: 2, nullable: true }) resultBalance!: string | null;
  @Column('timestamptz', { name: 'processed_at', nullable: true }) processedAt!: Date | null;
  @Column('integer', { name: 'reference_attempts' }) referenceAttempts!: number;
  @Column('timestamptz', { name: 'next_attempt_at', nullable: true }) nextAttemptAt!: Date | null;
  @Column('timestamptz', { name: 'created_at' }) createdAt!: Date;
}

export const db = new DataSource({
  type: 'postgres',
  url: process.env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager',
  entities: [WalletRow, TransactionRow],
  migrations: [InitialSchema1710000000000],
  synchronize: false,
  logging: false,
});
