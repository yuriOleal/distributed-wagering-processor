import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1710000000000 implements MigrationInterface {
  name = 'InitialSchema1710000000000';

  async up(q: QueryRunner): Promise<void> {
    await q.query(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY, player_id uuid NOT NULL, currency varchar(3) NOT NULL,
        balance numeric(20,2) NOT NULL CHECK (balance >= 0),
        version integer NOT NULL CHECK (version >= 1),
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (player_id, currency), CHECK (currency ~ '^[A-Z]{3}$')
      );
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY, provider_id text NOT NULL, external_transaction_id text NOT NULL,
        idempotency_key text NOT NULL UNIQUE, payload_hash char(64) NOT NULL,
        wallet_id uuid NOT NULL REFERENCES wallets(id), player_id uuid NOT NULL,
        round_id text NOT NULL, game_id text NOT NULL,
        kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(20,2) NOT NULL CHECK (amount > 0), currency varchar(3) NOT NULL,
        reference_external_transaction_id text,
        reference_transaction_id uuid REFERENCES wager_transactions(id),
        status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text, result_balance numeric(20,2), processed_at timestamptz,
        reference_attempts integer NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
        next_attempt_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (provider_id, external_transaction_id),
        CHECK ((kind IN ('REFUND','ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
          OR (kind = 'WIN')
          OR (kind IN ('OPENING','BET','LOSS') AND reference_external_transaction_id IS NULL)),
        CHECK ((status = 'REJECTED') = (failure_code IS NOT NULL) OR status = 'FAILED'),
        CHECK (status <> 'PROCESSED' OR processed_at IS NOT NULL)
      );
      CREATE UNIQUE INDEX one_reversal_per_type ON wager_transactions(kind, reference_transaction_id)
        WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';
      CREATE INDEX pending_references_due ON wager_transactions(next_attempt_at)
        WHERE status = 'PENDING_REFERENCE';
      CREATE TABLE wallet_ledger (
        id uuid PRIMARY KEY, wallet_id uuid NOT NULL REFERENCES wallets(id),
        transaction_id uuid NOT NULL UNIQUE REFERENCES wager_transactions(id),
        direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount numeric(20,2) NOT NULL CHECK (amount > 0), currency varchar(3) NOT NULL,
        balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
        balance_after numeric(20,2) NOT NULL CHECK (balance_after >= 0),
        created_at timestamptz NOT NULL DEFAULT now(),
        CHECK ((direction = 'CREDIT' AND balance_after = balance_before + amount)
          OR (direction = 'DEBIT' AND balance_after = balance_before - amount))
      );
      CREATE INDEX wallet_ledger_page ON wallet_ledger(wallet_id, created_at DESC, id DESC);
      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL, message_id text NOT NULL, payload_hash char(64) NOT NULL,
        received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (consumer_name, message_id)
      );
      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL,
        payload jsonb NOT NULL, occurred_at timestamptz NOT NULL DEFAULT now(),
        attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
      );
      CREATE INDEX outbox_due ON outbox_messages(next_attempt_at) WHERE published_at IS NULL;
      CREATE FUNCTION prevent_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'ledger entries are immutable'; END $$;
      CREATE TRIGGER immutable_ledger BEFORE UPDATE OR DELETE ON wallet_ledger
        FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();
      CREATE FUNCTION check_wallet_ledger_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE wid uuid; actual numeric(20,2); calculated numeric(20,2);
      BEGIN
        IF TG_TABLE_NAME = 'wallets' THEN
          wid := NEW.id;
        ELSE
          wid := NEW.wallet_id;
        END IF;
        SELECT balance INTO actual FROM wallets WHERE id = wid;
        SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)
          INTO calculated FROM wallet_ledger WHERE wallet_id = wid;
        IF actual <> calculated THEN
          RAISE EXCEPTION 'wallet % balance % differs from ledger %', wid, actual, calculated;
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER wallet_ledger_on_wallet AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger_consistency();
      CREATE CONSTRAINT TRIGGER wallet_ledger_on_entry AFTER INSERT ON wallet_ledger
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger_consistency();
    `);
  }

  async down(q: QueryRunner): Promise<void> {
    await q.query(`
      DROP TABLE outbox_messages;
      DROP TABLE inbox_messages;
      DROP TABLE wallet_ledger;
      DROP TABLE wager_transactions;
      DROP TABLE wallets;
      DROP FUNCTION check_wallet_ledger_consistency();
      DROP FUNCTION prevent_ledger_mutation();
    `);
  }
}
