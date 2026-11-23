-- ============================================================================
-- Shard B — Wallet DB (cluster scope "wallet": pg-b1, pg-b2)
-- Separate database server: a bid must atomically touch both shards [T4].
-- ============================================================================

CREATE TABLE wallets (
    user_id int           PRIMARY KEY,
    balance numeric(12,2) NOT NULL CHECK (balance >= 0),   -- total funds owned
    held    numeric(12,2) NOT NULL DEFAULT 0 CHECK (held >= 0), -- escrowed part
    CHECK (held <= balance)                                -- can't hold money you don't have
);

CREATE TABLE escrow_holds (
    id         bigserial     PRIMARY KEY,
    user_id    int           NOT NULL REFERENCES wallets(user_id),
    item_id    int           NOT NULL,     -- lives on Shard A (no cross-shard FK)
    amount     numeric(12,2) NOT NULL CHECK (amount > 0),
    txn_id     uuid          NOT NULL,     -- 2PC txn that created the hold
    status     text          NOT NULL CHECK (status IN ('active', 'released', 'settled')),
    closed_by  uuid,                       -- 2PC txn that released/settled it
    created_at timestamptz   NOT NULL DEFAULT now(),
    updated_at timestamptz   NOT NULL DEFAULT now()
);
-- [T4] DB-enforced invariant: at most ONE active hold (= the highest bid) per item.
CREATE UNIQUE INDEX escrow_one_active_per_item ON escrow_holds (item_id) WHERE status = 'active';
CREATE INDEX escrow_user ON escrow_holds (user_id);
