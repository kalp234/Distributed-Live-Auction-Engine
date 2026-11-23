-- ============================================================================
-- Shard A — Auction DB (cluster scope "auction": pg-a1, pg-a2, pg-a3)
-- Holds auction state + the 2PC coordinator log (schema coord).
-- ============================================================================

-- Reference data (users are needed here for bid history display).
CREATE TABLE users (
    id   int  PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE items (
    id             serial PRIMARY KEY,
    title          text          NOT NULL,
    seller_id      int           NOT NULL REFERENCES users(id),
    start_price    numeric(12,2) NOT NULL CHECK (start_price >= 0),
    current_price  numeric(12,2) NOT NULL,
    highest_bidder int           REFERENCES users(id),
    status         text          NOT NULL DEFAULT 'open'
                   CHECK (status IN ('open', 'sold', 'closed')),  -- closed = ended unsold
    ends_at        timestamptz   NOT NULL,
    -- [T4] optimistic-concurrency version, bumped on every accepted bid / close
    version        int           NOT NULL DEFAULT 0,
    created_at     timestamptz   NOT NULL DEFAULT now()
);
CREATE INDEX items_open_ends_at ON items (ends_at) WHERE status = 'open';

CREATE TABLE bids (
    id              bigserial     PRIMARY KEY,
    item_id         int           NOT NULL REFERENCES items(id),
    user_id         int           NOT NULL REFERENCES users(id),
    amount          numeric(12,2) NOT NULL CHECK (amount > 0),
    ts              timestamptz   NOT NULL DEFAULT now(),
    txn_id          uuid          NOT NULL,          -- global 2PC transaction id
    -- [T4] idempotency: a retried request with the same key can never create a 2nd bid
    idempotency_key text          UNIQUE,
    served_by       text                              -- app node that coordinated it
);
CREATE INDEX bids_item_amount ON bids (item_id, amount DESC);

-- [T4][T5] Durable 2PC coordinator log. Lives in the sync-replicated Shard A cluster,
-- so any app node can resolve another (crashed) node's in-doubt transactions.
CREATE SCHEMA coord;
CREATE TABLE coord.txn_log (
    txn_id       uuid        PRIMARY KEY,
    kind         text        NOT NULL,             -- 'bid' | 'close'
    state        text        NOT NULL
                 CHECK (state IN ('INIT', 'PREPARED', 'COMMITTED', 'ABORTED')),
    participants text[]      NOT NULL,             -- e.g. {auction,wallet}
    coordinator  text        NOT NULL,             -- node id that started it
    detail       jsonb,
    completed    boolean     NOT NULL DEFAULT false, -- phase 2 done on every participant
    ts           timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX txn_log_pending ON coord.txn_log (ts) WHERE NOT completed;
