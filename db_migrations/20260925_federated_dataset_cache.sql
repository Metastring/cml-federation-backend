-- Federation Phase 2 (see ../FEDERATION_ARCHITECTURE.md §13): cross-node
-- search. Each node's searchable-dataset catalog (GET /node/catalog) is
-- harvested into this table by every other node that searches it, so a
-- search can be split by owning node without asking every peer "do you have
-- dataset X?" first. Metadata only -- the data itself is always queried live
-- on the owning node (reference model, §8).
--
-- Ships to every node's DB (central and clients both fan out searches).
-- Apply by hand to cml_federation_db and cml_node_db -- there is no migration
-- runner.

BEGIN;

CREATE TABLE IF NOT EXISTS federated_dataset_cache (
    id                  BIGSERIAL PRIMARY KEY,
    origin_node_id      TEXT NOT NULL,
    origin_node_name    TEXT,
    origin_base_url     TEXT NOT NULL,
    remote_dataset_id   INTEGER NOT NULL,
    title               TEXT NOT NULL,
    category            TEXT,
    description         TEXT,
    keywords            TEXT,
    fields              JSONB NOT NULL DEFAULT '[]'::jsonb,
    row_count           BIGINT,
    date_min            DATE,
    date_max            DATE,
    catalog_hash        TEXT,
    harvested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (origin_base_url, remote_dataset_id)
);

CREATE INDEX IF NOT EXISTS federated_dataset_cache_title_idx ON federated_dataset_cache (lower(title));
CREATE INDEX IF NOT EXISTS federated_dataset_cache_category_idx ON federated_dataset_cache (lower(category));

COMMIT;
