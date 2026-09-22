-- Federation Phase 1 (see ../FEDERATION_ARCHITECTURE.md §4 and §12 "Working
-- order"): central node registry + audit log for register/heartbeat/detach
-- events.
--
-- This migration ships to every node's DB (all nodes run identical code,
-- per §3), even though node_registry is only meaningfully populated on
-- whichever instance is acting as central for a given deployment.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

CREATE TABLE IF NOT EXISTS node_registry (
    node_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    base_url            TEXT NOT NULL,
    sparql_endpoint     TEXT,
    geoserver_url       TEXT,
    maintained_by       TEXT,
    status              VARCHAR(20) NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'active', 'stale', 'detached', 'revoked')),
    api_key_hash        TEXT,
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at   TIMESTAMPTZ,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- One registry row per node base_url -- re-registering the same node
-- updates its row (and reissues its key) instead of creating a duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS node_registry_base_url_key ON node_registry (base_url);

CREATE TABLE IF NOT EXISTS federation_sync_log (
    id           BIGSERIAL PRIMARY KEY,
    node_id      UUID REFERENCES node_registry(node_id) ON DELETE CASCADE,
    event_type   VARCHAR(30) NOT NULL
                     CHECK (event_type IN ('register', 'heartbeat', 'detach', 'revoke', 'stale')),
    detail       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS federation_sync_log_node_id_idx ON federation_sync_log (node_id);

COMMIT;
