-- This server's own registry membership: the member id and API key it got
-- back from POST /nodes/register. Until now these lived only in .env
-- (CENTRAL_NODE_ID / NODE_API_KEY), so every re-registration from the UI
-- rotated the key without anything updating .env, and the heartbeat broke
-- silently. The Node Registry page now registers/detaches this server
-- through its own backend (POST /node/self/*), which keeps the key here and
-- never hands it to the browser. .env values are still used as a fallback
-- when this table is empty.
--
-- One row at most (id = 1). Lives in each server's own DB, not the registry's.

BEGIN;

CREATE TABLE IF NOT EXISTS node_membership (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    member_id TEXT NOT NULL,
    api_key TEXT NOT NULL,
    registry_url TEXT NOT NULL,
    registered_name TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
