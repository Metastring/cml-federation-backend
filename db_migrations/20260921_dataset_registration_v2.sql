-- v2 registration wizard: "federated node" model. Unlike the v1 wizard
-- (20260916_dataset_registration.sql), which uploaded files and .sql dumps
-- into cml-backend, v2 never takes a copy of the contributor's data -- it
-- stores a REFERENCE (a URI, a read-only connection string, or a map
-- service layer) and reads just enough of it (a header, a sample row, a
-- GetCapabilities document) to drive ontology-mapping suggestions.
--
-- v1's columns (file_name/file_path for an uploaded file, dump_file_name/
-- dump_file_path for an uploaded .sql dump) are untouched -- those
-- endpoints still work as-is. This migration only adds the columns v2
-- needs alongside them, plus 'map_service' as a fourth source_type.
--
-- Scope decision (2026-09-21, matches the same call made for v1's database
-- source): live database introspection ships for PostgreSQL only, since
-- psycopg2 is the only DB driver already in requirements.txt. Other engines
-- can be *recorded* (db_engine, db_connection_string) so the draft isn't
-- blocked, but "Verify reachability" returns a clear "not yet supported"
-- response for them rather than attempting a connection with no driver.

BEGIN;

ALTER TABLE dataset_master
    ADD COLUMN IF NOT EXISTS node_name             TEXT,
    ADD COLUMN IF NOT EXISTS node_maintained_by     TEXT;

ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS dataset_source_config_source_type_check;

ALTER TABLE dataset_source_config
    ADD CONSTRAINT dataset_source_config_source_type_check
    CHECK (source_type IN ('file', 'url', 'database', 'map_service'));

ALTER TABLE dataset_source_config
    -- source_type = 'file', v2 reference mode: a URI on the contributor's
    -- own infrastructure (s3://, nfs://, smb://, a plain path, ...), never
    -- fetched in full -- only a byte-range read to sniff a header row.
    ADD COLUMN IF NOT EXISTS reference_uri          TEXT,
    -- Not a raw secret -- a reference/label for a credential the operator
    -- manages elsewhere. Encrypted secret storage is flagged, not built,
    -- in this pass (see registration-api-plan.html, "looking ahead").
    ADD COLUMN IF NOT EXISTS access_credentials_ref  TEXT,

    -- source_type = 'database', v2 reference mode: a read-only connection
    -- string the operator controls, plus which table/view to register.
    ADD COLUMN IF NOT EXISTS db_connection_string    TEXT,
    ADD COLUMN IF NOT EXISTS db_engine               VARCHAR(30),

    -- source_type = 'map_service': a WMS/WFS/WMTS layer already hosted on
    -- the contributor's own GeoServer/MapServer -- not map-module-backend.
    ADD COLUMN IF NOT EXISTS map_service_url         TEXT,
    ADD COLUMN IF NOT EXISTS layer_type              VARCHAR(20),
    ADD COLUMN IF NOT EXISTS map_layer_name           TEXT,

    -- Shared across all four v2 reference types -- "Verify reachability"
    -- writes here instead of a separate table per type.
    ADD COLUMN IF NOT EXISTS reachability_status      VARCHAR(20)
        CHECK (reachability_status IN ('unverified', 'reachable', 'unreachable', 'unsupported')),
    ADD COLUMN IF NOT EXISTS reachability_checked_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reachability_detail       TEXT;

COMMIT;
