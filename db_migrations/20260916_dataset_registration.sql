-- Backs the registration wizard's step 1 (source & details) and step 3
-- (review & publish). Step 2 (ontology mapping) already has real tables
-- (dataset_mapping, custom_ontology) from earlier migrations.
--
-- dataset_source_config: one row per dataset, holding whichever source type
-- (file / url / database) the contributor picked in step 1, plus the fields
-- detected from it. Columns for the other two source types stay NULL --
-- cheaper than three separate tables for what is a small, rarely-queried
-- config blob, and keeps "replace the source" (UNIQUE dataset_id + upsert)
-- a single statement instead of a delete+insert across tables.
--
-- ontology_term_proposal: "propose a new ontology term" from step 2, when
-- nothing in the chosen ontology fits. Deliberately NOT written into
-- custom_ontology directly -- stays a pending row until someone reviews it
-- (product decision 2026-09-16).

BEGIN;

CREATE TABLE IF NOT EXISTS dataset_source_config (
    id                     SERIAL PRIMARY KEY,
    dataset_id             INTEGER NOT NULL REFERENCES dataset_master(dataset_id) ON DELETE CASCADE,
    source_type            VARCHAR(20) NOT NULL CHECK (source_type IN ('file','url','database')),

    -- source_type = 'file'
    file_name              TEXT,
    file_path              TEXT,
    file_format             VARCHAR(20),

    -- source_type = 'url'
    source_url              TEXT,
    http_method              VARCHAR(10),
    auth_header               TEXT,
    response_format           VARCHAR(20),

    -- source_type = 'database' (.sql dump upload only -- no live DB connection yet)
    dump_file_name            TEXT,
    dump_file_path             TEXT,
    selected_table_name         TEXT,

    -- shared: [{"field_name": "...", "sample_value": "...", "data_type": "..."}]
    -- for a 'database' source before a table is picked, this is instead
    -- {"tables": [{"table_name": "...", "columns": [...]}]}
    detected_fields         JSONB,

    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_id)
);

CREATE TABLE IF NOT EXISTS ontology_term_proposal (
    id                          SERIAL PRIMARY KEY,
    dataset_id                  INTEGER NOT NULL REFERENCES dataset_master(dataset_id) ON DELETE CASCADE,
    field_name                   VARCHAR(255) NOT NULL,
    proposed_label                 TEXT NOT NULL,
    proposed_definition              TEXT,
    target_ontology_graph_key          VARCHAR(64),
    status                       VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at                     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_ontology_term_proposal_status ON ontology_term_proposal(status);

COMMIT;
