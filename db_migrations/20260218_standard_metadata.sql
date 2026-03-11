-- Standard metadata model based on 6 Atlan categories
-- This file defines core dataset_master adjustments and metadata category tables.
-- Run this against your Postgres database after reviewing names/types with your DBA.

BEGIN;

-- 1) Core entity: dataset_master
-- NOTE: Adjust existing dataset_master instead of recreating it.
-- These statements assume dataset_master already exists.

-- Example: add new columns if they don't exist yet.
-- You may need to modify types/names to match your current schema.

ALTER TABLE dataset_master
    ADD COLUMN IF NOT EXISTS dataset_version        VARCHAR(50),
    ADD COLUMN IF NOT EXISTS schema_version         VARCHAR(50),
    ADD COLUMN IF NOT EXISTS domain                 VARCHAR(255),
    ADD COLUMN IF NOT EXISTS source_system          VARCHAR(255),
    ADD COLUMN IF NOT EXISTS status                 VARCHAR(50),
    ADD COLUMN IF NOT EXISTS created_at             TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at             TIMESTAMPTZ DEFAULT NOW();

-- dataset_type is already present in your code; make sure it is compatible
-- with the new semantics (table/api/file/graph). If needed, constrain values:
-- ALTER TABLE dataset_master
--     ADD CONSTRAINT dataset_type_check
--     CHECK (dataset_type IN ('table','api','file','graph'));


-- 2) Metadata category tables (1:1 with dataset_master)

CREATE TABLE IF NOT EXISTS technical_metadata (
    technical_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id        INTEGER NOT NULL,

    data_format       VARCHAR(100),
    encoding          VARCHAR(50),
    row_count         BIGINT,
    column_count      INTEGER,
    file_size         BIGINT, -- store bytes; convert to MB in UI if needed
    primary_key       VARCHAR(255),
    storage_location  TEXT,
    api_endpoint      TEXT,
    rdf_graph_uri     TEXT,
    ontology_reference TEXT,

    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_technical_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_technical_dataset
        UNIQUE (dataset_id)
);

CREATE TABLE IF NOT EXISTS governance_metadata (
    governance_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id         INTEGER NOT NULL,

    data_owner         VARCHAR(255),
    data_steward       VARCHAR(255),
    owner_email        VARCHAR(255),
    compliance_status  VARCHAR(100),
    access_level       VARCHAR(50),
    data_classification VARCHAR(100),
    retention_policy   TEXT,
    pii_present        BOOLEAN,
    license            VARCHAR(255),

    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_governance_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_governance_dataset
        UNIQUE (dataset_id)
);

CREATE TABLE IF NOT EXISTS operational_metadata (
    operational_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id          INTEGER NOT NULL,

    publish_date        DATE,
    registration_date   DATE,
    last_refresh_date   DATE,
    refresh_frequency   VARCHAR(50),
    refresh_method      VARCHAR(100),
    environment         VARCHAR(50),
    ingestion_pipeline  VARCHAR(255),
    supported_by        VARCHAR(255),
    last_job_run_time   TIMESTAMPTZ,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_operational_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_operational_dataset
        UNIQUE (dataset_id)
);

CREATE TABLE IF NOT EXISTS collaboration_metadata (
    collaboration_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id          INTEGER NOT NULL,

    review_status       VARCHAR(50),
    rating              NUMERIC(3,2),   -- 0.00 - 9.99; adjust as needed
    documentation_link  TEXT,
    tags                TEXT,          -- optional; you also have normalized tags below

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_collaboration_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_collaboration_dataset
        UNIQUE (dataset_id)
);

CREATE TABLE IF NOT EXISTS quality_metadata (
    quality_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id              INTEGER NOT NULL,

    completeness_score      NUMERIC(5,2),
    accuracy_score          NUMERIC(5,2),
    freshness_score         NUMERIC(5,2),
    consistency_score       NUMERIC(5,2),
    duplicate_count         BIGINT,
    null_percentage         NUMERIC(5,2),
    validation_status       VARCHAR(50),
    last_quality_check_date DATE,

    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_quality_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_quality_dataset
        UNIQUE (dataset_id)
);

CREATE TABLE IF NOT EXISTS usage_metadata (
    usage_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id         INTEGER NOT NULL,

    query_count        BIGINT,
    download_count     BIGINT,
    api_call_count     BIGINT,
    last_accessed      TIMESTAMPTZ,
    active_users_count BIGINT,
    popularity_score   NUMERIC(5,2),

    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_usage_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_usage_dataset
        UNIQUE (dataset_id)
);


-- 3) Dataset roles (integrates with external user system)

-- Note: we do NOT create a users table here because you
-- already have a separate user module. We *do* keep a
-- foreign key to that users table, assuming it exists in
-- this database as users(user_id).

CREATE TABLE IF NOT EXISTS dataset_users (
    dataset_user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id      INTEGER NOT NULL,
    user_id         UUID NOT NULL,
    role_type       VARCHAR(50) NOT NULL, -- owner/steward/contributor/reviewer

    created_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_dataset_users_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE
    -- NOTE: Add a FOREIGN KEY to users(user_id) separately once you confirm
    -- that users.user_id has type UUID (or adjust this column type accordingly).
);

-- 4) Tags and dataset_tags (many-to-many)

CREATE TABLE IF NOT EXISTS tags (
    tag_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tag_name    VARCHAR(100) UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dataset_tags (
    dataset_id  INTEGER NOT NULL,
    tag_id      UUID NOT NULL,

    PRIMARY KEY (dataset_id, tag_id),

    CONSTRAINT fk_dataset_tags_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_dataset_tags_tag
        FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
        ON DELETE CASCADE
);

COMMIT;
