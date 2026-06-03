-- Consolidate metadata table into dataset_master + map_layer_info
-- Run order: Steps 1-3 must run before code is deployed. Step 4 (DROP TABLE) runs after verifying the new code.

BEGIN;

-- Step 1: Add map-specific columns to dataset_master
ALTER TABLE dataset_master
    ADD COLUMN IF NOT EXISTS theme              VARCHAR(255),
    ADD COLUMN IF NOT EXISTS access_constraints TEXT,
    ADD COLUMN IF NOT EXISTS use_constraints    TEXT;

-- Step 2: Create map_layer_info (stores only geoserver link, FK to dataset_master)
CREATE TABLE IF NOT EXISTS map_layer_info (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id     INTEGER      NOT NULL,
    geoserver_name VARCHAR(255) NOT NULL UNIQUE,
    created_at     TIMESTAMPTZ  DEFAULT NOW(),
    updated_at     TIMESTAMPTZ,
    CONSTRAINT fk_map_layer_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset_master(dataset_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_map_layer_info_geoserver_name ON map_layer_info(geoserver_name);

-- Step 3: Migrate existing metadata rows
-- Each metadata row becomes: one dataset_master row + one map_layer_info row + one dataset_contacts row
DO $$
DECLARE
    m     RECORD;
    dm_id INTEGER;
BEGIN
    FOR m IN SELECT * FROM metadata LOOP
        INSERT INTO dataset_master (
            title, description, keywords, dataset_type,
            theme, access_constraints, use_constraints,
            is_active, created_at, updated_at
        ) VALUES (
            m.name_of_dataset,
            m.purpose_of_creating_data,
            array_to_string(m.keywords, '; '),
            m.data_type,
            m.theme,
            m.access_constraints,
            m.use_constraints,
            TRUE,
            m.created_on,
            m.updated_on
        ) RETURNING dataset_id INTO dm_id;

        INSERT INTO map_layer_info (id, dataset_id, geoserver_name, created_at, updated_at)
        VALUES (m.id, dm_id, m.geoserver_name, m.created_on, m.updated_on)
        ON CONFLICT (geoserver_name) DO NOTHING;

        IF m.contact_person IS NOT NULL OR m.contact_email IS NOT NULL OR m.organization IS NOT NULL THEN
            INSERT INTO dataset_contacts (dataset_id, name, email, organization, address, city, country)
            VALUES (dm_id, m.contact_person, m.contact_email, m.organization,
                    m.mailing_address, m.city_locality_country, m.country);
        END IF;
    END LOOP;
END $$;

COMMIT;

-- Step 4: Drop metadata table (run AFTER verifying the new code works)
-- DROP TABLE metadata;
