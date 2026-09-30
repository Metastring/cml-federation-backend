-- Per-field metadata on dataset_mapping, captured by the registration
-- frontend alongside each field -> ontology mapping and returned with the
-- dataset's fields by /categories-with-datasets, /metadata and
-- /dataset-ontology-mapping/{id}/mappings:
--   ontology_uri -- the ontology term the field maps to. For a field the
--                   contributor couldn't find in any ontology this is the
--                   only link to a vocabulary (ontology_mapping is then
--                   just the field name).
--   sample_value -- a few example values from the source, "; "-separated
--   value_range  -- "min – max" for numeric/date fields, or the allowed
--                   values for a small categorical field
--   metadata     -- free-form details about the field (label, definition,
--                   unit, source column type, ...)
-- Existing rows are backfilled by 20260930b_dataset_mapping_field_metadata_seed.sql.

BEGIN;

ALTER TABLE dataset_mapping
    ADD COLUMN IF NOT EXISTS ontology_uri TEXT,
    ADD COLUMN IF NOT EXISTS sample_value TEXT,
    ADD COLUMN IF NOT EXISTS value_range TEXT,
    ADD COLUMN IF NOT EXISTS metadata JSONB;

CREATE INDEX IF NOT EXISTS ix_dataset_mapping_ontology_uri ON dataset_mapping(ontology_uri);

COMMIT;
