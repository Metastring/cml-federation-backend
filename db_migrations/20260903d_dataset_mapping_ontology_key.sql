-- dataset_mapping previously assumed a single ontology (the predefined
-- 'ayurveda' one) -- there was no column recording which ontology a mapping
-- was made against. Now that datasets can be mapped against either the
-- predefined ontology or any user-authored one (custom_ontology), record
-- that choice per mapping row. Existing rows all predate the custom-ontology
-- builder, so they're backfilled to 'ayurveda'.

BEGIN;

ALTER TABLE dataset_mapping
    ADD COLUMN IF NOT EXISTS ontology_graph_key VARCHAR(64);

UPDATE dataset_mapping
    SET ontology_graph_key = 'ayurveda'
    WHERE ontology_graph_key IS NULL;

ALTER TABLE dataset_mapping
    ALTER COLUMN ontology_graph_key SET DEFAULT 'ayurveda';

CREATE INDEX IF NOT EXISTS ix_dataset_mapping_ontology_graph_key ON dataset_mapping(ontology_graph_key);

COMMIT;
