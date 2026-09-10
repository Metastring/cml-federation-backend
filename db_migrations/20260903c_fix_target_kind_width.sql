-- custom_class_mapping.target_kind was VARCHAR(16), too narrow for the value
-- 'external_ontology' (17 chars) -- caught by the first end-to-end test of
-- POST /ontology/builder/{graph_key}/classes/{name}/mappings.

BEGIN;

ALTER TABLE custom_class_mapping
    ALTER COLUMN target_kind TYPE VARCHAR(24);

COMMIT;
