-- Track when a custom ontology's draft was last pushed live (Publish is an
-- in-place overwrite, not tied to a version snapshot -- see
-- custom_ontology_version's own comment in 20260903_custom_ontology_builder.sql).

BEGIN;

ALTER TABLE custom_ontology
    ADD COLUMN IF NOT EXISTS last_published_at TIMESTAMPTZ;

COMMIT;
