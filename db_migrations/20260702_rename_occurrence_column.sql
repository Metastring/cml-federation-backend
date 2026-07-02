-- Fix misspelled column name on dataset_master: is_occurance_available -> is_occurrence_available

BEGIN;

ALTER TABLE dataset_master
    RENAME COLUMN is_occurance_available TO is_occurrence_available;

COMMIT;
