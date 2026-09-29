-- CML term crosswalk: our own canonical terms (district, rainfall_daily, ...)
-- and every URI in the loaded ontologies that means the same (or a related)
-- thing. Different ontologies name one concept differently -- daily rainfall
-- is CF lwe_thickness_of_precipitation_amount, SWEET Precipitation, ENVO
-- "volume of hydrological precipitation" -- so without this, two datasets
-- mapped to different ontologies look unrelated and search returns them as
-- separate things. Resolve any URI or column name to a term_key, then group
-- by term_key.
--
-- Central owns these tables; nodes get a read-only copy (same migration +
-- seed) so a detached node can still resolve terms.
--
-- Source of truth for the rows: "ontology and datasets/registration/
-- cml_terms.json". Regenerate the seed (20260929b_*) with
-- "ontology and datasets/scripts/build_term_crosswalk.py" -- don't hand-edit it.

BEGIN;

CREATE TABLE IF NOT EXISTS cml_term (
    term_key    VARCHAR(64)  PRIMARY KEY,   -- e.g. 'district', 'rainfall_daily'
    label       TEXT         NOT NULL,
    definition  TEXT,
    category    VARCHAR(16)  NOT NULL CHECK (category IN ('join_key','location','time','measure','provenance')),
    domain      VARCHAR(32)  NOT NULL,      -- reference | climate | environment | energy | health | ...
    unit_uri    TEXT,                       -- QUDT unit, e.g. http://qudt.org/vocab/unit/MilliM
    -- raw column names (or long-format indicator strings) that mean this
    -- term in datasets seen so far; matched case-insensitively
    synonyms    TEXT[]       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cml_term_synonyms ON cml_term USING GIN (synonyms);

-- match_type says how the ontology term relates to OUR term (SKOS mapping
-- semantics): exact | close | broader (theirs is more general) | narrower |
-- related. Only exact/close should merge search results; broader/related
-- are for "see also".
CREATE TABLE IF NOT EXISTS cml_term_ontology_match (
    id                  SERIAL       PRIMARY KEY,
    term_key            VARCHAR(64)  NOT NULL REFERENCES cml_term(term_key) ON DELETE CASCADE ON UPDATE CASCADE,
    ontology_graph_key  VARCHAR(64)  NOT NULL,  -- graph_key as uploaded (Fuseki graph http://cml.org/ontology/{graph_key})
    term_uri            TEXT         NOT NULL,
    term_label          TEXT,                   -- label in that ontology, for display
    match_type          VARCHAR(16)  NOT NULL CHECK (match_type IN ('exact','close','broader','narrower','related')),
    is_primary          BOOLEAN      NOT NULL DEFAULT FALSE,  -- the URI we publish by default
    note                TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (term_key, term_uri)
);

-- reverse lookup: ontology URI -> our term(s)
CREATE INDEX IF NOT EXISTS ix_cml_term_ontology_match_uri ON cml_term_ontology_match(term_uri);
-- at most one primary URI per term
CREATE UNIQUE INDEX IF NOT EXISTS ux_cml_term_ontology_match_primary
    ON cml_term_ontology_match(term_key) WHERE is_primary;

COMMIT;
