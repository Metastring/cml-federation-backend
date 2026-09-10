-- Custom Ontology Builder (GET/POST /ontology/builder/*): lets a user author
-- their own ontology (details -> class tree -> properties -> mappings) in the
-- UI. Distinct from the predefined ontology, which stays file-based
-- (ontology-ttl-files/cphr-ayurveda-ontology.ttl). A custom ontology lives in
-- these tables while being edited; "Publish" serializes it to Turtle and
-- reuses the existing app/ontology_publish.py registry/save_ttl/push_to_fuseki
-- functions, so it lands as ontology-ttl-files/{graph_key}.ttl + a Fuseki
-- named graph exactly like the predefined one does.
--
-- No owner/user column anywhere here (per product decision 2026-09-03): a
-- separate user module will add ownership later, additively.

BEGIN;

CREATE TABLE IF NOT EXISTS custom_ontology (
    graph_key          VARCHAR(64)  PRIMARY KEY,  -- slug; same convention as ontology_publish.py's graph_key
    title              TEXT         NOT NULL,
    acronym            VARCHAR(32)  NOT NULL,
    visibility         VARCHAR(16)  NOT NULL DEFAULT 'Public'  CHECK (visibility IN ('Public','Private')),
    status             VARCHAR(16)  NOT NULL DEFAULT 'Staging' CHECK (status IN ('Staging','Beta','Production','Retired')),
    description        TEXT,
    categories         TEXT[]       NOT NULL DEFAULT '{}',
    bibliographic_refs TEXT[]       NOT NULL DEFAULT '{}',
    contact            TEXT,
    published_version  INTEGER,                   -- custom_ontology_version.version_no last pushed live; NULL until first publish
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Single-parent class tree (rdfs:subClassOf). Cross-ontology / non-tree
-- relationships go through custom_class_mapping instead of a second parent.
CREATE TABLE IF NOT EXISTS custom_class (
    id           SERIAL       PRIMARY KEY,
    graph_key    VARCHAR(64)  NOT NULL REFERENCES custom_ontology(graph_key) ON DELETE CASCADE,
    name         VARCHAR(128) NOT NULL,  -- local Turtle name, e.g. "Formulation"
    label        TEXT         NOT NULL,
    definition   TEXT,
    synonyms     TEXT[]       NOT NULL DEFAULT '{}',
    parent_name  VARCHAR(128),           -- NULL = top-level class
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (graph_key, name),
    -- self-referencing FK: parent must be a class in the SAME ontology; NULL
    -- parent_name bypasses the check (top-level classes). Default RESTRICT
    -- means a class can't be deleted while it still has children -- delete
    -- leaves first, same rule the builder API enforces.
    FOREIGN KEY (graph_key, parent_name) REFERENCES custom_class(graph_key, name)
);

CREATE INDEX IF NOT EXISTS ix_custom_class_parent ON custom_class(graph_key, parent_name);

CREATE TABLE IF NOT EXISTS custom_property (
    id                SERIAL       PRIMARY KEY,
    graph_key         VARCHAR(64)  NOT NULL REFERENCES custom_ontology(graph_key) ON DELETE CASCADE,
    name              VARCHAR(128) NOT NULL,
    label             TEXT         NOT NULL,
    comment           TEXT,
    property_type     VARCHAR(16)  NOT NULL CHECK (property_type IN ('datatype','object')),
    domain_class_name VARCHAR(128) NOT NULL,
    -- datatype: an xsd:* type name (validated in the app against a fixed
    -- enum); object: another class's name in this same ontology. Two
    -- different "universes" for one column, so this isn't FK-enforced --
    -- the app validates range_value against the right one for property_type.
    range_value       VARCHAR(128) NOT NULL,
    cardinality_note  VARCHAR(64),  -- documentation only (e.g. "0..1", "1..many"), not OWL-enforced
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (graph_key, name),
    -- domain class disappearing takes its properties with it (unlike a
    -- parent class, which is protected -- see custom_class's FK above)
    FOREIGN KEY (graph_key, domain_class_name) REFERENCES custom_class(graph_key, name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_custom_property_domain ON custom_property(graph_key, domain_class_name);

-- Mappings replace multi-inheritance (product decision 2026-09-03): a class
-- keeps a single parent, and is related to other classes -- in this
-- ontology, in another registered ontology, or to a raw external term --
-- through a mapping instead of a second rdfs:subClassOf.
CREATE TABLE IF NOT EXISTS custom_class_mapping (
    id                    SERIAL       PRIMARY KEY,
    graph_key             VARCHAR(64)  NOT NULL REFERENCES custom_ontology(graph_key) ON DELETE CASCADE,
    from_class_name       VARCHAR(128) NOT NULL,
    relation              VARCHAR(32)  NOT NULL CHECK (relation IN ('owl:equivalentClass','skos:exactMatch','skos:closeMatch','skos:relatedMatch')),
    target_kind           VARCHAR(24)  NOT NULL CHECK (target_kind IN ('internal','external_ontology','external_uri')),
    to_class_name         VARCHAR(128),  -- target_kind='internal': another class in THIS ontology (FK-checked below)
    to_ontology_graph_key VARCHAR(64),   -- target_kind='external_ontology': graph_key of another ontology (a custom one, or the predefined registry key e.g. 'ayurveda')
    to_ref                TEXT,          -- target_kind='external_ontology': the class name over there; target_kind='external_uri': a raw CURIE/URI, e.g. "dwc:Taxon"
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    FOREIGN KEY (graph_key, from_class_name) REFERENCES custom_class(graph_key, name) ON DELETE CASCADE,
    FOREIGN KEY (graph_key, to_class_name) REFERENCES custom_class(graph_key, name)
);

CREATE INDEX IF NOT EXISTS ix_custom_class_mapping_from ON custom_class_mapping(graph_key, from_class_name);

-- Explicit "+ New version" snapshots (product decision 2026-09-03: versioning
-- is a deliberate action, decoupled from Publish, which just overwrites the
-- live TTL/graph in place). Snapshots are TTL text, not their own Fuseki
-- graphs, to avoid graph sprawl -- add that later only if old versions need
-- to stay independently SPARQL-queryable.
CREATE TABLE IF NOT EXISTS custom_ontology_version (
    id                       SERIAL       PRIMARY KEY,
    graph_key                VARCHAR(64)  NOT NULL REFERENCES custom_ontology(graph_key) ON DELETE CASCADE,
    version_no               INTEGER      NOT NULL,
    ttl_content              TEXT         NOT NULL,
    triple_count             INTEGER      NOT NULL,
    class_count              INTEGER      NOT NULL,
    object_property_count    INTEGER      NOT NULL,
    datatype_property_count  INTEGER      NOT NULL,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (graph_key, version_no)
);

COMMIT;
