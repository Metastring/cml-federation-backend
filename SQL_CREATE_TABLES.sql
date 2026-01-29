-- Ontology Management System - PostgreSQL CREATE TABLE Statements
-- Run these queries directly in your PostgreSQL database to create all ontology tables

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- 1. Create cml_domain table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_domain (
    domain_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_code VARCHAR(100) NOT NULL UNIQUE,
    domain_name VARCHAR(255) NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX ix_domain_code ON cml_domain(domain_code);
CREATE INDEX ix_domain_is_active ON cml_domain(is_active);

-- ============================================================================
-- 2. Create cml_ontology_version table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_ontology_version (
    ontology_version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id UUID NOT NULL,
    version_code VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'deprecated')),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (domain_id) REFERENCES cml_domain(domain_id) ON DELETE CASCADE,
    CONSTRAINT uq_domain_version UNIQUE(domain_id, version_code)
);

CREATE INDEX ix_ontology_version_code ON cml_ontology_version(version_code);
CREATE INDEX ix_ontology_status ON cml_ontology_version(status);
CREATE INDEX ix_ontology_domain_id ON cml_ontology_version(domain_id);

-- ============================================================================
-- 3. Create cml_concept table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_concept (
    concept_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ontology_version_id UUID NOT NULL,
    concept_code VARCHAR(100) NOT NULL,
    concept_name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_concept_id UUID,
    is_abstract BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (ontology_version_id) REFERENCES cml_ontology_version(ontology_version_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_concept_id) REFERENCES cml_concept(concept_id) ON DELETE SET NULL,
    CONSTRAINT uq_version_concept UNIQUE(ontology_version_id, concept_code)
);

CREATE INDEX ix_concept_code ON cml_concept(concept_code);
CREATE INDEX ix_concept_ontology_version_id ON cml_concept(ontology_version_id);
CREATE INDEX ix_concept_parent_id ON cml_concept(parent_concept_id);

-- ============================================================================
-- 4. Create cml_property table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_property (
    property_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    concept_id UUID NOT NULL,
    property_code VARCHAR(100) NOT NULL,
    property_name VARCHAR(255) NOT NULL,
    description TEXT,
    data_type VARCHAR(50) NOT NULL,
    is_required BOOLEAN NOT NULL DEFAULT FALSE,
    is_multivalued BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    CONSTRAINT uq_concept_property UNIQUE(concept_id, property_code)
);

CREATE INDEX ix_property_code ON cml_property(property_code);
CREATE INDEX ix_property_concept_id ON cml_property(concept_id);
CREATE INDEX ix_property_data_type ON cml_property(data_type);

-- ============================================================================
-- 5. Create cml_property_constraint table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_property_constraint (
    constraint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID NOT NULL,
    constraint_type VARCHAR(50) NOT NULL,
    constraint_value JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (property_id) REFERENCES cml_property(property_id) ON DELETE CASCADE
);

CREATE INDEX ix_property_constraint_property_id ON cml_property_constraint(property_id);
CREATE INDEX ix_property_constraint_type ON cml_property_constraint(constraint_type);

-- ============================================================================
-- 6. Create cml_vocab table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_vocab (
    vocab_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vocab_code VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX ix_vocab_code ON cml_vocab(vocab_code);

-- ============================================================================
-- 7. Create cml_vocab_term table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_vocab_term (
    term_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vocab_id UUID NOT NULL,
    term_code VARCHAR(100) NOT NULL,
    term_label VARCHAR(255) NOT NULL,
    description TEXT,
    parent_term_id UUID,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (vocab_id) REFERENCES cml_vocab(vocab_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_term_id) REFERENCES cml_vocab_term(term_id) ON DELETE SET NULL,
    CONSTRAINT uq_vocab_term UNIQUE(vocab_id, term_code)
);

CREATE INDEX ix_vocab_term_vocab_id ON cml_vocab_term(vocab_id);
CREATE INDEX ix_vocab_term_code ON cml_vocab_term(term_code);
CREATE INDEX ix_vocab_term_parent_id ON cml_vocab_term(parent_term_id);

-- ============================================================================
-- 8. Create cml_dataset_field_mapping table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_dataset_field_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id INTEGER NOT NULL,
    concept_id UUID NOT NULL,
    property_id UUID NOT NULL,
    external_field_name VARCHAR(255) NOT NULL,
    external_data_type VARCHAR(50),
    transform_rule JSONB,
    is_exposed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    FOREIGN KEY (property_id) REFERENCES cml_property(property_id) ON DELETE CASCADE,
    CONSTRAINT uq_dataset_field UNIQUE(dataset_id, external_field_name)
);

CREATE INDEX ix_dataset_field_mapping_dataset_id ON cml_dataset_field_mapping(dataset_id);
CREATE INDEX ix_dataset_field_mapping_concept_id ON cml_dataset_field_mapping(concept_id);
CREATE INDEX ix_dataset_field_mapping_property_id ON cml_dataset_field_mapping(property_id);

-- ============================================================================
-- 9. Create cml_dataset_concept table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_dataset_concept (
    dataset_concept_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id INTEGER NOT NULL,
    concept_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    CONSTRAINT uq_dataset_concept UNIQUE(dataset_id, concept_id)
);

CREATE INDEX ix_dataset_concept_dataset_id ON cml_dataset_concept(dataset_id);
CREATE INDEX ix_dataset_concept_concept_id ON cml_dataset_concept(concept_id);

-- ============================================================================
-- 10. Create cml_ontology_change_log table
-- ============================================================================
CREATE TABLE IF NOT EXISTS cml_ontology_change_log (
    change_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ontology_version_id UUID NOT NULL,
    change_type VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    changed_by VARCHAR(255),
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ontology_version_id) REFERENCES cml_ontology_version(ontology_version_id) ON DELETE CASCADE
);

CREATE INDEX ix_change_log_ontology_version_id ON cml_ontology_change_log(ontology_version_id);
CREATE INDEX ix_change_log_type ON cml_ontology_change_log(change_type);
CREATE INDEX ix_change_log_changed_at ON cml_ontology_change_log(changed_at);

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- List all tables
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'cml_%';

-- List all indexes
-- SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename LIKE 'cml_%';

-- Check table structure (replace with table name)
-- \d cml_domain

-- ============================================================================
-- Sample Data Insertion (Optional - for testing)
-- ============================================================================

-- Insert a domain
-- INSERT INTO cml_domain (domain_code, domain_name, description)
-- VALUES ('biodiversity', 'Biodiversity Domain', 'Domain for biodiversity data')
-- RETURNING domain_id;

-- Insert ontology version
-- INSERT INTO cml_ontology_version (domain_id, version_code, status, description)
-- VALUES ('YOUR_DOMAIN_ID', 'v1.0', 'draft', 'Initial version')
-- RETURNING ontology_version_id;

-- Insert concept
-- INSERT INTO cml_concept (ontology_version_id, concept_code, concept_name, description, is_abstract)
-- VALUES ('YOUR_VERSION_ID', 'Species', 'Species', 'Species concept', FALSE)
-- RETURNING concept_id;

-- Insert property
-- INSERT INTO cml_property (concept_id, property_code, property_name, data_type, is_required)
-- VALUES ('YOUR_CONCEPT_ID', 'scientificName', 'Scientific Name', 'string', TRUE)
-- RETURNING property_id;

-- Insert vocabulary
-- INSERT INTO cml_vocab (vocab_code, description)
-- VALUES ('conservation_status', 'Conservation status vocabulary')
-- RETURNING vocab_id;

-- Insert vocabulary term
-- INSERT INTO cml_vocab_term (vocab_id, term_code, term_label, description)
-- VALUES ('YOUR_VOCAB_ID', 'EN', 'Endangered', 'Endangered species')
-- RETURNING term_id;
