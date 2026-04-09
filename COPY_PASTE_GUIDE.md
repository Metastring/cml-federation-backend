# Copy-Paste Guide - Ontology Setup

## Overview

This guide provides exact commands and SQL you can copy-paste to set up the complete ontology system.

---

## Step 1: Create PostgreSQL Database (2 minutes)

### Copy-Paste Command 1: Create Database

```bash
createdb ontology_db
```

### Copy-Paste Command 2: Connect to Database

```bash
psql -d ontology_db
```

### Inside psql - Enable UUID Extension

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
\q
```

---

## Step 2: Load SQL Schema (1 minute)

### Copy-Paste Command 3: Run SQL File

```bash
psql -d ontology_db -f SQL_CREATE_TABLES.sql
```

### Copy-Paste Command 4: Verify Tables Created

```bash
psql -d ontology_db -c "\dt cml_*"
```

**Expected Output:**
```
             List of relations
 Schema |           Name            | Type  | Owner
--------+---------------------------+-------+-------
 public | cml_concept               | table | user
 public | cml_dataset_concept       | table | user
 public | cml_dataset_field_mapping | table | user
 public | cml_domain                | table | user
 public | cml_ontology_change_log   | table | user
 public | cml_ontology_version      | table | user
 public | cml_property              | table | user
 public | cml_property_constraint   | table | user
 public | cml_vocab                 | table | user
 public | cml_vocab_term            | table | user
(10 rows)
```

---

## Step 3: Configure Python Environment (3 minutes)

### Copy-Paste Command 5: Create .env File

```bash
cat > .env << 'EOF'
DATABASE_URL=postgresql://postgres:password@localhost:5432/ontology_db
DEBUG=True
ALLOWED_ORIGINS=["http://localhost:3000","http://localhost:8000"]
EOF
```

### Copy-Paste Command 6: Activate Virtual Environment

```bash
source env/bin/activate
```

### Copy-Paste Command 7: Install Dependencies

```bash
pip install -r requirements.txt
```

### Copy-Paste Command 8: Verify Installation

```bash
pip list | grep -E "fastapi|sqlalchemy|pydantic|psycopg2"
```

---

## Step 4: Run Tests (5 minutes)

### Copy-Paste Command 9: Run Unit Tests

```bash
pytest tests/unit/test_ontology_repository.py -v
```

**Expected:** 31+ tests passing ✅

### Copy-Paste Command 10: Run Integration Tests

```bash
pytest tests/integration/test_ontology.py -v
```

**Expected:** 11+ tests passing ✅

### Copy-Paste Command 11: Run All Tests with Coverage

```bash
pytest tests/ -v --cov=app --cov-report=term-missing
```

**Expected:** 42+ tests passing, 80%+ coverage ✅

---

## Step 5: Start API Server (2 minutes)

### Copy-Paste Command 12: Start Server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Expected Output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete
```

Open browser: **http://localhost:8000/docs**

---

## Step 6: Test API Endpoints (5 minutes)

### Copy-Paste Command 13: Create Domain

**Using curl:**
```bash
curl -X POST http://localhost:8000/api/v1/ontology/domains \
  -H "Content-Type: application/json" \
  -d '{
    "domain_code": "biodiversity",
    "domain_name": "Biodiversity Domain",
    "description": "Domain for managing biodiversity data"
  }'
```

**Expected Response:**
```json
{
  "domain_id": "550e8400-e29b-41d4-a716-446655440000",
  "domain_code": "biodiversity",
  "domain_name": "Biodiversity Domain",
  "description": "Domain for managing biodiversity data",
  "is_active": true,
  "created_at": "2024-01-15T10:30:45.123456Z",
  "updated_at": null
}
```

### Copy-Paste Command 14: List Domains

```bash
curl http://localhost:8000/api/v1/ontology/domains
```

### Copy-Paste Command 15: Save Domain ID (for next steps)

```bash
# After creating domain, copy the domain_id from response
# Example: 550e8400-e29b-41d4-a716-446655440000

DOMAIN_ID="550e8400-e29b-41d4-a716-446655440000"
```

### Copy-Paste Command 16: Create Ontology Version

```bash
curl -X POST http://localhost:8000/api/v1/ontology/versions \
  -H "Content-Type: application/json" \
  -d "{
    \"domain_id\": \"${DOMAIN_ID}\",
    \"version_code\": \"v1.0.0\",
    \"status\": \"draft\"
  }"
```

**Expected Response:** 201 Created with version_id

### Copy-Paste Command 17: Create Concept

```bash
# Save version_id from previous response
VERSION_ID="550e8400-e29b-41d4-a716-446655440010"

curl -X POST http://localhost:8000/api/v1/ontology/concepts \
  -H "Content-Type: application/json" \
  -d "{
    \"ontology_version_id\": \"${VERSION_ID}\",
    \"concept_code\": \"Organism\",
    \"concept_name\": \"Organism\",
    \"is_abstract\": true
  }"
```

**Expected Response:** 201 Created with concept_id

---

## Step 7: Verify Data in Database (3 minutes)

### Copy-Paste Command 18: Check Domain Data

```bash
psql -d ontology_db -c "SELECT domain_id, domain_code, domain_name FROM cml_domain;"
```

### Copy-Paste Command 19: Check Version Data

```bash
psql -d ontology_db -c "SELECT ontology_version_id, version_code, status FROM cml_ontology_version;"
```

### Copy-Paste Command 20: Check Concept Data

```bash
psql -d ontology_db -c "SELECT concept_id, concept_code, concept_name FROM cml_concept;"
```

### Copy-Paste Command 21: Check All Table Row Counts

```bash
psql -d ontology_db << 'EOF'
SELECT 
  'cml_domain' as table_name, 
  COUNT(*) as row_count 
FROM cml_domain
UNION ALL
SELECT 'cml_ontology_version', COUNT(*) FROM cml_ontology_version
UNION ALL
SELECT 'cml_concept', COUNT(*) FROM cml_concept
UNION ALL
SELECT 'cml_property', COUNT(*) FROM cml_property
UNION ALL
SELECT 'cml_vocab', COUNT(*) FROM cml_vocab
ORDER BY table_name;
EOF
```

---

## Full Setup Script (Copy-Paste All At Once)

If you want to run everything in one go, copy-paste this entire script:

```bash
#!/bin/bash
set -e

echo "=== Step 1: Create Database ==="
createdb ontology_db 2>/dev/null || echo "Database already exists"
psql -d ontology_db -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"

echo "=== Step 2: Load SQL Schema ==="
psql -d ontology_db -f SQL_CREATE_TABLES.sql

echo "=== Step 3: Verify Tables ==="
psql -d ontology_db -c "\dt cml_*"

echo "=== Step 4: Create .env File ==="
cat > .env << 'ENVEOF'
DATABASE_URL=postgresql://postgres:password@localhost:5432/ontology_db
DEBUG=True
ALLOWED_ORIGINS=["http://localhost:3000","http://localhost:8000"]
ENVEOF

echo "=== Step 5: Setup Python Environment ==="
source env/bin/activate
pip install -r requirements.txt --quiet

echo "=== Step 6: Run Tests ==="
pytest tests/ -v --tb=short

echo "=== Setup Complete ==="
echo ""
echo "To start the API server, run:"
echo "  uvicorn app.main:app --reload --host 0.0.0.0 --port 8000"
echo ""
echo "Then open: http://localhost:8000/docs"
```

Save as `setup.sh` and run:
```bash
bash setup.sh
```

---

## Direct SQL Copy-Paste (Alternative to File)

If you can't use the SQL file, copy-paste this into `psql`:

```sql
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create cml_domain table
CREATE TABLE IF NOT EXISTS cml_domain (
    domain_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_code VARCHAR(100) NOT NULL UNIQUE,
    domain_name VARCHAR(255) NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Create cml_ontology_version table
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

-- Create cml_concept table
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
    FOREIGN KEY (parent_concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    CONSTRAINT uq_concept UNIQUE(ontology_version_id, concept_code)
);

-- Create cml_property table
CREATE TABLE IF NOT EXISTS cml_property (
    property_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    concept_id UUID NOT NULL,
    property_code VARCHAR(100) NOT NULL,
    property_name VARCHAR(255) NOT NULL,
    description TEXT,
    data_type VARCHAR(50) NOT NULL,
    is_required BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    CONSTRAINT uq_property UNIQUE(concept_id, property_code)
);

-- Create cml_property_constraint table
CREATE TABLE IF NOT EXISTS cml_property_constraint (
    constraint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID NOT NULL,
    constraint_type VARCHAR(50) NOT NULL,
    constraint_value JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (property_id) REFERENCES cml_property(property_id) ON DELETE CASCADE
);

-- Create cml_vocab table
CREATE TABLE IF NOT EXISTS cml_vocab (
    vocab_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vocab_code VARCHAR(100) NOT NULL UNIQUE,
    vocab_name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Create cml_vocab_term table
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
    FOREIGN KEY (parent_term_id) REFERENCES cml_vocab_term(term_id) ON DELETE CASCADE,
    CONSTRAINT uq_vocab_term UNIQUE(vocab_id, term_code)
);

-- Create cml_dataset_field_mapping table
CREATE TABLE IF NOT EXISTS cml_dataset_field_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    concept_id UUID NOT NULL,
    property_id UUID NOT NULL,
    external_field_name VARCHAR(255) NOT NULL,
    transform_rule JSONB,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    FOREIGN KEY (property_id) REFERENCES cml_property(property_id) ON DELETE CASCADE
);

-- Create cml_dataset_concept table
CREATE TABLE IF NOT EXISTS cml_dataset_concept (
    dataset_concept_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    concept_id UUID NOT NULL,
    external_dataset_name VARCHAR(255) NOT NULL,
    external_dataset_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (concept_id) REFERENCES cml_concept(concept_id) ON DELETE CASCADE,
    CONSTRAINT uq_dataset_concept UNIQUE(concept_id, external_dataset_name)
);

-- Create cml_ontology_change_log table
CREATE TABLE IF NOT EXISTS cml_ontology_change_log (
    change_log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ontology_version_id UUID NOT NULL,
    change_type VARCHAR(50) NOT NULL,
    changed_entity_type VARCHAR(50) NOT NULL,
    changed_entity_id UUID NOT NULL,
    changed_by VARCHAR(255),
    change_details JSONB,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ontology_version_id) REFERENCES cml_ontology_version(ontology_version_id) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX ix_domain_code ON cml_domain(domain_code);
CREATE INDEX ix_domain_is_active ON cml_domain(is_active);
CREATE INDEX ix_ontology_version_code ON cml_ontology_version(version_code);
CREATE INDEX ix_ontology_status ON cml_ontology_version(status);
CREATE INDEX ix_ontology_domain_id ON cml_ontology_version(domain_id);
CREATE INDEX ix_concept_code ON cml_concept(concept_code);
CREATE INDEX ix_concept_ontology_version_id ON cml_concept(ontology_version_id);
CREATE INDEX ix_concept_parent_id ON cml_concept(parent_concept_id);
CREATE INDEX ix_concept_is_abstract ON cml_concept(is_abstract);
CREATE INDEX ix_property_code ON cml_property(property_code);
CREATE INDEX ix_property_concept_id ON cml_property(concept_id);
CREATE INDEX ix_property_data_type ON cml_property(data_type);
CREATE INDEX ix_property_is_required ON cml_property(is_required);
CREATE INDEX ix_constraint_property_id ON cml_property_constraint(property_id);
CREATE INDEX ix_constraint_type ON cml_property_constraint(constraint_type);
CREATE INDEX ix_vocab_code ON cml_vocab(vocab_code);
CREATE INDEX ix_vocab_term_vocab_id ON cml_vocab_term(vocab_id);
CREATE INDEX ix_vocab_term_code ON cml_vocab_term(term_code);
CREATE INDEX ix_vocab_term_parent_id ON cml_vocab_term(parent_term_id);
CREATE INDEX ix_mapping_concept_id ON cml_dataset_field_mapping(concept_id);
CREATE INDEX ix_mapping_property_id ON cml_dataset_field_mapping(property_id);
CREATE INDEX ix_mapping_external_field ON cml_dataset_field_mapping(external_field_name);
CREATE INDEX ix_dataset_concept_id ON cml_dataset_concept(concept_id);
CREATE INDEX ix_dataset_external_name ON cml_dataset_concept(external_dataset_name);
CREATE INDEX ix_changelog_ontology_version_id ON cml_ontology_change_log(ontology_version_id);
CREATE INDEX ix_changelog_changed_at ON cml_ontology_change_log(changed_at);
CREATE INDEX ix_changelog_entity ON cml_ontology_change_log(changed_entity_type, changed_entity_id);
```

---

## Troubleshooting Copy-Paste

### Issue: "database ontology_db already exists"
Just continue with the next steps, database is already ready.

### Issue: "relation cml_domain already exists"
Run:
```bash
# Drop all tables
psql -d ontology_db << 'EOF'
DROP TABLE IF EXISTS cml_ontology_change_log;
DROP TABLE IF EXISTS cml_dataset_concept;
DROP TABLE IF EXISTS cml_dataset_field_mapping;
DROP TABLE IF EXISTS cml_property_constraint;
DROP TABLE IF EXISTS cml_property;
DROP TABLE IF EXISTS cml_concept;
DROP TABLE IF EXISTS cml_vocab_term;
DROP TABLE IF EXISTS cml_vocab;
DROP TABLE IF EXISTS cml_ontology_version;
DROP TABLE IF EXISTS cml_domain;
EOF

# Then re-run: psql -d ontology_db -f SQL_CREATE_TABLES.sql
```

### Issue: "role postgres does not exist"
Update .env with your PostgreSQL username:
```bash
# Find your PostgreSQL user
psql -U your_username -d postgres -c "SELECT current_user;"

# Update DATABASE_URL
DATABASE_URL=postgresql://your_username:password@localhost:5432/ontology_db
```

### Issue: "password authentication failed"
Update .env with correct password:
```bash
# If no password
DATABASE_URL=postgresql://postgres@localhost:5432/ontology_db

# If has password
DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/ontology_db
```

---

## Complete Workflow Summary

```
1. createdb ontology_db                  [1 min]
2. psql -d ontology_db -f SQL...         [1 min]
3. Create .env file                      [1 min]
4. source env/bin/activate               [1 min]
5. pip install -r requirements.txt       [1 min]
6. pytest tests/ -v                      [5 min]
7. uvicorn app.main:app --reload        [2 min]
8. curl -X POST http://localhost:8000... [2 min]
9. psql -d ontology_db -c "SELECT..."   [1 min]

TOTAL TIME: ~15-20 minutes 🚀
```

---

**Ready to start? Copy the commands above one section at a time!**
