# Setup Checklist - Ontology System

## Phase 1: Database Setup ✓ READY

### PostgreSQL Database Setup
- [ ] PostgreSQL installed and running
- [ ] Create database: `createdb ontology_db`
- [ ] Connect to database: `psql -d ontology_db`
- [ ] Enable UUID extension: `CREATE EXTENSION "uuid-ossp";`
- [ ] Run SQL script: Copy contents of `SQL_CREATE_TABLES.sql`
- [ ] Verify tables created: `\dt cml_*`

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

## Phase 2: Python Environment ✓ READY

### Virtual Environment
- [ ] Python 3.8+ installed
- [ ] Virtual environment exists: `env/bin/activate`
- [ ] Activate: `source env/bin/activate`
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Verify installation: `pip list | grep -E "fastapi|sqlalchemy|pydantic"`

**Expected:**
- fastapi 0.117.1+
- sqlalchemy 2.0+
- pydantic 2.0+
- psycopg2 2.9+

---

## Phase 3: Configuration ✓ READY

### Environment Variables
- [ ] Create `.env` file in project root
- [ ] Add DATABASE_URL: `DATABASE_URL=postgresql://user:password@localhost:5432/ontology_db`
- [ ] Add DEBUG flag: `DEBUG=True`
- [ ] Add ALLOWED_ORIGINS: `ALLOWED_ORIGINS=["http://localhost:3000"]`

**Example .env:**
```
DATABASE_URL=postgresql://postgres:password@localhost:5432/ontology_db
DEBUG=True
ALLOWED_ORIGINS=["http://localhost:3000","http://localhost:8000"]
```

---

## Phase 4: Code Verification ✓ READY

### Key Files Exist
- [x] `app/models/ontology_model.py` (456 lines) - ✅ ORM models
- [x] `app/schemas/ontology_pydantic.py` (324 lines) - ✅ Pydantic models
- [x] `app/services/ontology_service.py` (499 lines) - ✅ Business logic
- [x] `app/repositories/ontology_repository.py` (634 lines) - ✅ Data access
- [x] `app/api/ontology_controller.py` (470 lines) - ✅ API endpoints
- [x] `SQL_CREATE_TABLES.sql` (237 lines) - ✅ Database setup

### Code Quality
- [ ] Run linting: `flake8 app/`
- [ ] Run type checking: `mypy app/`
- [ ] Run formatter: `black app/`

---

## Phase 5: Testing ✓ READY

### Unit Tests
- [ ] Test file exists: `tests/unit/test_ontology_repository.py`
- [ ] Run tests: `pytest tests/unit/test_ontology_repository.py -v`
- [ ] Expected: 31+ tests passing

**Command:**
```bash
pytest tests/unit/test_ontology_repository.py -v --tb=short
```

### Integration Tests
- [ ] Test file exists: `tests/integration/test_ontology.py`
- [ ] Run tests: `pytest tests/integration/test_ontology.py -v`
- [ ] Expected: 11+ tests passing

**Command:**
```bash
pytest tests/integration/test_ontology.py -v --tb=short
```

### All Tests
- [ ] Run all tests: `pytest tests/ -v --cov=app`
- [ ] Expected: 42+ tests passing
- [ ] Expected coverage: 80%+

**Command:**
```bash
pytest tests/ -v --cov=app --cov-report=term-missing
```

---

## Phase 6: Server Launch ✓ READY

### Start API Server
- [ ] Activate environment: `source env/bin/activate`
- [ ] Navigate to project: `cd /home/metastring/src/central_server`
- [ ] Start server: `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`
- [ ] Server running: Check `http://localhost:8000/docs`

**Expected Output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete
```

---

## Phase 7: API Testing ✓ READY

### Test Endpoints

#### 1. Create Domain
```bash
curl -X POST http://localhost:8000/api/v1/ontology/domains \
  -H "Content-Type: application/json" \
  -d '{
    "domain_code": "biodiversity",
    "domain_name": "Biodiversity Domain",
    "description": "Domain for biodiversity data"
  }'
```

**Expected Response:** 201 Created with domain_id

#### 2. List Domains
```bash
curl -X GET http://localhost:8000/api/v1/ontology/domains
```

**Expected Response:** 200 OK with list of domains

#### 3. Create Ontology Version
```bash
curl -X POST http://localhost:8000/api/v1/ontology/versions \
  -H "Content-Type: application/json" \
  -d '{
    "domain_id": "YOUR_DOMAIN_ID",
    "version_code": "v1.0.0",
    "status": "draft"
  }'
```

**Expected Response:** 201 Created with version_id

#### 4. Create Concept
```bash
curl -X POST http://localhost:8000/api/v1/ontology/concepts \
  -H "Content-Type: application/json" \
  -d '{
    "ontology_version_id": "YOUR_VERSION_ID",
    "concept_code": "Organism",
    "concept_name": "Organism",
    "is_abstract": true
  }'
```

**Expected Response:** 201 Created with concept_id

### Swagger UI
- [ ] Open browser: `http://localhost:8000/docs`
- [ ] See all endpoints
- [ ] Try endpoints with Swagger UI
- [ ] Check request/response schemas

---

## Phase 8: Database Verification ✓ READY

### Verify Data in PostgreSQL

#### Check Tables
```sql
-- Connect to database
psql -d ontology_db

-- List tables
\dt cml_*

-- Check row counts
SELECT 'cml_domain' as table_name, COUNT(*) as rows FROM cml_domain
UNION ALL
SELECT 'cml_ontology_version', COUNT(*) FROM cml_ontology_version
UNION ALL
SELECT 'cml_concept', COUNT(*) FROM cml_concept;
```

#### Check Inserted Data
```sql
-- View domains
SELECT domain_id, domain_code, domain_name FROM cml_domain;

-- View versions
SELECT ontology_version_id, version_code, status FROM cml_ontology_version;

-- View concepts
SELECT concept_id, concept_code, concept_name FROM cml_concept;
```

---

## Phase 9: Documentation ✓ READY

### Documentation Files
- [x] `QUICK_START.md` - Quick start guide
- [x] `ONTOLOGY_SETUP_GUIDE.md` - Detailed setup
- [x] `ONTOLOGY_IMPLEMENTATION.md` - Architecture
- [x] `ONTOLOGY_API.md` - API reference
- [x] `.github/copilot-instructions.md` - Code standards

---

## Quick Commands Reference

### Database
```bash
# Create database
createdb ontology_db

# Connect
psql -d ontology_db

# Run SQL file
psql -d ontology_db -f SQL_CREATE_TABLES.sql

# List tables
\dt cml_*
```

### Python
```bash
# Activate environment
source env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v

# Run linting
flake8 app/

# Format code
black app/
```

### Server
```bash
# Start server
uvicorn app.main:app --reload

# Start with specific port
uvicorn app.main:app --port 8001 --reload

# Production server
gunicorn app.main:app --workers 4 --bind 0.0.0.0:8000
```

### API Testing
```bash
# Create domain
curl -X POST http://localhost:8000/api/v1/ontology/domains \
  -H "Content-Type: application/json" \
  -d '{"domain_code":"test","domain_name":"Test"}'

# List domains
curl http://localhost:8000/api/v1/ontology/domains

# Get Swagger UI
open http://localhost:8000/docs
```

---

## Troubleshooting

### PostgreSQL Issues

**Error: database does not exist**
```bash
createdb ontology_db
```

**Error: UUID extension not found**
```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

**Error: Foreign key constraint error**
- Ensure parent records exist before inserting child records
- Check parent table: `SELECT * FROM parent_table WHERE id = 'YOUR_ID';`

### Python Issues

**Error: ModuleNotFoundError: No module named 'fastapi'**
```bash
pip install -r requirements.txt
```

**Error: sqlalchemy.exc.OperationalError**
- Check DATABASE_URL in .env
- Verify PostgreSQL is running
- Test connection: `psql -d ontology_db -c "SELECT 1;"`

### API Issues

**Error: 422 Unprocessable Entity**
- Check JSON payload format
- Use Swagger UI to validate: `http://localhost:8000/docs`
- Check field constraints (min/max length)

**Error: 404 Not Found**
- Check endpoint path
- Verify server is running
- Check Swagger UI for correct endpoints

---

## Success Criteria

### ✅ Phase Complete When:

**Phase 1: Database**
- [x] All 10 tables created in PostgreSQL
- [x] Foreign keys and indexes in place
- [x] Can query: `SELECT COUNT(*) FROM cml_domain;` returns 0

**Phase 2: Python**
- [ ] Virtual environment activated
- [ ] All dependencies installed
- [ ] Can import: `from app.models.ontology_model import Domain`

**Phase 3: Configuration**
- [ ] .env file created and configured
- [ ] DATABASE_URL points to correct database
- [ ] Can connect: `psql -c "SELECT 1;"`

**Phase 4: Code**
- [ ] All key files exist and contain correct code
- [ ] No syntax errors
- [ ] Linting passes: `flake8 app/` (0 errors)

**Phase 5: Testing**
- [ ] All unit tests pass: `pytest tests/unit/`
- [ ] All integration tests pass: `pytest tests/integration/`
- [ ] Coverage >= 80%

**Phase 6: Server**
- [ ] Server starts without errors
- [ ] Swagger UI accessible at `http://localhost:8000/docs`
- [ ] Can see all 23+ endpoints listed

**Phase 7: API**
- [ ] Can create domain via POST
- [ ] Can list domains via GET
- [ ] Can create version via POST
- [ ] Can create concept via POST

**Phase 8: Data**
- [ ] Created data persists in database
- [ ] Can query via SQL
- [ ] Relationships work (foreign keys)

---

## Estimated Time

| Phase | Task | Time |
|-------|------|------|
| 1 | Create tables | 5 min |
| 2 | Setup Python env | 5 min |
| 3 | Configuration | 5 min |
| 4 | Verify code | 5 min |
| 5 | Run tests | 10 min |
| 6 | Start server | 2 min |
| 7 | Test API | 10 min |
| 8 | Verify database | 5 min |
| **TOTAL** | **Complete Setup** | **~45 minutes** |

---

## Notes

- Keep .env file private (add to .gitignore)
- Run tests after any code changes
- Check API docs at `http://localhost:8000/docs`
- Use Swagger UI for interactive API testing
- Review copilot-instructions.md for code standards

---

**Status: Ready to begin! 🚀**

Start with Phase 1: Database Setup
