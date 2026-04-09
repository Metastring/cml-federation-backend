# Ontology API Documentation

## Base URL
```
http://localhost:8000/api/v1/ontology
```

## Authentication
All endpoints are currently unsecured. In production, add Bearer token authentication.

---

## Domains

### Create Domain
**POST** `/domains`

Creates a new ontology domain.

**Request Body:**
```json
{
  "domain_code": "biodiversity",
  "domain_name": "Biodiversity Domain",
  "description": "Domain for biodiversity data"
}
```

**Response (201 Created):**
```json
{
  "domain_id": "550e8400-e29b-41d4-a716-446655440000",
  "domain_code": "biodiversity",
  "domain_name": "Biodiversity Domain",
  "description": "Domain for biodiversity data",
  "is_active": true,
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

### List Domains
**GET** `/domains`

Returns all domains.

**Response (200 OK):**
```json
[
  {
    "domain_id": "550e8400-e29b-41d4-a716-446655440000",
    "domain_code": "biodiversity",
    "domain_name": "Biodiversity Domain",
    "description": "Domain for biodiversity data",
    "is_active": true,
    "created_at": "2024-01-15T10:30:00Z"
  },
  {
    "domain_id": "550e8400-e29b-41d4-a716-446655440001",
    "domain_code": "geology",
    "domain_name": "Geology Domain",
    "description": "Domain for geological data",
    "is_active": true,
    "created_at": "2024-01-15T10:35:00Z"
  }
]
```

---

### Get Domain
**GET** `/domains/{domain_id}`

Retrieves a specific domain by ID.

**Parameters:**
- `domain_id` (UUID): Domain identifier

**Response (200 OK):**
```json
{
  "domain_id": "550e8400-e29b-41d4-a716-446655440000",
  "domain_code": "biodiversity",
  "domain_name": "Biodiversity Domain",
  "description": "Domain for biodiversity data",
  "is_active": true,
  "created_at": "2024-01-15T10:30:00Z"
}
```

**Response (404 Not Found):**
```json
{
  "detail": "Domain with id 550e8400-e29b-41d4-a716-446655440000 not found"
}
```

---

### Get Domain by Code
**GET** `/domains/code/{code}`

Retrieves a domain by its code.

**Parameters:**
- `code` (string): Domain code

**Response (200 OK):** [Same as Get Domain]

**Response (404 Not Found):**
```json
{
  "detail": "Domain with code 'biodiversity' not found"
}
```

---

### Update Domain
**PATCH** `/domains/{domain_id}`

Updates an existing domain.

**Request Body:**
```json
{
  "domain_code": "biodiversity",
  "domain_name": "Updated Biodiversity Domain",
  "description": "Updated description"
}
```

**Response (200 OK):** [Domain object with updates]

**Response (404 Not Found):**
```json
{
  "detail": "Domain with id 550e8400-e29b-41d4-a716-446655440000 not found"
}
```

---

### Delete Domain
**DELETE** `/domains/{domain_id}`

Deletes a domain and all related versions, concepts, and properties.

**Response (204 No Content)**

**Response (404 Not Found):**
```json
{
  "detail": "Domain with id 550e8400-e29b-41d4-a716-446655440000 not found"
}
```

---

## Ontology Versions

### Create Ontology Version
**POST** `/domains/{domain_id}/versions`

Creates a new version of an ontology for a domain.

**Request Body:**
```json
{
  "version_code": "v1.0",
  "status": "draft",
  "description": "Initial version"
}
```

**Response (201 Created):**
```json
{
  "ontology_version_id": "550e8400-e29b-41d4-a716-446655440010",
  "domain_id": "550e8400-e29b-41d4-a716-446655440000",
  "version_code": "v1.0",
  "status": "draft",
  "description": "Initial version",
  "created_at": "2024-01-15T10:40:00Z"
}
```

**Status Values:**
- `draft` - Version is in development
- `active` - Currently active version
- `deprecated` - No longer in use

---

### List Versions by Domain
**GET** `/domains/{domain_id}/versions`

Lists all versions for a domain.

**Response (200 OK):**
```json
[
  {
    "ontology_version_id": "550e8400-e29b-41d4-a716-446655440010",
    "domain_id": "550e8400-e29b-41d4-a716-446655440000",
    "version_code": "v1.0",
    "status": "active",
    "description": "Initial version",
    "created_at": "2024-01-15T10:40:00Z"
  },
  {
    "ontology_version_id": "550e8400-e29b-41d4-a716-446655440011",
    "domain_id": "550e8400-e29b-41d4-a716-446655440000",
    "version_code": "v2.0",
    "status": "draft",
    "description": "Next version",
    "created_at": "2024-01-15T10:50:00Z"
  }
]
```

---

### Get Active Version
**GET** `/domains/{domain_id}/versions/active`

Gets the currently active version for a domain.

**Response (200 OK):** [Version object]

**Response (404 Not Found):**
```json
{
  "detail": "No active version found for domain 550e8400-e29b-41d4-a716-446655440000"
}
```

---

### Get Version
**GET** `/versions/{version_id}`

Gets a specific version by ID.

**Response (200 OK):** [Version object]

---

## Concepts

### Create Concept
**POST** `/versions/{version_id}/concepts`

Creates a concept in a version.

**Request Body:**
```json
{
  "concept_code": "Species",
  "concept_name": "Species",
  "description": "A concept representing species",
  "parent_concept_id": null,
  "is_abstract": false
}
```

**Response (201 Created):**
```json
{
  "concept_id": "550e8400-e29b-41d4-a716-446655440020",
  "ontology_version_id": "550e8400-e29b-41d4-a716-446655440010",
  "concept_code": "Species",
  "concept_name": "Species",
  "description": "A concept representing species",
  "parent_concept_id": null,
  "is_abstract": false,
  "created_at": "2024-01-15T11:00:00Z"
}
```

---

### List Concepts by Version
**GET** `/versions/{version_id}/concepts`

Lists all concepts in a version.

**Response (200 OK):**
```json
[
  {
    "concept_id": "550e8400-e29b-41d4-a716-446655440020",
    "concept_code": "Species",
    "concept_name": "Species",
    "description": "A concept representing species",
    "parent_concept_id": null,
    "is_abstract": false,
    "created_at": "2024-01-15T11:00:00Z"
  }
]
```

---

### Get Concept
**GET** `/concepts/{concept_id}`

Gets a specific concept by ID.

**Response (200 OK):** [Concept object]

---

## Properties

### Create Property
**POST** `/concepts/{concept_id}/properties`

Adds a property to a concept.

**Request Body:**
```json
{
  "property_code": "scientificName",
  "property_name": "Scientific Name",
  "description": "The scientific name",
  "data_type": "string",
  "is_required": true,
  "is_multivalued": false
}
```

**Data Types:**
- `string` - Text values
- `number` - Numeric values
- `date` - ISO 8601 dates
- `geometry` - GeoJSON geometries
- `boolean` - True/false values

**Response (201 Created):**
```json
{
  "property_id": "550e8400-e29b-41d4-a716-446655440030",
  "concept_id": "550e8400-e29b-41d4-a716-446655440020",
  "property_code": "scientificName",
  "property_name": "Scientific Name",
  "description": "The scientific name",
  "data_type": "string",
  "is_required": true,
  "is_multivalued": false,
  "created_at": "2024-01-15T11:10:00Z"
}
```

---

### List Properties by Concept
**GET** `/concepts/{concept_id}/properties`

Lists all properties for a concept.

**Response (200 OK):** [Array of property objects]

---

### Get Property
**GET** `/properties/{property_id}`

Gets a specific property by ID.

**Response (200 OK):** [Property object]

---

## Vocabularies

### Create Vocabulary
**POST** `/vocabularies`

Creates a controlled value list.

**Request Body:**
```json
{
  "vocab_code": "conservation_status",
  "description": "Conservation status values"
}
```

**Response (201 Created):**
```json
{
  "vocab_id": "550e8400-e29b-41d4-a716-446655440040",
  "vocab_code": "conservation_status",
  "description": "Conservation status values",
  "created_at": "2024-01-15T11:20:00Z"
}
```

**Response (409 Conflict):**
```json
{
  "detail": "Vocabulary with code 'conservation_status' already exists"
}
```

---

### List Vocabularies
**GET** `/vocabularies`

Lists all vocabularies.

**Response (200 OK):** [Array of vocabulary objects]

---

### Get Vocabulary
**GET** `/vocabularies/{vocab_id}`

Gets a specific vocabulary.

**Response (200 OK):** [Vocabulary object]

---

### Get Vocabulary by Code
**GET** `/vocabularies/code/{code}`

Gets a vocabulary by its code.

**Response (200 OK):** [Vocabulary object]

---

## Vocabulary Terms

### Create Term
**POST** `/vocabularies/{vocab_id}/terms`

Adds a term to a vocabulary.

**Request Body:**
```json
{
  "term_code": "EN",
  "term_label": "Endangered",
  "description": "Endangered species",
  "parent_term_id": null
}
```

**Response (201 Created):**
```json
{
  "vocab_term_id": "550e8400-e29b-41d4-a716-446655440050",
  "vocab_id": "550e8400-e29b-41d4-a716-446655440040",
  "term_code": "EN",
  "term_label": "Endangered",
  "description": "Endangered species",
  "parent_term_id": null,
  "created_at": "2024-01-15T11:30:00Z"
}
```

---

### List Terms by Vocabulary
**GET** `/vocabularies/{vocab_id}/terms`

Lists all terms in a vocabulary.

**Response (200 OK):** [Array of term objects]

---

### Get Term
**GET** `/terms/{term_id}`

Gets a specific term.

**Response (200 OK):** [Term object]

---

## Error Responses

All errors follow this format:

**400 Bad Request:**
```json
{
  "detail": "Request validation failed"
}
```

**404 Not Found:**
```json
{
  "detail": "Resource not found"
}
```

**409 Conflict:**
```json
{
  "detail": "Resource already exists"
}
```

**500 Internal Server Error:**
```json
{
  "detail": "Internal server error"
}
```

---

## HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | OK - Request succeeded |
| 201 | Created - Resource created successfully |
| 204 | No Content - Deletion successful |
| 400 | Bad Request - Invalid input |
| 404 | Not Found - Resource doesn't exist |
| 409 | Conflict - Resource already exists |
| 500 | Internal Server Error |

---

## Usage Examples

### Create Complete Ontology Structure

```bash
# 1. Create domain
curl -X POST http://localhost:8000/api/v1/ontology/domains \
  -H "Content-Type: application/json" \
  -d '{
    "domain_code": "biodiversity",
    "domain_name": "Biodiversity Domain"
  }'

# Response contains domain_id, save it as DOMAIN_ID

# 2. Create version
curl -X POST http://localhost:8000/api/v1/ontology/domains/$DOMAIN_ID/versions \
  -H "Content-Type: application/json" \
  -d '{
    "version_code": "v1.0",
    "status": "draft"
  }'

# Response contains ontology_version_id, save it as VERSION_ID

# 3. Create concept
curl -X POST http://localhost:8000/api/v1/ontology/versions/$VERSION_ID/concepts \
  -H "Content-Type: application/json" \
  -d '{
    "concept_code": "Species",
    "concept_name": "Species",
    "is_abstract": false
  }'

# Response contains concept_id, save it as CONCEPT_ID

# 4. Create property
curl -X POST http://localhost:8000/api/v1/ontology/concepts/$CONCEPT_ID/properties \
  -H "Content-Type: application/json" \
  -d '{
    "property_code": "scientificName",
    "property_name": "Scientific Name",
    "data_type": "string",
    "is_required": true
  }'

# 5. Create vocabulary
curl -X POST http://localhost:8000/api/v1/ontology/vocabularies \
  -H "Content-Type: application/json" \
  -d '{
    "vocab_code": "conservation_status",
    "description": "Conservation status"
  }'

# Response contains vocab_id, save it as VOCAB_ID

# 6. Add vocabulary term
curl -X POST http://localhost:8000/api/v1/ontology/vocabularies/$VOCAB_ID/terms \
  -H "Content-Type: application/json" \
  -d '{
    "term_code": "EN",
    "term_label": "Endangered"
  }'

# 7. Query results
curl http://localhost:8000/api/v1/ontology/domains
curl http://localhost:8000/api/v1/ontology/domains/$DOMAIN_ID/versions
curl http://localhost:8000/api/v1/ontology/versions/$VERSION_ID/concepts
curl http://localhost:8000/api/v1/ontology/concepts/$CONCEPT_ID/properties
curl http://localhost:8000/api/v1/ontology/vocabularies
curl http://localhost:8000/api/v1/ontology/vocabularies/$VOCAB_ID/terms
```

---

## Rate Limiting

Not currently implemented. Add in production as needed.

## Pagination

Not currently implemented. Add for large result sets:
```
GET /api/v1/ontology/domains?skip=0&limit=20
```

## Filtering

Not currently implemented. Can be added per resource:
```
GET /api/v1/ontology/domains?code=biodiversity&is_active=true
```

---

**Last Updated**: 2024-01-15
**API Version**: 1.0
