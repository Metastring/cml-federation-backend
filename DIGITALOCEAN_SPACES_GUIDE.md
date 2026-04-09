# DigitalOcean Spaces API Integration Guide

## Overview

This document provides instructions for setting up and using the DigitalOcean Spaces integration APIs in the Central Server.

## Features

- ✅ Create folders in DigitalOcean Spaces
- ✅ Upload single files
- ✅ Upload multiple files at once
- ✅ List all visible folders
- ✅ List contents of specific folders
- ✅ Delete files
- ✅ Get file metadata
- ✅ Health check endpoint

## Environment Setup

### 1. Install Dependencies

```bash
# Install required packages
pip install -r requirements.txt

# Or with uv
uv pip install -r requirements.txt
```

Key dependencies:
- `boto3==1.28.85` - AWS SDK for Python (S3-compatible API)
- `pydantic-settings==2.0.3` - Settings management

### 2. Configure Environment Variables

Create a `.env` file in the project root with your DigitalOcean Spaces credentials:

```env
# DigitalOcean Spaces Configuration
DO_SPACES_ACCESS_KEY=DO00PQGBGYWALPAJWNCJ
DO_SPACES_SECRET_KEY=5QEAOYRObnuSmtC3G9S1WN70M8eot+gr0uTkj5vyXoI
DO_SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
DO_SPACES_REGION=nyc3
DO_SPACES_BUCKET_NAME=cmldataset
```

**⚠️ SECURITY WARNING**: Never commit credentials to version control. Use `.env` file locally only.

### 3. Start the Server

```bash
# Start FastAPI server
uvicorn app.main:app --reload

# The API will be available at http://localhost:8000
# Swagger UI: http://localhost:8000/docs
# ReDoc: http://localhost:8000/redoc
```

## API Endpoints

### 1. Create Folder

**POST** `/api/v1/spaces/folders/create`

Creates a new folder in DigitalOcean Spaces.

**Request:**
```json
{
  "folder_path": "datasets/project1"
}
```

**Response (201 Created):**
```json
{
  "status": "success",
  "folder_path": "datasets/project1/",
  "message": "Folder 'datasets/project1/' created successfully"
}
```

**Example with curl:**
```bash
curl -X POST http://localhost:8000/api/v1/spaces/folders/create \
  -H "Content-Type: application/json" \
  -d '{"folder_path": "datasets/project1"}'
```

### 2. Upload Single File

**POST** `/api/v1/spaces/files/upload`

Uploads a single file to Spaces.

**Request:**
- `file` (form-data, required): The file to upload
- `folder_path` (form-data, optional): Folder path where file should be stored

**Response (201 Created):**
```json
{
  "status": "success",
  "file_name": "document.pdf",
  "folder_path": "datasets/project1",
  "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/document.pdf",
  "file_size": 2048576,
  "message": "File 'document.pdf' uploaded successfully"
}
```

**Example with curl:**
```bash
curl -X POST http://localhost:8000/api/v1/spaces/files/upload \
  -F "file=@/path/to/document.pdf" \
  -F "folder_path=datasets/project1"
```

**Example with Python:**
```python
import requests

url = "http://localhost:8000/api/v1/spaces/files/upload"
files = {"file": open("document.pdf", "rb")}
data = {"folder_path": "datasets/project1"}

response = requests.post(url, files=files, data=data)
print(response.json())
```

### 3. Upload Multiple Files

**POST** `/api/v1/spaces/files/upload-multiple`

Uploads multiple files at once.

**Request:**
- `files` (form-data, required): Multiple files to upload
- `folder_path` (form-data, optional): Folder path where files should be stored

**Response (201 Created):**
```json
{
  "status": "success",
  "total_files": 3,
  "uploaded_count": 3,
  "folder_path": "datasets/project1",
  "files": [
    {
      "file_name": "file1.txt",
      "s3_key": "datasets/project1/file1.txt",
      "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/file1.txt",
      "bucket": "cmldataset"
    },
    {
      "file_name": "file2.csv",
      "s3_key": "datasets/project1/file2.csv",
      "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/file2.csv",
      "bucket": "cmldataset"
    },
    {
      "file_name": "file3.json",
      "s3_key": "datasets/project1/file3.json",
      "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/file3.json",
      "bucket": "cmldataset"
    }
  ],
  "message": "3 out of 3 files uploaded successfully"
}
```

**Example with curl:**
```bash
curl -X POST http://localhost:8000/api/v1/spaces/files/upload-multiple \
  -F "files=@file1.txt" \
  -F "files=@file2.csv" \
  -F "files=@file3.json" \
  -F "folder_path=datasets/project1"
```

**Example with Python:**
```python
import requests

url = "http://localhost:8000/api/v1/spaces/files/upload-multiple"
files = [
    ("files", open("file1.txt", "rb")),
    ("files", open("file2.csv", "rb")),
    ("files", open("file3.json", "rb")),
]
data = {"folder_path": "datasets/project1"}

response = requests.post(url, files=files, data=data)
print(response.json())
```

### 4. List All Folders

**GET** `/api/v1/spaces/folders/list`

Lists all visible folders in Spaces.

**Query Parameters:**
- `prefix` (optional): Filter folders by prefix

**Response (200 OK):**
```json
{
  "status": "success",
  "prefix": "root",
  "folder_count": 2,
  "folders": [
    "datasets/",
    "backups/"
  ],
  "message": "Found 2 folders"
}
```

**Example with curl:**
```bash
# List all folders
curl http://localhost:8000/api/v1/spaces/folders/list

# List folders with prefix
curl "http://localhost:8000/api/v1/spaces/folders/list?prefix=datasets"
```

**Example with Python:**
```python
import requests

url = "http://localhost:8000/api/v1/spaces/folders/list"
response = requests.get(url)
print(response.json())

# With prefix
url = "http://localhost:8000/api/v1/spaces/folders/list?prefix=datasets"
response = requests.get(url)
print(response.json())
```

### 5. List Folder Contents

**GET** `/api/v1/spaces/folders/{folder_path}/contents`

Lists all files and subfolders in a specific folder.

**Path Parameters:**
- `folder_path` (required): Path to the folder

**Response (200 OK):**
```json
{
  "status": "success",
  "folder_path": "datasets/project1",
  "file_count": 2,
  "subfolder_count": 1,
  "total_items": 3,
  "files": [
    {
      "name": "document.pdf",
      "key": "datasets/project1/document.pdf",
      "size": 2048576,
      "last_modified": "2024-01-20T10:30:00+00:00",
      "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/document.pdf"
    },
    {
      "name": "data.csv",
      "key": "datasets/project1/data.csv",
      "size": 1024,
      "last_modified": "2024-01-20T11:00:00+00:00",
      "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/data.csv"
    }
  ],
  "subfolders": [
    "datasets/project1/reports/"
  ],
  "message": "Folder contains 3 items"
}
```

**Example with curl:**
```bash
curl http://localhost:8000/api/v1/spaces/folders/datasets%2Fproject1/contents
```

**Example with Python:**
```python
import requests

folder_path = "datasets/project1"
url = f"http://localhost:8000/api/v1/spaces/folders/{folder_path}/contents"
response = requests.get(url)
print(response.json())
```

### 6. Delete File

**DELETE** `/api/v1/spaces/files/delete`

Deletes a file from Spaces.

**Request:**
```json
{
  "file_key": "datasets/project1/document.pdf"
}
```

**Response (200 OK):**
```json
{
  "status": "success",
  "file_key": "datasets/project1/document.pdf",
  "message": "File 'datasets/project1/document.pdf' deleted successfully"
}
```

**Example with curl:**
```bash
curl -X DELETE http://localhost:8000/api/v1/spaces/files/delete \
  -H "Content-Type: application/json" \
  -d '{"file_key": "datasets/project1/document.pdf"}'
```

**Example with Python:**
```python
import requests

url = "http://localhost:8000/api/v1/spaces/files/delete"
data = {"file_key": "datasets/project1/document.pdf"}
response = requests.delete(url, json=data)
print(response.json())
```

### 7. Get File Information

**GET** `/api/v1/spaces/files/info`

Retrieves metadata about a file.

**Query Parameters:**
- `file_key` (required): Full S3 key of the file

**Response (200 OK):**
```json
{
  "status": "success",
  "file_info": {
    "file_key": "datasets/project1/document.pdf",
    "size": 2048576,
    "last_modified": "2024-01-20T10:30:00+00:00",
    "content_type": "application/pdf",
    "url": "https://cmldataset.nyc3.digitaloceanspaces.com/datasets/project1/document.pdf"
  }
}
```

**Example with curl:**
```bash
curl "http://localhost:8000/api/v1/spaces/files/info?file_key=datasets%2Fproject1%2Fdocument.pdf"
```

**Example with Python:**
```python
import requests

url = "http://localhost:8000/api/v1/spaces/files/info"
params = {"file_key": "datasets/project1/document.pdf"}
response = requests.get(url, params=params)
print(response.json())
```

### 8. Health Check

**GET** `/api/v1/spaces/health`

Verifies that the DigitalOcean Spaces service is accessible.

**Response (200 OK):**
```json
{
  "status": "healthy",
  "message": "DigitalOcean Spaces connection is active"
}
```

**Example with curl:**
```bash
curl http://localhost:8000/api/v1/spaces/health
```

## Architecture

### Layered Architecture

```
┌─────────────────────────────────────────┐
│         API Controller                   │
│  (spaces_controller.py)                 │
│  - Request validation                   │
│  - HTTP handling                        │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│         Service Layer                    │
│  (spaces_service.py)                    │
│  - Business logic                       │
│  - Validation                           │
│  - Orchestration                        │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│      Repository Layer                    │
│  (spaces_repository.py)                 │
│  - S3 operations                        │
│  - Error handling                       │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│      DigitalOcean Spaces                │
│  (S3-compatible API)                    │
└─────────────────────────────────────────┘
```

### Component Responsibilities

1. **Controller** (`spaces_controller.py`)
   - Handles HTTP requests and responses
   - Validates request parameters
   - Maps HTTP status codes

2. **Service** (`spaces_service.py`)
   - Implements business logic
   - Validates domain rules
   - Orchestrates repository calls

3. **Repository** (`spaces_repository.py`)
   - Interacts directly with S3/Spaces
   - Handles boto3 client operations
   - Error handling and retries

4. **Schema** (`spaces_schema.py`)
   - Pydantic models for request/response validation
   - Type hints and documentation

## Error Handling

### Common Error Codes

| Status Code | Error | Cause |
|------------|-------|-------|
| 400 | Bad Request | Invalid folder path, file name, or parameters |
| 401 | Unauthorized | Invalid or missing Spaces credentials |
| 403 | Forbidden | Access denied to Spaces bucket |
| 404 | Not Found | File or folder not found |
| 500 | Internal Server Error | Unexpected server error |
| 503 | Service Unavailable | Spaces service not accessible |

### Error Response Format

```json
{
  "status": "error",
  "error": "Failed to upload file",
  "details": "File size exceeds maximum limit"
}
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_spaces_service.py

# Run with coverage
pytest --cov=app --cov-report=html

# Run integration tests
pytest tests/integration/
```

### Test Coverage

Current test coverage includes:
- Repository layer: File/folder operations, error handling
- Service layer: Business logic, validation rules
- Validation: Input sanitization, path traversal prevention

## Security Considerations

1. **Path Traversal Prevention**: All folder paths are validated to prevent `..` sequences
2. **Input Validation**: All user inputs are validated before processing
3. **Credentials**: Never store credentials in code - use environment variables
4. **CORS**: Configure CORS properly before production deployment
5. **File Uploads**: Implement file size limits and type checking

## Troubleshooting

### Connection Issues

```python
# Test Spaces connection
from app.services.spaces_service import SpacesService

service = SpacesService()
try:
    result = service.list_folders()
    print("Connection successful:", result)
except Exception as e:
    print(f"Connection failed: {e}")
```

### Credentials Issues

```bash
# Verify environment variables
echo $DO_SPACES_ACCESS_KEY
echo $DO_SPACES_ENDPOINT

# Test credentials with boto3
python -c "
import boto3
client = boto3.client('s3',
    region_name='nyc3',
    endpoint_url='https://nyc3.digitaloceanspaces.com',
    aws_access_key_id='YOUR_KEY',
    aws_secret_access_key='YOUR_SECRET'
)
print(client.list_buckets())
"
```

### File Upload Issues

- Ensure file size doesn't exceed limits
- Verify folder exists before uploading
- Check file permissions (for local files)
- Verify Spaces quota not exceeded

## Performance Tips

1. **Batch Operations**: Use bulk upload for multiple files
2. **Caching**: Cache folder listings for frequently accessed paths
3. **Pagination**: Use prefix filtering to reduce API calls
4. **Cleanup**: Regularly delete unused files to manage storage

## Production Deployment

1. Update `.env` with production credentials
2. Disable debug mode: `DEBUG=false`
3. Configure CORS for production domain
4. Implement rate limiting
5. Set up monitoring and logging
6. Use environment-specific configuration
7. Implement authentication/authorization

## References

- [DigitalOcean Spaces Documentation](https://docs.digitalocean.com/products/spaces/)
- [Boto3 S3 API Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
