# DigitalOcean Spaces API - Quick Reference

## Setup Checklist

- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Create `.env` file with Spaces credentials
- [ ] Start server: `uvicorn app.main:app --reload`
- [ ] Access API docs: `http://localhost:8000/docs`

## Environment Variables

```env
DO_SPACES_ACCESS_KEY=DO00PQGBGYWALPAJWNCJ
DO_SPACES_SECRET_KEY=5QEAOYRObnuSmtC3G9S1WN70M8eot+gr0uTkj5vyXoI
DO_SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
DO_SPACES_REGION=nyc3
DO_SPACES_BUCKET_NAME=cmldataset
```

## Core Operations

### 1. Create Folder
```bash
curl -X POST http://localhost:8000/api/v1/spaces/folders/create \
  -H "Content-Type: application/json" \
  -d '{"folder_path": "datasets/project1"}'
```

### 2. Upload Single File
```bash
curl -X POST http://localhost:8000/api/v1/spaces/files/upload \
  -F "file=@document.pdf" \
  -F "folder_path=datasets/project1"
```

### 3. Upload Multiple Files
```bash
curl -X POST http://localhost:8000/api/v1/spaces/files/upload-multiple \
  -F "files=@file1.txt" \
  -F "files=@file2.csv" \
  -F "files=@file3.json" \
  -F "folder_path=datasets/project1"
```

### 4. List All Folders
```bash
curl http://localhost:8000/api/v1/spaces/folders/list
```

### 5. List Folder Contents
```bash
curl http://localhost:8000/api/v1/spaces/folders/datasets%2Fproject1/contents
```

### 6. Get File Info
```bash
curl "http://localhost:8000/api/v1/spaces/files/info?file_key=datasets%2Fproject1%2Fdocument.pdf"
```

### 7. Delete File
```bash
curl -X DELETE http://localhost:8000/api/v1/spaces/files/delete \
  -H "Content-Type: application/json" \
  -d '{"file_key": "datasets/project1/document.pdf"}'
```

### 8. Health Check
```bash
curl http://localhost:8000/api/v1/spaces/health
```

## Python Examples

### Upload File
```python
import requests

url = "http://localhost:8000/api/v1/spaces/files/upload"
with open("document.pdf", "rb") as f:
    files = {"file": f}
    data = {"folder_path": "datasets/project1"}
    response = requests.post(url, files=files, data=data)
    print(response.json())
```

### Upload Multiple Files
```python
import requests

url = "http://localhost:8000/api/v1/spaces/files/upload-multiple"
files = [
    ("files", open("file1.txt", "rb")),
    ("files", open("file2.csv", "rb")),
]
data = {"folder_path": "datasets/project1"}
response = requests.post(url, files=files, data=data)
print(response.json())
```

### List Folders
```python
import requests

url = "http://localhost:8000/api/v1/spaces/folders/list"
response = requests.get(url)
print(response.json())
```

### List Folder Contents
```python
import requests

folder_path = "datasets/project1"
url = f"http://localhost:8000/api/v1/spaces/folders/{folder_path}/contents"
response = requests.get(url)
for file in response.json()["files"]:
    print(f"Name: {file['name']}, Size: {file['size']} bytes")
```

## File Structure

```
app/
├── api/
│   └── spaces_controller.py       # API endpoints
├── repositories/
│   └── spaces_repository.py       # S3 operations
├── services/
│   └── spaces_service.py          # Business logic
├── schemas/
│   └── spaces_schema.py           # Pydantic models
└── core/
    └── config.py                  # Configuration (updated)

tests/
└── unit/
    └── test_spaces_service.py     # Unit tests
```

## Common Response Formats

### Success Response
```json
{
  "status": "success",
  "message": "Operation completed successfully",
  "data": {}
}
```

### Error Response
```json
{
  "status": "error",
  "error": "Error message",
  "details": "Additional details"
}
```

## Testing

```bash
# Run all tests
pytest

# Run specific test
pytest tests/unit/test_spaces_service.py::TestSpacesService::test_upload_file_success

# Run with coverage
pytest --cov=app
```

## Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| 401 Unauthorized | Check credentials in `.env` file |
| 403 Forbidden | Verify bucket name and permissions |
| Path traversal error | Don't use `..` in paths |
| File not found | Ensure file exists and folder path is correct |
| Connection timeout | Check Spaces endpoint URL and network |

## API Documentation

Interactive API documentation available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Next Steps

1. Test with provided curl examples
2. Integrate into your application
3. Configure authentication/authorization
4. Set up logging and monitoring
5. Deploy to production

For detailed documentation, see [DIGITALOCEAN_SPACES_GUIDE.md](DIGITALOCEAN_SPACES_GUIDE.md)
