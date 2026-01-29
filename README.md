# Central Server Project Structure

This project follows a Clean Architecture and strict testing standards.

## Folder Structure

```
project/
├── app/
│   ├── main.py                # App entry point
│   ├── api/                   # Controllers (HTTP layer)
│   │   ├── __init__.py
│   │   └── species_controller.py
│   ├── schemas/               # Beans / DTOs (Pydantic models)
│   │   ├── __init__.py
│   │   └── species_schema.py
│   ├── services/              # Business logic
│   │   ├── __init__.py
│   │   └── species_service.py
│   ├── repositories/          # DAO / DB access
│   │   ├── __init__.py
│   │   └── species_repository.py
│   ├── models/                # DB models (SQLAlchemy)
│   │   ├── __init__.py
│   │   └── species_model.py
│   ├── core/                  # Cross-cutting concerns
│   │   ├── config.py
│   │   ├── database.py
│   │   └── exceptions.py
│   └── utils/                 # Helpers, constants
│       └── validators.py
├── tests/
│   ├── unit/
│   └── integration/
├── migrations/
├── .github/
│   └── copilot-instructions.md
├── AI.md
├── pyproject.toml
└── README.md
```

- All business logic is in `services/`, not in controllers.
- All DB access is via `repositories/` using SQLAlchemy Core.
- All models use UUID as primary key.
- All features require both unit and integration tests.
