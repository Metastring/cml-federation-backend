.PHONY: help test test-unit test-integration test-cov test-cov-html lint format type-check install clean

help:
	@echo "Available commands:"
	@echo "  make test              - Run all tests"
	@echo "  make test-unit         - Run unit tests only (fast)"
	@echo "  make test-integration  - Run integration tests only (slow)"
	@echo "  make test-cov          - Run tests with coverage report"
	@echo "  make test-cov-html     - Generate HTML coverage report"
	@echo "  make lint              - Run linters (ruff)"
	@echo "  make format            - Format code with black"
	@echo "  make type-check        - Run mypy type checker"
	@echo "  make install-dev       - Install dev dependencies"
	@echo "  make clean             - Clean cache and build files"

install-dev:
	pip install -e ".[dev]"

test:
	pytest

test-unit:
	pytest -m "unit" -v

test-integration:
	pytest -m "integration" -v

test-cov:
	pytest --cov=app --cov-report=term-missing

test-cov-html:
	pytest --cov=app --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in coverage_html_report/index.html"

lint:
	ruff check app tests
	@echo "Linting passed ✓"

format:
	black app tests
	ruff check --fix app tests
	@echo "Code formatted ✓"

type-check:
	mypy app

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name coverage_html_report -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name .coverage -delete
	@echo "Cleaned cache and build files ✓"

# CI/CD Command (run before committing)
verify:
	@echo "Running verification suite..."
	make lint
	make type-check
	make test
	@echo "All verifications passed ✓"
