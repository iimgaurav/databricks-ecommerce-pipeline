# ════════════════════════════════════════════════════════════════
# Makefile — Developer shortcuts
# ════════════════════════════════════════════════════════════════
# Usage:
#   make lint     — check code style
#   make test     — run all tests
#   make build    — build Python wheel
#   make deploy   — deploy to staging
#   make all      — lint + test + build (what CI does)
# ════════════════════════════════════════════════════════════════

.PHONY: lint test build deploy clean all

# Run linter
lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

# Fix linting issues automatically
lint-fix:
	ruff check src/ tests/ --fix
	ruff format src/ tests/

# Run all unit tests with coverage
test:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

# Build Python wheel
build:
	python -m build --wheel --outdir dist/

# Deploy to staging using DABs
deploy-staging:
	databricks bundle validate -t staging
	databricks bundle deploy -t staging

# Deploy to production using DABs
deploy-prod:
	databricks bundle validate -t prod
	databricks bundle deploy -t prod

# Clean build artifacts
clean:
	rm -rf dist/ build/ *.egg-info .pytest_cache .ruff_cache .coverage coverage.xml

# Full CI check (same as what GitHub Actions runs)
all: lint test build
	@echo "✅ All checks passed!"
