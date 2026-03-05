.PHONY: dev backend backend-prod frontend refresh refresh-light refresh-cold setup cuse4-bootstrap cuse4-estu prune-history prune-history-dry clean-local

setup:
	cd backend && python3 -m pip install -e ".[dev]"
	cd frontend && npm install

backend:
	uvicorn backend.main:app --reload --port 8000

backend-prod:
	uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers $${BACKEND_WORKERS:-1}

frontend:
	cd frontend && npm run dev -- --port 3000

dev:
	@echo "Starting backend and frontend..."
	$(MAKE) backend & $(MAKE) frontend & wait

refresh:
	@if [ -n "$$REFRESH_API_TOKEN" ]; then \
		curl -X POST -H "X-Refresh-Token: $$REFRESH_API_TOKEN" "http://localhost:8000/api/refresh"; \
	else \
		curl -X POST "http://localhost:8000/api/refresh"; \
	fi

refresh-light:
	@if [ -n "$$REFRESH_API_TOKEN" ]; then \
		curl -X POST -H "X-Refresh-Token: $$REFRESH_API_TOKEN" "http://localhost:8000/api/refresh?mode=light"; \
	else \
		curl -X POST "http://localhost:8000/api/refresh?mode=light"; \
	fi

refresh-cold:
	@if [ -n "$$REFRESH_API_TOKEN" ]; then \
		curl -X POST -H "X-Refresh-Token: $$REFRESH_API_TOKEN" "http://localhost:8000/api/refresh?mode=cold"; \
	else \
		curl -X POST "http://localhost:8000/api/refresh?mode=cold"; \
	fi

cuse4-bootstrap:
	python3 -m backend.scripts.bootstrap_cuse4_source_tables --db-path backend/runtime/data.db

cuse4-estu:
	python3 -m backend.scripts.build_cuse4_estu_membership --db-path backend/runtime/data.db

prune-history:
	python3 -m backend.scripts.prune_history_by_lookback --years $${YEARS:-5} --apply --vacuum

prune-history-dry:
	python3 -m backend.scripts.prune_history_by_lookback --years $${YEARS:-5} --dry-run

clean-local:
	find . -name ".DS_Store" -delete || true
	find . -type d -name "__pycache__" -prune -exec rm -rf {} + || true
	find . -type f -name "*.pyc" -delete || true
