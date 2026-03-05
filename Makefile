.PHONY: dev backend frontend refresh setup cuse4-bootstrap cuse4-estu

setup:
	cd backend && python3 -m pip install -e ".[dev]"
	cd frontend && npm install

backend:
	uvicorn backend.main:app --reload --port 8000

frontend:
	cd frontend && npm run dev -- --port 3000

dev:
	@echo "Starting backend and frontend..."
	$(MAKE) backend & $(MAKE) frontend & wait

refresh:
	curl -X POST http://localhost:8000/api/refresh

cuse4-bootstrap:
	python3 -m backend.scripts.bootstrap_cuse4_source_tables --db-path backend/data.db

cuse4-estu:
	python3 -m backend.scripts.build_cuse4_estu_membership --db-path backend/data.db
