.PHONY: dev backend frontend refresh setup cuse4-bootstrap cuse4-estu

setup:
	cd backend && pip install -e ".[dev]"
	cd frontend && npm install

backend:
	cd backend && uvicorn main:app --reload --port 8000

frontend:
	cd frontend && npm run dev -- --port 3000

dev:
	@echo "Starting backend and frontend..."
	$(MAKE) backend & $(MAKE) frontend & wait

refresh:
	curl -X POST http://localhost:8000/api/refresh

cuse4-bootstrap:
	python3 backend/scripts/bootstrap_cuse4_source_tables.py --db-path backend/data.db

cuse4-estu:
	python3 backend/scripts/build_cuse4_estu_membership.py --db-path backend/data.db
