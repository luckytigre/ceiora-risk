.PHONY: dev backend frontend refresh setup

setup:
	cd backend && pip install -e ".[dev]"
	cd frontend && npm install

backend:
	cd backend && uvicorn main:app --reload --port 8001

frontend:
	cd frontend && npm run dev -- --port 3002

dev:
	@echo "Starting backend and frontend..."
	$(MAKE) backend & $(MAKE) frontend & wait

refresh:
	curl -X POST http://localhost:8001/api/refresh
