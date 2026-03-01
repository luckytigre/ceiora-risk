.PHONY: dev backend frontend refresh setup

setup:
	cd backend && pip install -e ".[dev]"
	cd frontend && npm install

backend:
	cd backend && uvicorn main:app --reload --port 8000

frontend:
	cd frontend && npm run dev

dev:
	@echo "Starting backend and frontend..."
	$(MAKE) backend & $(MAKE) frontend & wait

refresh:
	curl -X POST http://localhost:8000/api/refresh
