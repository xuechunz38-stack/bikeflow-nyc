.PHONY: up down logs test lint train backfill

up:
	mkdir -p data artifacts
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

test:
	set -a; . ./.env; set +a; POSTGRES_HOST=localhost pytest -q

lint:
	ruff check .

train:
	docker compose exec airflow python -c "from bikeflow.model.train import train; train()"

backfill:
	docker compose exec airflow python scripts/backfill_status.py
