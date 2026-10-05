.PHONY: check unit integration start demo stop

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
	uv run lint-imports
	TEST=1 uv run pytest tests/unit

unit:
	TEST=1 uv run pytest tests/unit

integration:
	docker compose -p nebus-tests -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests

start:
	docker compose build
	docker compose up -d --wait

demo:
	docker compose --profile demo up -d webhook
	docker compose exec api python -m scripts.smoke --api http://api:8000 --receiver http://webhook:8081

stop:
	docker compose --profile demo down
