.PHONY: format
format:
	uv run ruff format ./src

.PHONY: lint
lint:
	uv run ruff check --fix ./src

.PHONY: ruff
ruff: format lint

.PHONY: ty
ty:
	uv run ty check