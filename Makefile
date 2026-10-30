.PHONY: install generate pipeline gold test lint all clean

export PYTHONPATH := src
export CLAIMS_WAREHOUSE := $(CURDIR)/data/warehouse.duckdb

install:
	pip install -r requirements.txt

generate:
	python -m claims_pipeline generate --batches 3

pipeline:
	python -m claims_pipeline run

gold:
	cd dbt && dbt build --profiles-dir .

test:
	pytest -q

lint:
	ruff check .

all: generate pipeline gold

clean:
	rm -rf data dbt/target dbt/logs
