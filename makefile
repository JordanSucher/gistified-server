.PHONY: help test unit-tests integration-tests lint test-dags

AIRFLOW_CONTAINER=airflow_webserver

help:
	@echo "Available commands:"
	@echo "  make test                 Run all tests"
	@echo "  make unit-tests           Run unit tests only"
	@echo "  make integration-tests    Run integration tests only"
	@echo "  make lint                 Run linting"
	@echo "  make test-dags            Test DAG files for errors"

test:
	docker exec $(AIRFLOW_CONTAINER) python -m pytest /opt/airflow/tests

unit-tests:
	docker exec $(AIRFLOW_CONTAINER) python -m pytest /opt/airflow/tests/unit -v

test-single:
	docker exec $(AIRFLOW_CONTAINER) python -m pytest $(TEST_PATH) -v

integration-tests:
	docker exec -e INTEGRATION_TESTS=1 $(AIRFLOW_CONTAINER) python -m pytest /opt/airflow/tests/integration -v

lint:
	docker exec $(AIRFLOW_CONTAINER) pip install flake8
	docker exec $(AIRFLOW_CONTAINER) flake8 /opt/airflow/dags

test-dags:
	docker exec $(AIRFLOW_CONTAINER) python -c "from airflow.models import DagBag; dag_bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False); print(f'DAG import errors: {dag_bag.import_errors}'); exit(1 if dag_bag.import_errors else 0)"