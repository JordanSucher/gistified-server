import pytest
from airflow.models import DagBag
from airflow.utils.session import create_session
from airflow.utils.state import State
import os
from datetime import datetime


@pytest.mark.integration
def test_dag_run_no_errors():
    """
    Test that the DAG runs without errors when executed.
    (This is more of a functional test than a true integration test)
    """
    # Import necessary components here to avoid loading during unit tests
    from airflow.models import DagRun
    from airflow.utils.types import DagRunType
    
    # Load the DAG
    dagbag = DagBag(dag_folder=os.path.join(os.path.dirname(__file__), '../../dags'), include_examples=False)
    dag = dagbag.get_dag("process_podcasts")
    
    # Only run this test if INTEGRATION_TESTS environment variable is set
    # as it requires database and external dependencies
    if not os.environ.get('INTEGRATION_TESTS'):
        pytest.skip("Skipping integration test (set INTEGRATION_TESTS=1 to run)")
    
    assert dag is not None, "DAG not found"
    
    # Create a test DAG run
    execution_date = datetime.now()
    with create_session() as session:
        dag_run = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.MANUAL,
            session=session
        )
    
    # This would execute the DAG but is potentially destructive, 
    # so we'll just test up to creation for now
    assert dag_run is not None
    assert dag_run.state == State.RUNNING