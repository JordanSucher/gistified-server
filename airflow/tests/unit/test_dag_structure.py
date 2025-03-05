import pytest
from airflow.models import DagBag
import os


@pytest.mark.unit
def test_dag_loads_with_no_errors(dagbag):
    """Test that all DAGs can be loaded without errors."""
    expected_dags = ['process_podcasts', 'process_new_podcast', 'send_daily_email_digest']
    actual_dags = dagbag.dag_ids
    
    # Check for import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    # Check that expected DAGs are present
    for dag_id in expected_dags:
        assert dag_id in actual_dags, f"{dag_id} not found in DAG bag"


@pytest.mark.unit
def test_process_podcasts_dag_structure(dagbag):
    """Test the structure of the process_podcasts DAG."""
    dag = dagbag.get_dag("process_podcasts")
    assert dag is not None
    
    # Check DAG structure
    assert dag.default_args.get('retries') == 1
    assert dag.tags == ['podcast']
    
    # Check for expected task pattern
    task_ids = [task.task_id for task in dag.tasks]
    
    # High-level check for key task components
    expected_patterns = ['fetch_podcasts', 'get_podcast_episodes', 'flatten_episodes', 
                         'insert_episode', 'process_audio', 'transcribe_audio', 
                         'generate_summary', 'store_summary']
    
    for pattern in expected_patterns:
        assert any(pattern in task_id for task_id in task_ids), f"No task with pattern '{pattern}' found"
