import os
import pytest
from unittest.mock import MagicMock, patch
from airflow.models import DagBag


@pytest.fixture
def dagbag():
    """Fixture to get a DAG bag for testing."""
    return DagBag(dag_folder=os.path.join(os.path.dirname(__file__), '../dags'), include_examples=False)


@pytest.fixture
def mock_db_conn():
    """Fixture to mock database connection."""
    with patch('psycopg2.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        yield {
            'connect': mock_connect,
            'conn': mock_conn,
            'cursor': mock_cursor
        }
        
@pytest.fixture
def valid_episode():
    return {
        "url": "http://example.com/podcast.mp3",
        "status": "new"
    }

@pytest.fixture
def mock_openai():
    """Fixture to mock OpenAI client."""
    with patch('openai.OpenAI') as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_requests():
    """Fixture to mock requests library."""
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_get.return_value = mock_response
        mock_response.raise_for_status = MagicMock()
        yield {
            'get': mock_get,
            'response': mock_response
        }
