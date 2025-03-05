import pytest
from unittest.mock import MagicMock, patch, mock_open, call
import os
import json
from airflow.models import DagBag
import sys
import importlib.util
from utils.dag_functions import fetch_podcasts_from_db, get_podcast_episodes_from_feed, flatten_episodes_from_nested_list, insert_episode_to_db, process_audio_for_episode, transcribe_audio_for_episode, generate_summary_for_episode, store_summary_for_episode

# Paths for storing files
AUDIO_DIR = "/opt/airflow/data/audio"
TRANSCRIPT_DIR = "/opt/airflow/data/transcripts"
SUMMARY_DIR = "/opt/airflow/data/summaries"


@pytest.mark.unit
class TestDagFunctions:
    def test_fetch_podcasts(self, mock_db_conn):
        """Test the fetch_podcasts function returns expected data."""
        # Setup mock response
        mock_db_conn['cursor'].fetchall.return_value = [
            (1, "Test Podcast", "http://example.com/rss"),
            (2, "Another Podcast", "http://example.com/feed")
        ]
        
        # Call the function
        result = fetch_podcasts_from_db()
        
        # Assert expected behavior
        assert mock_db_conn['cursor'].execute.called
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["title"] == "Test Podcast"
        assert result[0]["rssFeedUrl"] == "http://example.com/rss"
    
    def test_get_podcast_episodes(self, mock_requests):
        """Test the get_podcast_episodes function extracts episodes correctly."""
        # Setup mock response
        sampleFeed = """
            <rss>
                <channel>
                    <item>
                        <title>Episode 1</title>
                        <description>Description 1</description>
                        <published>Tue, 02 Jan 2023 12:00:00 +0000</published>
                        <enclosure url="http://example.com/episode1.mp3" type="audio/mpeg" />
                    </item>
                    <item>
                        <title>Episode 2</title>
                        <description>Description 2</description>
                        <published>Wed, 03 Jan 2023 12:00:00 +0000</published>
                        <enclosure url="http://example.com/episode2.mp3" type="audio/mpeg" />
                    </item>
                </channel>
            </rss>
            """
        
        mock_requests['response'].text = sampleFeed
        
        podcast = {"id": 1, "title": "Test Podcast", "rssFeedUrl": "http://example.com/rss"}
    
        # Call the function
        result = get_podcast_episodes_from_feed(podcast)
        
        # Assert expected behavior
        assert len(result) == 2
        assert result[0]["episode_title"] == "Episode 1"
        assert result[0]["publication_id"] == 1
        assert result[0]["episode_summary"] == "Description 1"
        assert result[0]["episode_pub_date"] == "Tue, 02 Jan 2023 12:00:00 +0000"
        assert result[1]["episode_url"] == "http://example.com/episode2.mp3"
            
    def test_insert_episode(self, mock_db_conn):
        """Test the insert_episode function handles new and existing episodes correctly."""
        # Setup for new episode
        mock_db_conn['cursor'].fetchone.side_effect = [
            (101, "http://example.com/new.mp3")  # First call returns new episode
        ]
        
        episode = {
            "publication_id": 1,
            "episode_title": "New Episode",
            "episode_summary": "Test summary",
            "episode_pub_date": "Wed, 03 Jan 2023 12:00:00 +0000",
            "episode_url": "http://example.com/new.mp3"
        }
        
        # Call the function
        result = insert_episode_to_db(episode)
        
        # Assert expected behavior for new episode
        assert result["status"] == "new"
        assert result["id"] == 101
        
        # Reset side effect for existing episode test
        mock_db_conn['cursor'].fetchone.side_effect = [
            None,  # First call returns no new episode (conflict)
            (102,)  # Second call returns existing ID
        ]
        
        # Call the function again for existing episode
        result = insert_episode_to_db(episode)
        
        # Assert expected behavior for existing episode
        assert result["status"] == "exists"
        assert result["id"] == 102

    def test_flatten_episodes_from_nested_list (self, mock_requests):
        """Test the flatten_episodes_from_nested_list function returns expected data."""
        # Setup mock response
        mock_json_data = [
            [
                {"episode_title": "Episode 1", "episode_url": "http://example.com/episode1.mp3"}, 
                {"episode_title": "Episode 2", "episode_url": "http://example.com/episode2.mp3"}
            ],
            [
                {"episode_title": "Episode 3", "episode_url": "http://example.com/episode3.mp3"}, 
                {"episode_title": "Episode 4", "episode_url": "http://example.com/episode4.mp3"}
            ]
        ]        
        
        mock_requests['response'].json.return_value = mock_json_data
        
        # Call the function
        result = flatten_episodes_from_nested_list(mock_requests['response'].json())
        
        # Assert expected behavior
        assert len(result) == 4
        assert result[0]["episode_title"] == "Episode 1"
        assert result[0]["episode_url"] == "http://example.com/episode1.mp3"
        assert result[1]["episode_title"] == "Episode 2"
        assert result[1]["episode_url"] == "http://example.com/episode2.mp3"

    def test_process_audio_skips_non_new_episodes (self):
        """Test the process_audio function skips processing for non-new episodes."""
        
        # Test cases where processing should be skipped
        test_cases = [
            None,  # None episode
            {"status": "processed", "url": "http://example.com/old.mp3"},  # Processed episode
            {"status": "error", "url": "http://example.com/error.mp3"}  # Error episode
        ]

        for episode in test_cases:
            result = process_audio_for_episode(episode)
            url = episode.get("url") if episode else None

            # Assert expected behavior
            assert result["status"] == "skipped"
            assert result["episode_url"] == url
            assert result["main_audio_path"] is None
            assert result["chunk_paths"] == []

    def test_process_audio_raises_error_for_missing_url(self):
        """Test the process_audio function raises an error for missing URL."""
        with pytest.raises(ValueError):
            process_audio_for_episode({"status": "new"})


    @patch('requests.get')
    @patch('pydub.AudioSegment.from_file')
    def test_process_audio_download_and_split_success (self, mock_audio_segment, mock_get, valid_episode, tmp_path):
        # Setup mocks for external dependencies we can't avoid
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Create a mock audio segment that has a length and can be sliced
        mock_audio = MagicMock()
        mock_audio.__len__.return_value = 30 * 60 * 1000  # 30 minutes
        mock_chunk = MagicMock()
        mock_audio.__getitem__.return_value = mock_chunk
        mock_audio_segment.return_value = mock_audio

        # Temporarily override AUDIO_DIR to use pytest's tmp_path
        with patch('utils.dag_functions.AUDIO_DIR', str(tmp_path)):
            # Execute function
            result = process_audio_for_episode(valid_episode)
        
        # Test the interface/outcomes:
        # 1. We should have received a dictionary with the expected keys
        assert set(result.keys()) >= {"episode_url", "main_audio_path", "chunk_paths"}
        
        # 2. The episode URL should match what we passed in
        assert result["episode_url"] == valid_episode["url"]
        
        # 3. We should have a main audio path
        assert result["main_audio_path"] is not None
        
        # 4. Since our mock audio was 30 minutes and chunks are 15 minutes, we should have 2 chunks
        assert len(result["chunk_paths"]) == 2
        
        # 5. Each chunk path should include the main path
        for chunk_path in result["chunk_paths"]:
            assert result["main_audio_path"] in chunk_path




        