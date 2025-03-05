import pytest
from unittest.mock import MagicMock, patch, mock_open
import os
import json
from airflow.models import DagBag
import sys
import importlib.util

# Import the DAG's module for testing functions directly
sys.path.append(os.path.join(os.path.dirname(__file__), '../../dags'))
spec = importlib.util.spec_from_file_location("pull_latest_episodes", 
                                              os.path.join(os.path.dirname(__file__), 
                                                          '../../dags/pull_latest_episodes.py'))
pull_latest_episodes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pull_latest_episodes)


@pytest.mark.unit
class TestPullLatestEpisodes:
    
    def test_fetch_podcasts(self, mock_db_conn):
        """Test the fetch_podcasts function returns expected data."""
        # Setup mock response
        mock_db_conn['cursor'].fetchall.return_value = [
            (1, "Test Podcast", "http://example.com/rss"),
            (2, "Another Podcast", "http://example.com/feed")
        ]
        
        # Call the function
        result = pull_latest_episodes.fetch_podcasts()
        
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
                        <summary>Summary 1</summary>
                        <published>Tue, 02 Jan 2023 12:00:00 +0000</published>
                        <enclosure url="http://example.com/episode1.mp3" type="audio/mpeg" />
                    </item>
                    <item>
                        <title>Episode 2</title>
                        <description>Description 2</description>
                        <summary>Summary 2</summary>
                        <published>Wed, 03 Jan 2023 12:00:00 +0000</published>
                        <enclosure url="http://example.com/episode2.mp3" type="audio/mpeg" />
                    </item>
                </channel>
            </rss>
            """
        
        mock_requests['response'].text = sampleFeed
        
        podcast = {"id": 1, "title": "Test Podcast", "rssFeedUrl": "http://example.com/rss"}
    
        # Call the function
        result = pull_latest_episodes.get_podcast_episodes(podcast)
        
        # Assert expected behavior
        assert len(result) == 2
        assert result[0]["episode_title"] == "Episode 1"
        assert result[0]["publication_id"] == 1
        assert result[0]["episode_summary"] == "Summary 1"
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
        result = pull_latest_episodes.insert_episode(episode)
        
        # Assert expected behavior for new episode
        assert result["status"] == "new"
        assert result["id"] == 101
        
        # Reset side effect for existing episode test
        mock_db_conn['cursor'].fetchone.side_effect = [
            None,  # First call returns no new episode (conflict)
            (102,)  # Second call returns existing ID
        ]
        
        # Call the function again for existing episode
        result = pull_latest_episodes.insert_episode(episode)
        
        # Assert expected behavior for existing episode
        assert result["status"] == "exists"
        assert result["id"] == 102
