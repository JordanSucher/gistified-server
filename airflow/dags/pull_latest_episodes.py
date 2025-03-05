from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
from typing import TypedDict, List, Optional
from utils.dag_functions import fetch_podcasts_from_db, get_podcast_episodes_from_feed, flatten_episodes_from_nested_list, insert_episode_to_db, process_audio_for_episode, transcribe_audio_for_episode, generate_summary_for_episode, store_summary_for_episode

# Type definitions for better type checking
class PodcastFeed(TypedDict):
    id: int
    title: str
    rssFeedUrl: str

class Episode(TypedDict):
    publication_id: int
    episode_title: str
    episode_summary: str
    episode_pub_date: str
    episode_url: str

class InsertedEpisode(TypedDict):
    id: Optional[int]
    url: str
    status: Optional[str]

class AudioProcessingResult(TypedDict):
    episode_url: Optional[str]
    main_audio_path: Optional[str]
    chunk_paths: List[str]
    status: str

class TranscriptionResult(TypedDict):
    episode_url: Optional[str]
    transcript_path: Optional[str]
    audio_paths: List[str]
    status: str

class SummaryResult(TypedDict):
    episode_url: Optional[str]
    summary_path: Optional[str]
    audio_paths: List[str]
    transcript_path: Optional[str]
    status: str



# Database connection settings
DB_CONN = {
    "dbname": "app_db",
    "user": "app_user",
    "password": os.getenv("APP_DB_PASSWORD"),
    "host": "postgres",
    "port": "5432",
}

# Paths for storing files
AUDIO_DIR = "/opt/airflow/data/audio"
TRANSCRIPT_DIR = "/opt/airflow/data/transcripts"
SUMMARY_DIR = "/opt/airflow/data/summaries"

# Ensure directories exist
for directory in [AUDIO_DIR, TRANSCRIPT_DIR, SUMMARY_DIR]:
    os.makedirs(directory, exist_ok=True)

@dag(
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 2, 22),
    catchup=False,
    tags=['podcast'],
    max_active_runs=1,
    max_active_tasks=3,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=2)
    }
)
def process_podcasts():
    """
    DAG for fetching and processing podcast episodes in parallel.
    """
    
    @task
    def fetch_podcasts() -> List[PodcastFeed]:
        """Fetch podcast feeds from the database"""
        return fetch_podcasts_from_db()

    @task
    def get_podcast_episodes(podcast: PodcastFeed) -> List[Episode]:
        """Fetch recent episodes for a single podcast"""
        return get_podcast_episodes_from_feed(podcast)
    
    @task
    def flatten_episodes(nested_episodes: List[List[Episode]]) -> List[Episode]:
        """Flatten list of episode lists into single list of episodes"""
        return flatten_episodes_from_nested_list(nested_episodes)

    @task
    def insert_episode(episode: Episode) -> InsertedEpisode:
        """Insert single episode and return info if new"""
        return insert_episode_to_db(episode)

    @task
    def process_audio(episode: InsertedEpisode) -> AudioProcessingResult:
        """Download and split audio for single episode"""
        return process_audio_for_episode(episode)

    @task
    def transcribe_audio(audio_info: AudioProcessingResult) -> TranscriptionResult:
        """Generate transcript for single episode"""
        return transcribe_audio_for_episode(audio_info)

    @task
    def generate_summary(transcript_info: TranscriptionResult) -> SummaryResult:
        """Generate summary for single episode"""
        return generate_summary_for_episode(transcript_info)

    @task
    def store_summary(summary_info: SummaryResult) -> None:
        """Store summary and clean up files"""
        store_summary_for_episode(summary_info)

    # Define the task flow
    podcasts = fetch_podcasts()
    episodes = get_podcast_episodes.expand(podcast=podcasts)

    # Flatten the nested list of episodes
    flattened = flatten_episodes(episodes)
    new_episodes = insert_episode.expand(episode=flattened)

    # Process audio and generate summary
    audio_files = process_audio.expand(episode=new_episodes)
    transcripts = transcribe_audio.expand(audio_info=audio_files)
    summaries = generate_summary.expand(transcript_info=transcripts)
    store_summary.expand(summary_info=summaries)

# Create the DAG
dag = process_podcasts()
