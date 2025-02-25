from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import feedparser
import openai
import re
import logging
from pydub import AudioSegment
from typing import TypedDict, List, Optional, Dict, Union

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
    DAG for fetching and processing podcast episodes in parallel using TaskFlow API.
    """
    
    @task
    def fetch_podcasts() -> List[PodcastFeed]:
        """Fetch all podcast RSS feeds"""
        with psycopg2.connect(**DB_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, title, \"rssFeedUrl\" FROM \"Publication\"")
                publications = [
                    {"id": row[0], "title": row[1], "rssFeedUrl": row[2]} 
                    for row in cur.fetchall()
                ]
        
        if not publications:
            raise ValueError("No podcasts found in database")
        return publications

    @task
    def get_podcast_episodes(podcast: PodcastFeed) -> List[Episode]:
        """Fetch recent episodes for a single podcast"""
        try: 
            response = requests.get(podcast["rssFeedUrl"])
            response.raise_for_status()
            feed = feedparser.parse(response.text)

            if not feed.entries:
                logging.warning(f"No episodes found for {podcast['title']}")
                return []
                
            return [
                {
                    "publication_id": podcast["id"],
                    "episode_title": entry.title,
                    "episode_summary": entry.summary,
                    "episode_pub_date": entry.published,
                    "episode_url": entry.enclosures[0].href if entry.enclosures else None
                }
                for entry in feed.entries[:3] if entry.enclosures
            ]
        
        except requests.RequestException as e:
            logging.error(f"Error fetching podcast feed {podcast['rssFeedUrl']}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error parsing podcast feed: {str(e)}")
            raise

    
    @task
    def flatten_episodes(nested_episodes: List[List[Episode]]) -> List[Episode]:
        """Flatten list of episode lists into single list of episodes"""
        flattened = []
        for episode_list in nested_episodes:
            flattened.extend(episode_list)

        if not flattened:
            logging.warning("No episodes found")
        return flattened

    @task
    def insert_episode(episode: Episode) -> InsertedEpisode:
        """Insert single episode and return info if new"""
        
        if not episode.get("episode_url"):
            logging.warning(f"Skipping episode with no URL: {episode.get('episode_title', 'Unknown')}")
            return {"id": None, "url": "", "status": "skipped"}

        logging.info(f"Inserting episode: {episode}")

        try: 
            with psycopg2.connect(**DB_CONN) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO \"Episode\" (title, description, url, \"publicationId\", \"publishedAt\")
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO NOTHING
                        RETURNING id, url
                        """,
                        (
                            episode["episode_title"],
                            episode["episode_summary"],
                            episode["episode_url"],
                            episode["publication_id"],
                            episode["episode_pub_date"]
                        )
                    )
                    result = cur.fetchone()
                    conn.commit()
                    
                    if result:
                        return {"id": result[0], "url": result[1], "status": "new"}
                    
                    # Get the ID for existing episodes
                    cur.execute(
                        "SELECT id FROM \"Episode\" WHERE url = %s",
                        (episode["episode_url"],)
                    )
                    existing_id = cur.fetchone()
                    
                    return {
                        "id": existing_id[0] if existing_id else None, 
                        "url": episode["episode_url"], 
                        "status": "exists"
                    }
            
        except Exception as e:
            logging.error(f"Error inserting episode: {str(e)}")
            raise


    @task
    def process_audio(episode: InsertedEpisode) -> AudioProcessingResult:
        """Download and split audio for single episode"""
        if not episode or episode.get("status") != "new":
            logging.info(f"Skipping audio processing for existing episode: {episode.get('url')}")
            return {
                "episode_url": episode.get("url") if episode else None,
                "main_audio_path": None,
                "chunk_paths": [],
                "status": "skipped"
            }

        if not episode.get("url"):
            raise ValueError(f"Invalid episode data received: {episode}")


        try:
            episode_url = episode["url"]
            safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(episode_url))
            local_path = os.path.join(AUDIO_DIR, safe_filename)

            # Download
            response = requests.get(episode_url, stream=True)
            response.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Split into chunks
            try:
                audio = AudioSegment.from_file(local_path)
            except Exception as e:
                os.remove(local_path)
                raise ValueError(f"Error processing audio: {e}")
            
            chunk_size_ms = 15 * 60 * 1000
            chunks = []

            for i, start in enumerate(range(0, len(audio), chunk_size_ms)):
                chunk = audio[start:start + chunk_size_ms]
                chunk_path = f"{local_path}_chunk_{i}.mp3"
                chunk.export(chunk_path, format="mp3")
                chunks.append(chunk_path)

            return {
                "episode_url": episode_url,
                "main_audio_path": local_path,
                "chunk_paths": chunks
            }

        except requests.RequestException as e:
            logging.error(f"Failed to download audio: {str(e)}")
            raise ValueError(f"Failed to download audio: {str(e)}")
        
        except Exception as e:
            logging.error(f"Failed to process audio: {str(e)}")
            # Clean up any partial files
            if 'local_path' in locals() and os.path.exists(local_path):
                os.remove(local_path)
            for chunk_path in chunks:
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
            raise


    @task
    def transcribe_audio(audio_info: AudioProcessingResult) -> TranscriptionResult:
        """Generate transcript for single episode"""
        if not audio_info or audio_info.get("status") == "skipped":
            logging.info(f"Skipping transcription for: {audio_info.get('episode_url') if audio_info else 'unknown'}")

            return {
                "episode_url": audio_info.get("episode_url") if audio_info else None,
                "transcript_path": None,
                "audio_paths": [],
                "status": "skipped"
            }

        if not audio_info.get("chunk_paths"):
            raise ValueError(f"Invalid audio info received: {audio_info}")

        try: 
            logging.info(f"Transcribing audio for: {audio_info['episode_url']}")

            client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            transcripts = []

            for chunk_path in audio_info["chunk_paths"]:

                if not os.path.exists(chunk_path):
                    raise ValueError(f"Audio chunk not found: {chunk_path}")

                with open(chunk_path, "rb") as audio_file:
                    response = client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file,
                    )
                    transcripts.append(response.text)

            if not transcripts:
                raise ValueError(f"No transcripts generated for: {audio_info['episode_url']}")

            # Save combined transcript
            full_transcript = " ".join(transcripts)
            safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(audio_info["episode_url"]))
            transcript_path = os.path.join(TRANSCRIPT_DIR, f"{safe_filename}.txt")
            
            with open(transcript_path, "w") as f:
                f.write(full_transcript)

            return {
                "episode_url": audio_info["episode_url"],
                "transcript_path": transcript_path,
                "audio_paths": [audio_info["main_audio_path"]] + audio_info["chunk_paths"],
                "status": "processed"
            }
        
        except Exception as e:
            logging.error(f"Transcription failed: {str(e)}")
            # Clean up any partial files
            if 'transcript_path' in locals() and os.path.exists(transcript_path):
                os.remove(transcript_path)
            raise


    @task
    def generate_summary(transcript_info: TranscriptionResult) -> SummaryResult:
        """Generate summary for single episode"""
        if not transcript_info or transcript_info.get("status") == "skipped":
            logging.info(f"Skipping summary for: {transcript_info.get('episode_url') if transcript_info else 'unknown'}")
            return {
                "episode_url": transcript_info.get("episode_url") if transcript_info else None,
                "summary_path": None,
                "audio_paths": [],
                "transcript_path": None,
                "status": "skipped"
            }

        if not transcript_info.get("transcript_path") or not os.path.exists(transcript_info["transcript_path"]):
            raise ValueError(f"Invalid or missing transcript: {transcript_info}")

        try: 
            logging.info(f"Generating summary for: {transcript_info['episode_url']}")
            client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            with open(transcript_info["transcript_path"], "r") as f:
                transcript = f.read()

                if not transcript.strip():
                    raise ValueError("Empty transcript")


                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": """
                            You are an insightful and succinct podcast summarizer. 
                            Summarize in 500 words or less, following the pareto principle.
                            Correct misspellings, especially names. 
                            Ignore advertisements.

                            Format as JSON with:
                            {
                                "takeaways": [5-10 key points],
                                "quotes": [3 interesting quotes with attribution],
                                "summary": [2-3 paragraphs]
                            }
                        """}, 
                        {"role": "user", "content": transcript}
                    ]
                )

                summary = response.choices[0].message.content
                safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(transcript_info["episode_url"]))
                summary_path = os.path.join(SUMMARY_DIR, f"{safe_filename}.json")

                with open(summary_path, "w") as f:
                    f.write(summary)

                return {
                    "episode_url": transcript_info["episode_url"],
                    "summary_path": summary_path,
                    "audio_paths": transcript_info["audio_paths"],
                    "transcript_path": transcript_info["transcript_path"],
                    "status": "processed"
                }
        
        except Exception as e:
            logging.error(f"Summary generation failed: {str(e)}")
            # Clean up any partial files
            if 'summary_path' in locals() and os.path.exists(summary_path):
                os.remove(summary_path)
            raise


    @task
    def store_summary(summary_info: SummaryResult) -> None:
        """Store summary and clean up files"""
        if not summary_info or summary_info.get("status") == "skipped":
            logging.info(f"Skipping DB storage for: {summary_info.get('episode_url') if summary_info else 'unknown'}")
            return

        if not summary_info.get("summary_path") or not os.path.exists(summary_info["summary_path"]):
            raise ValueError(f"Invalid or missing summary: {summary_info}")

        try:             
            with psycopg2.connect(**DB_CONN) as conn:
                with conn.cursor() as cur:
                    with open(summary_info["summary_path"], "r") as f:
                        summary = f.read()

                        if not summary.strip():
                            raise ValueError("Empty summary")
                        
                        cur.execute(
                            "SELECT id FROM \"Episode\" WHERE url = %s", 
                            (summary_info["episode_url"],)
                        )

                        result = cur.fetchone()

                        if not result:
                            raise ValueError(f"Episode not found for URL: {summary_info['episode_url']}")

                        episode_id = result[0]

                        cur.execute(
                            """
                            INSERT INTO "Summary" (content, \"episodeId\", status) 
                            VALUES (%s, %s, %s)
                            """,
                            (summary, episode_id, "published")
                        )
                        conn.commit()

            # Clean up all files
            logging.info(f"Cleaning up files for: {summary_info['episode_url']}")

            for path in summary_info["audio_paths"]:
                if os.path.exists(path):
                    os.remove(path)
            
            for path in [summary_info["transcript_path"], summary_info["summary_path"]]:
                if os.path.exists(path):
                    os.remove(path)

        except Exception as e:
            logging.error(f"Failed to store summary: {str(e)}")
            raise


    # Define the task flow
    podcasts = fetch_podcasts()
    episodes = get_podcast_episodes.expand(podcast=podcasts)

    # Flatten the nested list of episodes
    flattened = flatten_episodes(episodes)
    new_episodes = insert_episode.expand(episode=flattened)

    
    audio_files = process_audio.expand(episode=new_episodes)
    transcripts = transcribe_audio.expand(audio_info=audio_files)
    summaries = generate_summary.expand(transcript_info=transcripts)
    store_summary.expand(summary_info=summaries)

# Create the DAG
dag = process_podcasts()
