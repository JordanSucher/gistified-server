from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import feedparser
import openai
import re
from pydub import AudioSegment
from typing import List, Dict
from itertools import chain  # Add this import


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
    tags=['podcast']
)
def process_podcasts():
    """
    DAG for fetching and processing podcast episodes in parallel using TaskFlow API.
    """
    
    @task
    def fetch_podcasts() -> List[Dict]:
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
    def get_podcast_episodes(podcast: Dict) -> List[Dict]:
        """Fetch recent episodes for a single podcast"""
        feed = feedparser.parse(requests.get(podcast["rssFeedUrl"]).text)
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

    @task
    def insert_episode(episode: Dict) -> Dict:
        """Insert single episode and return info if new"""
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
                    return {"id": result[0], "url": result[1]}
                return None

    @task
    def process_audio(episode: Dict) -> Dict:
        """Download and split audio for single episode"""
        if not episode:  # Skip if episode wasn't newly inserted
            return None
            
        episode_url = episode["url"]
        safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(episode_url))
        local_path = os.path.join(AUDIO_DIR, safe_filename)

        # Download
        response = requests.get(episode_url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Split into chunks
        audio = AudioSegment.from_file(local_path)
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

    @task
    def transcribe_audio(audio_info: Dict) -> Dict:
        """Generate transcript for single episode"""
        if not audio_info:  # Skip if no audio was processed
            return None
            
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        transcripts = []

        for chunk_path in audio_info["chunk_paths"]:
            with open(chunk_path, "rb") as audio_file:
                response = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                )
                transcripts.append(response.text)

        # Save combined transcript
        full_transcript = "".join(transcripts)
        safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(audio_info["episode_url"]))
        transcript_path = os.path.join(TRANSCRIPT_DIR, f"{safe_filename}.txt")
        
        with open(transcript_path, "w") as f:
            f.write(full_transcript)

        return {
            "episode_url": audio_info["episode_url"],
            "transcript_path": transcript_path,
            "audio_paths": [audio_info["main_audio_path"]] + audio_info["chunk_paths"]
        }

    @task
    def generate_summary(transcript_info: Dict) -> Dict:
        """Generate summary for single episode"""
        if not transcript_info:  # Skip if no transcript was generated
            return None
            
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        with open(transcript_info["transcript_path"], "r") as f:
            transcript = f.read()

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
                "transcript_path": transcript_info["transcript_path"]
            }

    @task
    def store_summary(summary_info: Dict) -> None:
        """Store summary and clean up files"""
        if not summary_info:  # Skip if no summary was generated
            return
            
        with psycopg2.connect(**DB_CONN) as conn:
            with conn.cursor() as cur:
                with open(summary_info["summary_path"], "r") as f:
                    summary = f.read()
                    
                    cur.execute(
                        "SELECT id FROM \"Episode\" WHERE url = %s", 
                        (summary_info["episode_url"],)
                    )
                    episode_id = cur.fetchone()[0]

                    cur.execute(
                        """
                        INSERT INTO "Summary" (content, \"episodeId\", status) 
                        VALUES (%s, %s, %s)
                        """,
                        (summary, episode_id, "published")
                    )
                    conn.commit()

        # Clean up all files
        for path in summary_info["audio_paths"]:
            if os.path.exists(path):
                os.remove(path)
        
        for path in [summary_info["transcript_path"], summary_info["summary_path"]]:
            if os.path.exists(path):
                os.remove(path)

    # Define the task flow
    podcasts = fetch_podcasts()
    episodes_nested = get_podcast_episodes.expand(podcast=podcasts)
    episodes = list(chain.from_iterable(episodes_nested))
    new_episodes = insert_episode.expand(episode=episodes)
    audio_files = process_audio.expand(episode=new_episodes)
    transcripts = transcribe_audio.expand(audio_info=audio_files)
    summaries = generate_summary.expand(transcript_info=transcripts)
    store_summary.expand(summary_info=summaries)

# Create the DAG
dag = process_podcasts()
