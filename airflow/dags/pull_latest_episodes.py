from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import feedparser
import openai
import re
from pydub import AudioSegment

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


def fetch_podcasts_from_db(**kwargs):
    """Fetch all podcast RSS feed URLs from the Publication table."""
    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, title, rssFeedUrl FROM Publication")
            publications = [{"id": row[0], "title": row[1], "rssFeedUrl": row[2]} for row in cur.fetchall()]

    if not publications:
        raise ValueError("No podcasts found in the database.")

    kwargs["ti"].xcom_push(key="podcasts", value=publications)
    return publications


def fetch_recent_podcast_metadata(**kwargs):
    """Fetch metadata for all feeds stored in the database."""
    podcasts = kwargs["ti"].xcom_pull(key="podcasts", task_ids="fetch_podcasts_from_db")

    recent_episodes = []
    for podcast in podcasts:
        feed_url = podcast["rssFeedUrl"]
        response = requests.get(feed_url)
        response.raise_for_status()

        feed = feedparser.parse(response.text)
        new_episodes = [
            {
                "publication_id": podcast["id"],
                "episode_title": entry.title,
                "episode_summary": entry.summary,
                "episode_pub_date": entry.published,
                "episode_url": entry.enclosures[0].href if entry.enclosures else None
            }
            for entry in feed.entries if entry.enclosures
        ][:3]  # Get up to 3 most recent episodes per feed

        recent_episodes.extend(new_episodes)

    kwargs["ti"].xcom_push(key="recent_episodes", value=recent_episodes)
    return recent_episodes


def insert_recent_episodes(**kwargs):
    """Insert new episodes into the database if they don't already exist and return inserted episodes."""
    episodes = kwargs["ti"].xcom_pull(key="recent_episodes", task_ids="fetch_recent_podcast_metadata")

    if not episodes:
        raise ValueError("No new episodes found.")

    inserted_episodes = []

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            for episode in episodes:
                cur.execute(
                    """
                    INSERT INTO episodes (title, description, url, publicationId, publishedAt)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    RETURNING id, url
                    """,
                    (episode["episode_title"], episode["episode_summary"], episode["episode_url"], episode["publication_id"], episode["episode_pub_date"])
                )
                result = cur.fetchone()
                if result:
                    inserted_episodes.append({"id": result[0], "url": result[1]})

            conn.commit()

    if not inserted_episodes:
        raise ValueError("No new episodes were inserted.")

    kwargs["ti"].xcom_push(key="inserted_episodes", value=inserted_episodes)
    return inserted_episodes

def download_audio(**kwargs):
    """Download episode audio files."""
    episodes = kwargs["ti"].xcom_pull(task_ids="insert_recent_episodes", key="inserted_episodes")

    episode_details_enriched = []
    for episode in episodes:
        episode_url = episode["url"]
        safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(episode_url))
        local_path = os.path.join(AUDIO_DIR, safe_filename)

        response = requests.get(episode_url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            raise ValueError(f"Failed to download {episode_url}")

        episode_details_enriched.append({
            "episode_url": episode_url,
            "episode_path": local_path
        })

    kwargs["ti"].xcom_push(key="downloaded_audio", value=episode_details_enriched)
    return episode_details_enriched

def split_audio(**kwargs):
    """Split audio files into chunks."""

    episodes = kwargs["ti"].xcom_pull(task_ids="download_audio", key="downloaded_audio")
    chunks = []

    for episode in episodes:
        audio_path = episode["episode_path"]
        episode_url = episode["episode_url"]

        audio = AudioSegment.from_file(audio_path)
        chunk_size_ms = 15 * 60 * 1000  # 15-minute chunks

        for i, start in enumerate(range(0, len(audio), chunk_size_ms)):
            chunk = audio[start:start + chunk_size_ms]
            chunk_path = f"{audio_path}_chunk_{i}.mp3"
            chunk.export(chunk_path, format="mp3")
            chunks.append((chunk_path, episode_url))

    kwargs["ti"].xcom_push(key="audio_chunks", value=chunks)
    return chunks

def generate_transcripts(**kwargs):
    """Generate transcripts from audio chunks."""

    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    audio_chunks = kwargs["ti"].xcom_pull(task_ids="split_audio", key="audio_chunks")
    transcripts = {}
    transcript_paths = []

    for chunk_path, episode_url in audio_chunks:
        with open(chunk_path, "rb") as audio_file:
            response = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
            )

            if not response.text.strip():
                raise ValueError(f"Transcription failed for {chunk_path}")
            
        if episode_url not in transcripts:
            transcripts[episode_url] = []
        transcripts[episode_url].append(response.text)

    # Concatenate all transcripts for each episode
    for episode_url in transcripts:
        transcripts[episode_url] = "".join(transcripts[episode_url])

    for episode_url, transcript in transcripts.items():
        safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(episode_url))
        transcript_path = os.path.join(TRANSCRIPT_DIR, f"{safe_filename}.txt")
        with open(transcript_path, "w") as f:
            f.write(transcript)

        transcript_paths.append({
            "episode_url": episode_url,
            "transcript_path": transcript_path
        })

    return transcript_paths

def summarize_text(**kwargs):
    """Summarize transcripts and save to disk."""

    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    transcript_paths = kwargs["ti"].xcom_pull(task_ids="generate_transcripts", key="return_value")
    summary_paths = []

    for episode in transcript_paths:
        transcript_path = episode["transcript_path"]
        episode_url = episode["episode_url"]

        with open(transcript_path, "r") as f:
            transcript = f.read()

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "system", "content": 
                            """
                            You are an insightful and succinct podcast summarizer. 
                            Please summarize the provided transcript in 500 words or less, with the idea of the "pareto principle" in mind - provide 80% of the value with 20% of the length. 
                            Correct any misspellings in the transcript, especially misspelled names. 
                            Ignore / leave out advertisements.

                            Generate 3 sections:
                            1 - Key Takeaways (provide 5-10)
                            2 - Quotes (pick 3 of the most interesting or surprising, provide no commentary. Specify who said them. Wrap the quote in single quotes)
                            3 - Summary (break into short paragraphs)

                            Reply in a JSON string and nothing else: {takeaways: [1,2,3,4...], quotes: [1,2,3...], summary: [paragraph1, paragraph2...]}
                            """
                        }, {"role": "user", "content": transcript}]
            ) 

            if not response.choices[0].message.content.strip():
                raise ValueError(f"Summary failed for {episode_url}")

            safe_filename = re.sub(r'[^\w\-_]', '_', os.path.basename(episode_url)) + ".json"
            summary_path = os.path.join(SUMMARY_DIR, safe_filename)

            with open(summary_path, "w") as f:
                f.write(response.choices[0].message.content)

            summary_paths.append({"episode_url": episode_url, "summary_path": summary_path})

    return summary_paths


def store_results(**kwargs):
    episode_summary_paths = kwargs["ti"].xcom_pull(key="return_value", task_ids="summarize_text")

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            for episode in episode_summary_paths:
                with open(episode["summary_path"], "r") as f:
                    summary = f.read()
                    episode_url = episode["episode_url"]
                    # get episode id    
                    cur.execute("SELECT id FROM \"Episode\" WHERE url = %s", (episode_url,))
                    result = cur.fetchone()

                    if not result:
                        raise ValueError(f"Episode not found in database: {episode_url}")

                    episode_id = result[0]

                    # Insert summary
                    cur.execute(
                        "INSERT INTO \"Summary\" (content, \"episodeId\", status) VALUES (%s, %s, %s)", (summary, episode_id, "published")
                        )
        
            conn.commit()


def clean_up(**kwargs):
    # Clean up main audio files
    main_files = kwargs["ti"].xcom_pull(key="return_value", task_ids="download_audio") or []
    for main_file in main_files:
        if os.path.exists(main_file["episode_path"]):
            os.remove(main_file["episode_path"])

    # Clean up audio chunks
    audio_chunks = kwargs["ti"].xcom_pull(key="return_value", task_ids="split_audio") or []
    for chunk_path, episode_url in audio_chunks:
        if os.path.exists(chunk_path):
            os.remove(chunk_path)

    # Clean up transcript files
    transcript_files = kwargs["ti"].xcom_pull(key="return_value", task_ids="generate_transcripts") or []
    for episode in transcript_files:
        transcript_path = episode["transcript_path"]
        if os.path.exists(transcript_path):
            os.remove(transcript_path)

    # Clean up summary files
    summary_files = kwargs["ti"].xcom_pull(key="return_value", task_ids="summarize_text") or []
    for episode in summary_files:
        summary_path = episode["summary_path"]
        if os.path.exists(summary_path):
            os.remove(summary_path)
    


# Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_recent_podcast_episodes",
    default_args=default_args,
    description="Fetch and process recent podcast episodes from the database",
    schedule_interval="0 * * * *",  # Runs every hour
    catchup=False
)

fetch_podcasts = PythonOperator(
    task_id="fetch_podcasts_from_db",
    python_callable=fetch_podcasts_from_db,
    dag=dag,
)

fetch_metadata = PythonOperator(
    task_id="fetch_recent_podcast_metadata",
    python_callable=fetch_recent_podcast_metadata,
    dag=dag,
)

insert_episodes = PythonOperator(
    task_id="insert_recent_episodes",
    python_callable=insert_recent_episodes,
    dag=dag,
)

download = PythonOperator(
    task_id="download_audio",
    python_callable=download_audio,
    dag=dag,
)

split_audio = PythonOperator(
    task_id="split_audio",
    python_callable=split_audio,
    dag=dag,
)

generate_transcripts = PythonOperator(
    task_id="generate_transcripts",
    python_callable=generate_transcripts,
    dag=dag,
)

summarize_text = PythonOperator(
    task_id="summarize_text",
    python_callable=summarize_text,
    dag=dag,
)

store_results = PythonOperator(
    task_id="store_results",
    python_callable=store_results,
    dag=dag,
)

clean_up = PythonOperator(
    task_id="clean_up",
    python_callable=clean_up,
    dag=dag,
)

# Define dependencies
fetch_podcasts >> fetch_metadata >> insert_episodes >> download >> split_audio >> generate_transcripts >> summarize_text >> store_results >> clean_up
