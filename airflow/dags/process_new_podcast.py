from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import openai
import psycopg2
import os
import feedparser
from pydub import AudioSegment
import re

# Database connection settings
DB_CONN = {
    "dbname": "app_db",
    "user": "app_user",
    "password": os.getenv("APP_DB_PASSWORD"),
    "host": "postgres",
    "port": "5432",
}


# Function to fetch podcast metadata
def fetch_podcast_metadata(feed_url, pub_id, **kwargs):
    response = requests.get(feed_url)
    response.raise_for_status()
    # store feed_url as xcom
    kwargs["ti"].xcom_push(key="feed_url", value=feed_url)
    # store pub_id as xcom
    kwargs["ti"].xcom_push(key="pub_id", value=pub_id)
    # store rss_feed as xcom
    kwargs["ti"].xcom_push(key="rss_feed", value=response.text)

    #continue
    return


# Function to extract the latest episode URLs from an RSS feed
def extract_latest_episodes(**kwargs):
    feed_xml = kwargs["ti"].xcom_pull(key="rss_feed", task_ids="fetch_podcast_metadata")
    feed = feedparser.parse(feed_xml)
    episode_details = [
        {
            "episode_title": entry.title,
            "episode_summary": entry.summary,
            "episode_pub_date": entry.published,
            "episode_url": entry.enclosures[0].href if entry.enclosures else None
        } 
        for entry in feed.entries
        if entry.enclosures
    ][:3]  # Get the latest 3 episodes
    return episode_details

# Function to insert episodes into the database
def insert_episodes(**kwargs):
    
    # get pub_id and episode details
    pub_id = kwargs["ti"].xcom_pull(key="pub_id", task_ids="fetch_podcast_metadata")
    episode_details = kwargs["ti"].xcom_pull(key="return_value", task_ids="extract_latest_episodes")
    
    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            for episode in episode_details:
                cur.execute("INSERT INTO episodes (title, description, url, publicationId, publishedAt) VALUES (%s , %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING", (episode["episode_title"], episode["episode_summary"], episode["episode_url"], pub_id, episode["episode_pub_date"]))
            conn.commit()
            

    return episode_details


# Function to extract audio
def download_audio(**kwargs):

    episodes = kwargs["ti"].xcom_pull(key="return_value", task_ids="extract_latest_episodes")

    episode_details_enriched = []
    for episode in episodes:
        episode_url = episode["episode_url"]
        safe_filename = re.sub(r'[^\w\-_\.]', '_', os.path.basename(episode_url))
        local_path = f"/opt/airflow/data/audio/{safe_filename}.mp3"
        response = requests.get(episode_url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    
        episode_details_enriched.append({
            "episode_url": episode_url,
            "episode_path": local_path
        })

    return episode_details_enriched

def split_audio(chunk_size_mb=15, **kwargs):

    episode_details_enriched = kwargs["ti"].xcom_pull(key="return_value", task_ids="download_audio")

    chunks = []

    for episode in episode_details_enriched:
        audio_path = episode["episode_path"]
        episode_url = episode["episode_url"]

        try:
            audio = AudioSegment.from_file(audio_path)
        except Exception as e:
            print(f"Error processing {audio_path}: {e}")
            continue  # Skip this file


        # Estimate the duration of 10MB in milliseconds
        file_size_bytes = os.path.getsize(audio_path)
        duration_ms = len(audio)
        bytes_per_ms = file_size_bytes / duration_ms  # Estimate bytes per millisecond

        chunk_size_ms = (chunk_size_mb * 1024 * 1024) / bytes_per_ms  # Convert MB to ms
        chunk_size_ms = int(chunk_size_ms)  # Ensure it's an integer

        for i, start in enumerate(range(0, len(audio), chunk_size_ms)):
            chunk = audio[start:start + chunk_size_ms]
            chunk_path = f"{audio_path}_chunk_{i}.mp3"
            chunk.export(chunk_path, format="mp3")
            chunks.append((chunk_path, episode_url))

    return chunks


def generate_transcripts(**kwargs):
    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    episode_transcripts = {}
    transcript_paths = []
    
    audio_chunks = kwargs["ti"].xcom_pull(key="return_value", task_ids="split_audio")

    for chunk_path, episode_url in audio_chunks:
        with open(chunk_path, "rb") as audio_file:
            response = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
            )
            
            if episode_url not in episode_transcripts:
                episode_transcripts[episode_url] = []
            episode_transcripts[episode_url].append(response.text)

    # Concatenate all transcripts for each episode
    for episode_url in episode_transcripts:
        episode_transcripts[episode_url] = "".join(episode_transcripts[episode_url])

    # write transcripts to file
    for episode_url, transcript in episode_transcripts.items():
        safe_filename = re.sub(r'[^\w\-_\.]', '_', os.path.basename(episode_url))
        transcript_paths.append({"episode_url": episode_url, "transcript_path": f"/opt/airflow/data/transcripts/{safe_filename}.txt"})
        with open(f"/opt/airflow/data/transcripts/{safe_filename}.txt", "w") as f:
            f.write(transcript)


    return transcript_paths


# Function to summarize text with OpenAI
def summarize_text(**kwargs):

    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    transcript_paths = kwargs["ti"].xcom_pull(key="return_value", task_ids="generate_transcripts")
    episode_summary_paths = []

    for episode in transcript_paths:
        episode_url = episode["episode_url"]
        transcript_path = episode["transcript_path"]
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

            safe_filename = re.sub(r'[^\w\-_\.]', '_', os.path.basename(episode_url))
            local_path = f"/opt/airflow/data/summaries/{safe_filename}.json"

            with open(local_path, "w") as f:
                f.write(response.choices[0].message.content)

            
            episode_summary_paths.append({"episode_url": episode_url, "summary_path": local_path})

    return episode_summary_paths

# Function to store results in the database
def store_results(**kwargs):
    episode_summary_paths = kwargs["ti"].xcom_pull(key="return_value", task_ids="summarize_text")

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            for episode in episode_summary_paths:
                with open(episode["summary_path"], "r") as f:
                    summary = f.read()
                    episode_url = episode["episode_url"]
                    # get episode id
                    cur.execute("SELECT id FROM episodes WHERE url = %s", (episode_url,))
                    result = cur.fetchone()

                    if not result:
                        print(f"Episode with URL {episode_url} not found in database")
                        continue

                    episode_id = result[0]

                    # Insert summary
                    cur.execute(
                        "INSERT INTO summaries (content, episodeId, status) VALUES (%s, %s, %s)", (summary, episode_id, "processed")
                        )
        
            conn.commit()
            

# Function to clean up temporary files
def clean_up(**kwargs):
    # Clean up main audio files
    main_files = kwargs["ti"].xcom_pull(key="return_value", task_ids="download_audio")
    for main_file in main_files:
        if os.path.exists(main_file["episode_path"]):
            os.remove(main_file["episode_path"])

    # Clean up audio chunks
    audio_chunks = kwargs["ti"].xcom_pull(key="return_value", task_ids="split_audio")
    for chunk_path, episode_url in audio_chunks:
        if os.path.exists(chunk_path):
            os.remove(chunk_path)

    # Clean up transcript files
    transcript_files = kwargs["ti"].xcom_pull(key="return_value", task_ids="generate_transcripts")
    for episode in transcript_files:
        transcript_path = episode["transcript_path"]
        if os.path.exists(transcript_path):
            os.remove(transcript_path)

    # Clean up summary files
    summary_files = kwargs["ti"].xcom_pull(key="return_value", task_ids="summarize_text")
    for episode in summary_files:
        summary_path = episode["summary_path"]
        if os.path.exists(summary_path):
            os.remove(summary_path)
    


# Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "process_new_podcast",
    default_args=default_args,
    description="Process a newly added podcast",
    schedule_interval=None,
)

# Define tasks
fetch_metadata = PythonOperator(
    task_id="fetch_podcast_metadata",
    python_callable=fetch_podcast_metadata,
    op_kwargs={
        "feed_url": "{{ dag_run.conf['feed_url'] }}",
        "pub_id": "{{ dag_run.conf['pub_id'] }}"
        },
    dag=dag,
)

get_episodes = PythonOperator(
    task_id="extract_latest_episodes",
    python_callable=extract_latest_episodes,
    dag=dag,
)

insert_episodes = PythonOperator(
    task_id="insert_episodes",
    python_callable=insert_episodes,
    dag=dag,
)

download = PythonOperator(
    task_id="download_audio",
    python_callable=download_audio,
    dag=dag,
)

split = PythonOperator(
    task_id="split_audio",
    python_callable=split_audio,
    dag=dag,
)

transcribe = PythonOperator(
    task_id="generate_transcripts",
    python_callable=generate_transcripts,
    dag=dag,
)

summarize = PythonOperator(
    task_id="summarize_text",
    python_callable=summarize_text,
    dag=dag,
)

store = PythonOperator(
    task_id="store_results",
    python_callable=store_results,
    dag=dag,
)

cleanup = PythonOperator(
    task_id="clean_up",
    python_callable=clean_up,
    dag=dag,
)

# Define task dependencies
fetch_metadata >> get_episodes >> insert_episodes >> download >> split >> transcribe >> summarize >> store >> cleanup
