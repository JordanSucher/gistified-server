from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import whisper
import openai
import psycopg2
import os

# Database connection settings
DB_CONN = {
    "dbname": "app_db",
    "user": "app_user",
    "password": os.getenv("APP_DB_PASSWORD"),
    "host": "postgres",
    "port": "5432",
}


# Function to fetch podcast metadata
def fetch_podcast_metadata(feed_url, **kwargs):
    response = requests.get(feed_url)
    response.raise_for_status()
    return response.text  # Return RSS XML


# Function to download episodes
def download_episodes(feed_xml, **kwargs):
    # Extract episode URLs from the feed XML (parsing omitted for brevity)
    episode_urls = extract_episode_urls(feed_xml)[:3]
    return episode_urls  # Return list of URLs

# Function to extract audio
def extract_audio(episode_url, **kwargs):
    local_path = f"/opt/airflow/data/audio/{os.path.basename(episode_url)}"
    response = requests.get(episode_url, stream=True)
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return local_path


# Function to generate transcripts
def generate_transcripts(audio_path, **kwargs):
    model = whisper.load_model("small")
    transcript = model.transcribe(audio_path)["text"]
    return transcript


# Function to summarize text with OpenAI
def summarize_text(transcript, **kwargs):
    openai.api_key = os.getenv("OPENAI_API_KEY")
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "system", "content": "Summarize this:"}, {"role": "user", "content": transcript}]
    )
    return response["choices"][0]["message"]["content"]


# Function to store results in the database
def store_results(feed_url, transcript, summary, **kwargs):
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()
    cur.execute("INSERT INTO summaries (feed_url, transcript, summary) VALUES (%s, %s, %s)", (feed_url, transcript, summary))
    conn.commit()
    cur.close()
    conn.close()


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
    op_kwargs={"feed_url": "{{ dag_run.conf['feed_url'] }}"},
    provide_context=True,
    dag=dag,
)

download = PythonOperator(
    task_id="download_episodes",
    python_callable=download_episodes,
    provide_context=True,
    dag=dag,
)

extract = PythonOperator(
    task_id="extract_audio",
    python_callable=extract_audio,
    provide_context=True,
    dag=dag,
)

transcribe = PythonOperator(
    task_id="generate_transcripts",
    python_callable=generate_transcripts,
    provide_context=True,
    dag=dag,
)

summarize = PythonOperator(
    task_id="summarize_text",
    python_callable=summarize_text,
    provide_context=True,
    dag=dag,
)

store = PythonOperator(
    task_id="store_results",
    python_callable=store_results,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_metadata >> download >> extract >> transcribe >> summarize >> store
