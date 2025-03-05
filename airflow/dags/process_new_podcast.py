from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.dag_functions import get_podcast_episodes_from_feed, insert_episode_to_db, process_audio_for_episode, transcribe_audio_for_episode, generate_summary_for_episode, store_summary_for_episode

# Function to fetch podcast metadata
def fetch_podcast_metadata(feed_url, pub_id, **kwargs):
    # store feed_url as xcom
    kwargs["ti"].xcom_push(key="feed_url", value=feed_url)
    # store pub_id as xcom
    kwargs["ti"].xcom_push(key="pub_id", value=pub_id)

    return

# Function to extract the latest episode URLs from an RSS feed
def extract_latest_episodes(**kwargs):
    feed_url = kwargs["ti"].xcom_pull(key="feed_url", task_ids="fetch_podcast_metadata")
    pub_id = kwargs["ti"].xcom_pull(key="pub_id", task_ids="fetch_podcast_metadata")
    podcast = {
        "rssFeedUrl": feed_url,
        "id": pub_id,
        "title": feed_url
    }

    return get_podcast_episodes_from_feed(podcast)

# Function to insert episodes into the database
def insert_episodes(**kwargs):
    
    # get pub_id and episode details
    episode_details = kwargs["ti"].xcom_pull(key="return_value", task_ids="extract_latest_episodes")

    if not episode_details:
        raise ValueError("No episodes found")

    inserted_episodes = []
    for episode in episode_details:
        inserted_episodes.append(insert_episode_to_db(episode))

    return inserted_episodes


# Function to extract audio
def download_and_split_audio(**kwargs):

    episodes = kwargs["ti"].xcom_pull(key="return_value", task_ids="insert_episodes")

    if not episodes:
        raise ValueError("No episodes found")

    audio_info = []
    for episode in episodes:
        audio_info.append(process_audio_for_episode(episode))

    return audio_info

def generate_transcripts(**kwargs):
    audio_info = kwargs["ti"].xcom_pull(key="return_value", task_ids="download_and_split_audio")

    if not audio_info:
        raise ValueError("No audio info found")

    transcript_info = []
    for episode in audio_info:
        transcript_info.append(transcribe_audio_for_episode(episode))

    return transcript_info

# Function to summarize text with OpenAI
def summarize_text(**kwargs):
    transcript_info = kwargs["ti"].xcom_pull(key="return_value", task_ids="generate_transcripts")

    if not transcript_info:
        raise ValueError("No transcript info found")

    summary_info = []
    for episode in transcript_info:
        summary_info.append(generate_summary_for_episode(episode))

    return summary_info

# Function to store results in the database
def store_results(**kwargs):
    summary_info = kwargs["ti"].xcom_pull(key="return_value", task_ids="summarize_text")

    if not summary_info:
        raise ValueError("No summary info found")

    for episode in summary_info:
        store_summary_for_episode(episode)
            
    return

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
    python_callable=download_and_split_audio,
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

# Define task dependencies
fetch_metadata >> get_episodes >> insert_episodes >> download >> transcribe >> summarize >> store
