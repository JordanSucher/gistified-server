from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os
import json
import requests


# Database connection settings
DB_CONN = {
    "dbname": "app_db",
    "user": "app_user",
    "password": os.getenv("APP_DB_PASSWORD"),
    "host": "postgres",
    "port": "5432",
}

MAILJET_API_KEY = os.getenv("MJ_APIKEY_PUBLIC")
MAILJET_API_SECRET = os.getenv("MJ_APIKEY_PRIVATE")
MAILJET_SENDER_EMAIL = "jsucher@gmail.com"
MAILJET_SENDER_NAME = "Gistified"

def fetch_users(**kwargs):
    """Fetch all users with 'daily' email preference."""
    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email FROM users WHERE emailpreference = 'daily'")
            users = [{"id": row[0], "name": row[1], "email": row[2]} for row in cur.fetchall()]

    if not users:
        print("⚠️ No users found for daily email digest.")
    kwargs["ti"].xcom_push(key="users", value=users)
    return users


def fetch_summaries(**kwargs):
    """Fetch summaries from the last 24 hours for each user's subscriptions."""
    users = kwargs["ti"].xcom_pull(task_ids="fetch_users", key="users")

    if not users:
        print("⚠️ No users to send emails to.")
        return []

    summaries_by_user = {}

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            for user in users:
                cur.execute("""
                    SELECT s.id, s.content, e.title AS episode_title, e.url AS episode_url, 
                           p.title AS publication_title, p.imageurl AS publication_image
                    FROM summaries s
                    JOIN episodes e ON s.episodeid = e.id
                    JOIN publications p ON e.publicationid = p.id
                    JOIN subscriptions sub ON sub.publicationid = p.id
                    WHERE sub.userid = %s AND e.publishedat >= NOW() - INTERVAL '1 day'
                """, (user["id"],))

                summaries = [{
                    "id": row[0],
                    "content": json.loads(row[1]),  # Parse JSON content
                    "episode_title": row[2],
                    "episode_url": row[3],
                    "publication_title": row[4],
                    "publication_image": row[5]
                } for row in cur.fetchall()]

                if summaries:
                    summaries_by_user[user["email"]] = summaries

    kwargs["ti"].xcom_push(key="summaries_by_user", value=summaries_by_user)
    return summaries_by_user

def send_email(**kwargs):
    """Send daily email digests via Mailjet."""
    summaries_by_user = kwargs["ti"].xcom_pull(task_ids="fetch_summaries", key="summaries_by_user")

    if not summaries_by_user:
        print("⚠️ No summaries to send. Skipping email.")
        return

    mailjet_url = "https://api.mailjet.com/v3.1/send"
    headers = {"Content-Type": "application/json"}

    for user_email, summaries in summaries_by_user.items():
        email_body = f"""
            <h1>Gistified Daily Summary</h1>
            {''.join([
                f"""
                <div style="display: flex; align-items: center; margin-bottom: 10px;">
                    <img src="{s['publication_image']}" style="width: 60px; height: 60px; margin-right: 10px;" />
                    <div>
                        <a href="https://gistified.vercel.app/summaries/{s['id']}">
                            <h2 style="margin: 0;">{s['episode_title']}</h2>
                        </a>
                        <h3 style="margin: 0;">{s['publication_title']}</h3>
                    </div>
                </div>
                <ul>
                    {''.join([f"<li>{t}</li>" for t in s['content'].get('takeaways', [])])}
                </ul>
                """
                for s in summaries
            ])}
        """

        payload = {
            "Messages": [
                {
                    "From": {
                        "Email": MAILJET_SENDER_EMAIL,
                        "Name": MAILJET_SENDER_NAME
                    },
                    "To": [{"Email": user_email}],
                    "Subject": "Gistified Daily Summary",
                    "TextPart": "Your daily podcast digest from Gistified",
                    "HTMLPart": email_body
                }
            ]
        }

        response = requests.post(mailjet_url, auth=(MAILJET_API_KEY, MAILJET_API_SECRET), headers=headers, json=payload)

        if response.status_code != 200:
            print(f"❌ Failed to send email to {user_email}: {response.text}")
        else:
            print(f"✅ Email sent to {user_email}")


# Define DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "send_daily_email_digest",
    default_args=default_args,
    description="Send a daily email digest to users subscribed to podcasts.",
    schedule_interval="0 15 * * *",  # ✅ Runs every day at 15:00 UTC
    catchup=False,
)

fetch_users_task = PythonOperator(
    task_id="fetch_users",
    python_callable=fetch_users,
    dag=dag,
)

fetch_summaries_task = PythonOperator(
    task_id="fetch_summaries",
    python_callable=fetch_summaries,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    dag=dag,
)

# Define task dependencies
fetch_users_task >> fetch_summaries_task >> send_email_task
