FROM apache/airflow:2.6.3

USER root  # Switch to root user to install packages

RUN apt-get update && apt-get install -y ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow  # Switch back to airflow user

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
