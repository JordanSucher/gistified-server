FROM apache/airflow:2.6.3-python3.8
# This version includes Debian with apt-get

USER root
# This will work on the full Debian-based image

RUN apt-get update && apt-get install -y ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Switch back to airflow user for security

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
