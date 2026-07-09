FROM apache/airflow:3.2.2-python3.12

USER root

# test ssh connection
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc python3-dev openssh-client \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# Install requirements (providers + dependencies)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
