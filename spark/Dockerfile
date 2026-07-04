# =============================================================================
# Custom Spark Image with Kafka Connector and SSH Server for Airflow
# =============================================================================
FROM apache/spark:3.5.6-python3

USER root

# Install SSH server and other OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server \
    curl \
    wget \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up SSH for Airflow SSHOperator
RUN mkdir -p /var/run/sshd \
 && echo 'root:root' | chpasswd \
 && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -i 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' /etc/pam.d/sshd

RUN useradd -ms /bin/bash sparkuser && echo "sparkuser:sparkpass" | chpasswd

# Install Python dependencies for PySpark streaming
RUN pip install --no-cache-dir --quiet \
    py4j \
    prometheus-client \
    delta-spark==3.2.1

# Download Spark-Kafka connector and dependencies from Maven Central
# These JARs are required for Structured Streaming with Kafka and Iceberg+S3
RUN mkdir -p /opt/spark/jars/extras && \
    cd /opt/spark/jars/extras && \
    curl -sLO "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    curl -sLO "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Install OpenSSH Server
RUN apt-get update && apt-get install -y openssh-server && \
    mkdir -p /var/run/sshd

RUN useradd -ms /bin/bash sparkuser && echo "sparkuser:sparkpass" | chpasswd

# Configure SSH for sparkuser
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config || true

EXPOSE 22

# Copy Spark configuration
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/spark/work-dir