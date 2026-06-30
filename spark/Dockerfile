# =============================================================================
# Custom Spark Image with Kafka Connector
# =============================================================================
FROM apache/spark:3.5.6-python3

USER root

# Install Python dependencies for PySpark streaming
RUN pip install --no-cache-dir --quiet \
    py4j \
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

# Copy Spark configuration
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/spark/work-dir
