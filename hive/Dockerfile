FROM apache/hive:3.1.3
USER root
# Delete old postgresql driver and download a newer one
RUN rm -f /opt/hive/lib/postgresql-*.jar
COPY postgresql-42.7.4.jar /opt/hive/lib/

# Copy AWS Jars for S3/MinIO access
COPY hadoop-aws-3.1.0.jar /opt/hive/lib/
COPY aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/
#2 file jar này giúp hive có thể giao tiếp được mới minIO

# Copy hive-site.xml
COPY hive-site.xml /opt/hive/conf/hive-site.xml

USER hive

# Define entrypoint to init schema if it fails to start, then run metastore
ENTRYPOINT ["/bin/sh", "-c", "/opt/hive/bin/schematool -dbType postgres -initSchema || true && /opt/hive/bin/hive --service metastore"]
