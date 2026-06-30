# =============================================================================
# Custom Airflow Image with Kafka & Spark Providers
# =============================================================================
FROM apache/airflow:2.10.5-python3.11

#Để cài các gói hệ thống như java vì để cài thì nó cần quyền cao nhất
USER root 

#-y là để k cần xác nhận 
#default-jdk là bắt buộc để chạy đc spark
# Install system dependencies for providers

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        default-jdk \ 
        docker.io \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

#apt-get clean dùng để xóa bớt các file .deb để giảm kích thước của hđh
#rm -rf /var/lib/apt/lists/* xóa các file đã cập nhật 
# Set JAVA_HOME for Spark integration

ENV JAVA_HOME=/usr/lib/jvm/default-java

#khi tải default-jdk thì nó mặc định tải java vào thư mục sau, dùng env như vậy là để các ứng dụng chạy java có thể đọc được
USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
