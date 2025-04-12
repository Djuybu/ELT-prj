# Sử dụng ảnh base Apache Airflow
FROM apache/airflow:2.10.4

# Cập nhật các kho lưu trữ và cài đặt Java
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean;

# Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Đảm bảo quay lại user airflow
USER airflow
