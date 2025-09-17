FROM bitnami/spark:latest

USER root

# Install Python + dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt

# Copy JAR files to Spark jars directory
COPY spark_jar/*.jar /opt/bitnami/spark/jars/

# Copy application files
COPY main.py /opt/main.py
COPY spark_app/ /opt/spark_app/
COPY config/ /opt/config/

WORKDIR /opt
