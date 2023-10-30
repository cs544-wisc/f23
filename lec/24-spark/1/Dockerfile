FROM ubuntu:22.04
RUN apt-get update; apt-get install -y wget curl openjdk-11-jdk python3-pip net-tools lsof nano

# Jupyter
RUN pip3 install jupyterlab==4.0.3 pandas==2.1.1 pyspark==3.5.0 matplotlib==3.8.0

# HDFS
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && tar -xf hadoop-3.3.6.tar.gz && rm hadoop-3.3.6.tar.gz

# SPARK
RUN wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && tar -xf spark-3.5.0-bin-hadoop3.tgz && rm spark-3.5.0-bin-hadoop3.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${PATH}:/hadoop-3.3.6/bin"
ENV HADOOP_HOME=/hadoop-3.3.6
