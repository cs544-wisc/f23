# Container Setup

## Dockerfile.worker

```
# base image
FROM ubuntu:22.04

# necessary packages
RUN apt-get update; apt-get install -y wget curl openjdk-11-jdk python3-pip net-tools lsof nano unzip
RUN pip3 install jupyterlab==3.5.0 pandas matplotlib pyspark

RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz; tar -xf hadoop-3.3.6.tar.gz; rm hadoop-3.3.6.tar.gz

RUN wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz; \
    tar -xf spark-3.5.0-bin-hadoop3.tgz; \
    rm spark-3.5.0-bin-hadoop3.tgz

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${PATH}:/hadoop-3.3.6/bin:/spark-3.5.0-bin-hadoop3/bin"

# copy the start script into the container
COPY ./worker.sh /start.sh

# default command
CMD ["sh", "/start.sh"]
```

## Dockerfile.main

```
# base image
FROM ubuntu:22.04

# install necessary packages
RUN apt-get update; apt-get install -y wget curl openjdk-11-jdk python3-pip net-tools lsof nano unzip

# Python packages
RUN pip3 install jupyterlab==3.5.0 pandas matplotlib pyspark==3.5.0

# Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz; tar -xf hadoop-3.3.6.tar.gz; rm hadoop-3.3.6.tar.gz

# Spark
RUN wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz; \
    tar -xf spark-3.5.0-bin-hadoop3.tgz; \
    rm spark-3.5.0-bin-hadoop3.tgz

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${PATH}:/hadoop-3.3.6/bin:/spark-3.5.0-bin-hadoop3/bin"

# copy the start script into the container
COPY ./main.sh /start.sh

# default command
CMD ["sh", "/start.sh"]
```


## main.sh

```
./spark-3.5.0-bin-hadoop3/sbin/start-master.sh

hdfs namenode -format -force
hdfs namenode -D dfs.webhdfs.enabled=true -D dfs.replication=1 -fs hdfs://main:9000 &> /var/log/namenode.log &
hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://main:9000 &> /var/log/datanode.log &

cd /notebooks
python3 -m jupyterlab --no-browser --ip=0.0.0.0 --port=5000 --allow-root --NotebookApp.token=''
```

## worker.sh

```
./spark-3.5.0-bin-hadoop3/sbin/start-worker.sh spark://main:7077 -c 1 -m 512M
tail -f /spark-3.5.0-bin-hadoop3/logs/*.out
```

## run.sh

```
#!/bin/bash

# build the Docker images for the main and worker nodes
docker build -t p5-main-image -f Dockerfile.main . 
docker build -t p5-worker-image -f Dockerfile.worker . 

# run the Docker Compose configuration
docker compose up -d
```