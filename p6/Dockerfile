FROM ubuntu:22.04
RUN apt-get update; apt-get install -y wget curl openjdk-8-jdk python3-pip net-tools lsof vim unzip

# Python stuff
RUN pip3 install jupyterlab==4.0.3 pandas==2.1.1 pyspark==3.4.1 cassandra-driver==3.28.0 grpcio==1.58.0 grpcio-tools==1.58.0 nbformat==5.9.2

# SPARK
RUN wget https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && tar -xf spark-3.4.1-bin-hadoop3.tgz && rm spark-3.4.1-bin-hadoop3.tgz

# CASSANDRA
RUN wget https://dlcdn.apache.org/cassandra/4.1.3/apache-cassandra-4.1.3-bin.tar.gz; tar -xf apache-cassandra-4.1.3-bin.tar.gz; rm apache-cassandra-4.1.3-bin.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH="${PATH}:/apache-cassandra-4.1.3/bin:/spark-3.4.1-bin-hadoop3.2/bin"

COPY cassandra.sh /cassandra.sh
CMD ["sh", "/cassandra.sh"]
