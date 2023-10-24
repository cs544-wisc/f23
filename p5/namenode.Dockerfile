FROM p5-base
CMD hdfs namenode -format && \
    hdfs namenode -D -fs hdfs://nn:9000
