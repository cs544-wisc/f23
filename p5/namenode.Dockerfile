FROM p5-base
CMD hdfs namenode -format -force && \
    hdfs namenode -fs hdfs://nn:9000
