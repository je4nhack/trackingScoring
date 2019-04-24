FROM gettyimages/spark:2.1.0-hadoop-2.7

CMD /usr/spark-2.1.0/sbin/start-history-server.sh
RUN mkdir /tmp/spark-events

# Fix a bug. Hadoop common include avro 1.7.4 which is not compatible with Spark
# I've applied following solution https://github.com/SingularitiesCR/spark-docker/issues/5
COPY lib/avro-1.7.7.jar /tmp/avro-1.7.7.jar
RUN rm $HADOOP_HOME/share/hadoop/common/lib/avro-1.7.4.jar \
    && rm $HADOOP_HOME/share/hadoop/mapreduce/lib/avro-1.7.4.jar \
    && cp /tmp/avro-1.7.7.jar $HADOOP_HOME/share/hadoop/common/lib \
    && mv /tmp/avro-1.7.7.jar $HADOOP_HOME/share/hadoop/mapreduce/lib

WORKDIR $SPARK_HOME