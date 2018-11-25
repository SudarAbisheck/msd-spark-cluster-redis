FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    wget \
    openjdk-8-jdk \
    scala && \
    mkdir -p /opt/spark && cd /opt/spark && \
    wget http://mirrors.wuchna.com/apachemirror/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz && \
    tar -xvf spark-2.4.0-bin-hadoop2.7.tgz

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64 
ENV SPARK_HOME /opt/spark/spark-2.4.0-bin-hadoop2.7

ENTRYPOINT ["/opt/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-class","org.apache.spark.deploy.master.Master"]