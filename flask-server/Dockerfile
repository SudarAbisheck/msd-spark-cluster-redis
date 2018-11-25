FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    wget \
    openjdk-8-jdk \
    scala \
    python-pip \
    python-dev && \
    mkdir -p /opt/spark && cd /opt/spark && \
    wget http://mirrors.wuchna.com/apachemirror/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz && \
    tar -xvf spark-2.4.0-bin-hadoop2.7.tgz

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV SPARK_HOME /opt/spark/spark-2.4.0-bin-hadoop2.7
ENV PYSPARK_PYTHON python2.7
ENV PATH $PATH:$SPARK_HOME/bin

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt

ENTRYPOINT [ "spark-submit" ]
CMD ["--master", "spark://spark-master:7077", "--packages", "com.databricks:spark-csv_2.11:1.5.0", "--jars", "/data/spark-redis-2.3.1-SNAPSHOT-jar-with-dependencies.jar", "src/server.py"]