FROM eclipse-temurin:8-jdk

RUN apt-get update && \
    apt-get install -y wget tar && \
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz && \
    tar -xzf hadoop-2.10.2.tar.gz -C /opt/ && \
    mv /opt/hadoop-2.10.2 /opt/hadoop && \
    rm hadoop-2.10.2.tar.gz && \
    wget -q https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz && \
    tar -xzf apache-hive-2.3.9-bin.tar.gz -C /opt/ && \
    mv /opt/apache-hive-2.3.9-bin /opt/hive && \
    rm apache-hive-2.3.9-bin.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin

RUN mkdir -p /opt/hive/conf && \
    echo "<configuration><property><name>mapreduce.framework.name</name><value>local</value></property><property><name>hive.metastore.uris</name><value></value></property><property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:;databaseName=/tmp/metastore_db;create=true</value></property></configuration>" > /opt/hive/conf/hive-site.xml

RUN schematool -dbType derby -initSchema

RUN mkdir -p /workspace
WORKDIR /workspace
