FROM eclipse-temurin:8-jdk

RUN apt-get update && \
    apt-get install -y wget tar && \
    wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzf pig-0.17.0.tar.gz && \
    mv pig-0.17.0 /opt/pig && \
    rm pig-0.17.0.tar.gz

ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin

RUN mkdir -p /workspace

WORKDIR /workspace