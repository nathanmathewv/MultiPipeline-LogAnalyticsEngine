FROM maven:3.9-eclipse-temurin-21

WORKDIR /workspace

# Hadoop local mode can be noisy about user/native-library settings.
# The actual MapReduce framework classes will come from Maven dependencies.
ENV HADOOP_USER_NAME=root
ENV HADOOP_OPTS="-Djava.library.path=/tmp"
ENV MAVEN_OPTS="-Duser.timezone=UTC"

COPY app/mapreduce/run-mapreduce.sh /usr/local/bin/run-mapreduce.sh
RUN chmod +x /usr/local/bin/run-mapreduce.sh

CMD ["bash"]
