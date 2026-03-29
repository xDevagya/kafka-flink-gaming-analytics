ARG FLINK_VERSION=2.0.0
FROM flink:${FLINK_VERSION}-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2

RUN curl -fsSL \
      "${MAVEN}/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka.jar

RUN ls -lh /opt/flink/lib/*.jar
