FROM flink:1.18.1-scala_2.12-java11

COPY connectors/*.jar /opt/flink/lib/
COPY submit-cdc-job.sh /opt/flink/bin/submit-cdc-job.sh
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar /opt/flink/lib/flink-metrics-prometheus-1.18.1.jar

USER root
RUN chmod 755 /opt/flink/bin/submit-cdc-job.sh

RUN mkdir -p /opt/flink/checkpoints \
    && touch /opt/flink/checkpoints/.volume-init \
    && chown -R flink:flink /opt/flink/checkpoints
USER flink
