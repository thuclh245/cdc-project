FROM flink:1.18.1-scala_2.12-java11

COPY connectors/*.jar /opt/flink/lib/

USER root
RUN mkdir -p /opt/flink/checkpoints \
    && touch /opt/flink/checkpoints/.volume-init \
    && chown -R flink:flink /opt/flink/checkpoints
USER flink
