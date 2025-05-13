FROM flink:1.18.0
USER root

RUN rm -rf /opt/flink/*
ADD flink-1.18.0-thesis-mar-31-25-bin.tgz /opt/flink/
RUN chown -R flink:flink /opt/flink

USER flink
