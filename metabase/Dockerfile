FROM metabase/metabase:v0.49.16

USER root
RUN apk add --no-cache wget && \
    wget -O /plugins/hive-jdbc-3.1.3-standalone.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar && \
    chmod 644 /plugins/hive-jdbc-3.1.3-standalone.jar