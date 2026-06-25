FROM apache/hive:3.1.3

USER root
RUN apt-get update && apt-get install -y postgresql-client curl && apt-get clean

# Copie du driver PostgreSQL
COPY lib/postgresql-42.6.0.jar /opt/hive/lib/

# Copie des fichiers de configuration
COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY conf/core-site.xml /opt/hive/conf/core-site.xml

# Téléchargement des dépendances S3A et Hudi avec curl
RUN curl -o /opt/hive/lib/hadoop-aws-3.1.0.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar \
    && curl -o /opt/hive/lib/aws-java-sdk-bundle-1.11.901.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar \
    && curl -o /opt/hive/lib/hudi-spark3-bundle_2.12-0.15.0.jar \
        https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/0.15.0/hudi-spark3-bundle_2.12-0.15.0.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown -R hive:hive /opt/hive/conf

USER hive
ENTRYPOINT ["/entrypoint.sh"]