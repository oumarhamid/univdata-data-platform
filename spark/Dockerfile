FROM apache/spark:3.5.0

USER root

# Installation des dépendances via apt pour qu'elles soient disponibles partout
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-requests \
    python3-bs4 \
    python3-chardet \
    python3-boto3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Libs Hadoop AWS pour MinIO
RUN curl -sSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle.jar

# Connecteur Elasticsearch Spark
RUN curl -f -L -o /opt/spark/jars/elasticsearch-spark.jar https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.12.0/elasticsearch-spark-30_2.12-8.12.0.jar

# Dépendance Python pour parser les PDF
RUN pip3 install --no-cache-dir pypdf==5.9.0

USER spark