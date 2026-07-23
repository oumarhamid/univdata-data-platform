FROM apache/spark:3.5.0

USER root


# Utiliser un miroir plus rapide (par exemple celui de France)
RUN sed -i 's/archive.ubuntu.com/fr.archive.ubuntu.com/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/fr.archive.ubuntu.com/g' /etc/apt/sources.list

    
# Mise à jour et installation de pip3 (déjà présent normalement)
RUN apt-get update && apt-get install -y python3-pip && apt-get clean

# Installation des dépendances Python via pip (beaucoup plus fiable)
RUN pip3 install --no-cache-dir \
    requests \
    beautifulsoup4 \
    chardet \
    boto3 \
    pypdf

# Libs Hadoop AWS pour MinIO
RUN curl -sSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle.jar

# Connecteur Elasticsearch Spark
RUN curl -f -L -o /opt/spark/jars/elasticsearch-spark.jar https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.12.0/elasticsearch-spark-30_2.12-8.12.0.jar

# Dépendance Python pour parser les PDF
RUN pip3 install --no-cache-dir pypdf==5.9.0

USER spark