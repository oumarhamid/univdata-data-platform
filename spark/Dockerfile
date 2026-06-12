FROM apache/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y curl gcc python3-pip && apt-get clean
RUN pip3 install --no-cache-dir beautifulsoup4 requests

# Libs Hadoop AWS pour MinIO
RUN curl -sSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws.jar && \
    curl -sSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle.jar

USER spark