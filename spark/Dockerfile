FROM apache/spark:3.5.0
USER root
RUN apt-get update && apt-get install -y curl && apt-get clean
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
RUN pip install beautifulsoup4