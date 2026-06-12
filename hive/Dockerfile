FROM apache/hive:3.1.3

USER root
RUN apt-get update && apt-get install -y postgresql-client && apt-get clean
COPY lib/postgresql-42.6.0.jar /opt/hive/lib/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown -R hive:hive /opt/hive/conf   # donner les droits d'écriture à l'utilisateur hive
USER hive

ENTRYPOINT ["/entrypoint.sh"]