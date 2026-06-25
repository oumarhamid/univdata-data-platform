FROM apache/airflow:2.8.1-python3.8

USER root
# Ajout de "sudo" dans la liste des installations
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gcc default-jre-headless libnss3 libx11-xcb1 libxcomposite1 \
    libxdamage1 libxrandr2 libgbm1 libasound2 libatk-bridge2.0-0 \
    libgtk-3-0 libdrm2 libxshmfence1 docker.io sudo \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Magie : on autorise "airflow" à utiliser "sudo docker" sans mot de passe
RUN echo "airflow ALL=(ALL) NOPASSWD: /usr/bin/docker" >> /etc/sudoers

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN mkdir -p /opt/playwright && chmod -R 777 /opt/playwright

# On redescend sur l'utilisateur standard pour que Playwright soit content !
USER airflow
COPY --chown=airflow:airflow requirements.txt /requirements.txt

ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"

RUN pip install --no-cache-dir "apache-airflow==2.8.1" --constraint "${CONSTRAINT_URL}" apache-airflow-providers-apache-spark
RUN pip install --no-cache-dir "apache-airflow==2.8.1" playwright==1.40.0 -r /requirements.txt

RUN playwright install chromium && rm -rf /home/airflow/.cache/ms-playwright/*.zip
RUN playwright --version