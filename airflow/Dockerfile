FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libdrm2 \
    libxshmfence1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:airflow requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Playwright + Chromium
RUN pip install --no-cache-dir playwright==1.40.0 && \
    playwright install chromium && \
    rm -rf /home/airflow/.cache/ms-playwright/*.zip

RUN playwright --version

USER root
RUN apt-get update && apt-get install -y docker.io
USER airflow