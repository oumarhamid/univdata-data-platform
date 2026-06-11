FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y curl gcc && apt-get clean

# Installation de Playwright et des dépendances système nécessaires
RUN apt-get update && apt-get install -y \
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
    && apt-get clean

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Installer Playwright et le navigateur Chromium
RUN pip install playwright && \
    playwright install chromium

# Optionnel : vérifier l'installation
RUN playwright --version