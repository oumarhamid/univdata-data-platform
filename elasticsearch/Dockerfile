FROM docker.elastic.co/elasticsearch/elasticsearch:8.10.2

# Passage en root pour installer des packages (optionnel)
USER root

# Installation de plugins (exemples : analyse-icu, analyse-phonetic)
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install analysis-icu && \
    /usr/share/elasticsearch/bin/elasticsearch-plugin install analysis-phonetic

# Si tu veux installer d'autres plugins, ajoute-les ici
# RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install https://...

# Retour à l'utilisateur elasticsearch (sécurité)
USER elasticsearch

# Ports exposés (déjà dans l'image)
EXPOSE 9200 9300