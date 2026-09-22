FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg ca-certificates apt-transport-https unixodbc-dev gcc g++ \
 && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

COPY docker/entrypoint-airflow.sh /opt/airflow/entrypoint.sh
RUN chmod 0755 /opt/airflow/entrypoint.sh

USER airflow
# Apenas o pyodbc: ele nao tem dependencias Python, entao nao altera as versoes
# que a imagem oficial do Airflow ja resolveu. O pacote de DAGs usa a biblioteca
# padrao e o SQLAlchemy que ja vem na imagem.
RUN pip install --no-cache-dir pyodbc==5.2.0
