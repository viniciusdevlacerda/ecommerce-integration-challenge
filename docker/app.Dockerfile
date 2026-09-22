FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Driver ODBC 18 da Microsoft (necessario para o pyodbc falar com SQL Server).
# O repositorio da Microsoft para Debian 12 publica pacotes amd64 e arm64,
# entao a mesma imagem serve Intel e Apple Silicon.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl gnupg ca-certificates apt-transport-https gcc g++ unixodbc-dev \
 && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && apt-get purge -y --auto-remove gnupg apt-transport-https \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY . .
RUN chmod +x docker/*.sh

EXPOSE 8000
