# Dockerfile
FROM python:3.12-slim

# Dépendances système
RUN apt-get update && apt-get install -y \
    gcc build-essential libxml2-dev libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Dossier de travail
WORKDIR /app

# Copier tous les fichiers du projet
COPY . /app

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Définir le script par défaut
CMD ["python", "unitests.py"]
