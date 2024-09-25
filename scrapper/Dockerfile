FROM python:3.11-buster

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    gcc \
    libxml2-dev \
    libxslt-dev \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Definir o diretório de trabalho
WORKDIR /app

# Copiar o arquivo requirements.txt para instalar dependências do Python
COPY requirements.txt .

# Instalar dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código da aplicação para o diretório de trabalho
COPY . .

# Definir o comando padrão para executar o aplicativo
CMD ["python", "main.py"]
