# Use uma imagem Python leve
FROM python:3.11-bullseye

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    gcc \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libssl-dev \
    zlib1g-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --only-binary :all: pika beautifulsoup4 python-dotenv requests lxml && \
    pip install aiohttp==3.7.4

# Defina o diretório de trabalho

# Copie o arquivo de dependências
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
# Copie o código da aplicação
COPY . .

# Defina o comando padrão para rodar a aplicação
CMD ["python", "main.py"]
