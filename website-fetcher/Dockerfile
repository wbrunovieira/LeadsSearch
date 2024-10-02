FROM python:alpine3.20


ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk update && apk add --no-cache \
    wget \
    curl \
    gnupg \
    git \ 
    gcc \
    musl-dev \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    openssl-dev \
    zlib-dev \
    build-base 

RUN pip install --upgrade pip
RUN pip install --only-binary :all: pika beautifulsoup4 python-dotenv requests lxml && \
    pip install aiohttp==3.7.4




COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

COPY . .


CMD ["python", "main.py"]
