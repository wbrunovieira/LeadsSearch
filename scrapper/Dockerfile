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


COPY requirements.txt .


RUN pip install -r requirements.txt


WORKDIR /app


COPY . .


CMD ["python", "main.py"]
