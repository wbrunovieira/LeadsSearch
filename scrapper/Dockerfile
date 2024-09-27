FROM python:3.11-bullseye

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install required system dependencies
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

# Upgrade pip, setuptools, and wheel
RUN pip install --only-binary :all: pika beautifulsoup4 python-dotenv requests lxml && \
    pip install aiohttp==3.7.4

# Copy requirements.txt before installing dependencies
COPY requirements.txt .

# Install Python dependencies using pre-built binaries where possible
RUN pip install -r requirements.txt


WORKDIR /app

# Copy the rest of the application code
COPY . .

# Run the application
CMD ["python", "main.py"]
