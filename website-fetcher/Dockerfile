FROM python:3.11


RUN apt-get update && apt-get install -y --no-install-recommends \
  libxml2-dev \
  libxslt1-dev \
  zlib1g-dev \
  gcc \
  python3-dev \
  && rm -rf /var/lib/apt/lists/*



RUN pip install --upgrade pip


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt



RUN apt-get purge -y --auto-remove gcc libxml2-dev libxslt1-dev zlib1g-dev

WORKDIR /app


COPY . .



CMD ["python", "main.py"]
