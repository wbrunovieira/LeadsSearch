FROM buildkite/puppeteer:latest

USER root

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys E88979FB9B30ACF2

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev

WORKDIR /app

COPY requirements.txt .


RUN pip3 install --no-cache-dir lxml==4.6.3


RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "/app/main.py"]

