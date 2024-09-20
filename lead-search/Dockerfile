
FROM golang:1.20 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod tidy


COPY . .

COPY .env /app/.env


RUN go build -o main .




RUN apt-get update && apt-get install -y netcat-openbsd


COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh


RUN chmod +x /usr/local/bin/wait-for-it.sh

EXPOSE 8082


CMD ["wait-for-it.sh", "db", "5432", "--", "./main"]


