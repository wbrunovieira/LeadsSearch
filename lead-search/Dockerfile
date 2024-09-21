FROM golang:1.23

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin/app .

RUN apt-get update && apt-get install -y \
    netcat-openbsd


COPY .env /app/.env



COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh


RUN chmod +x /usr/local/bin/wait-for-it.sh

EXPOSE 8082


CMD ["wait-for-it.sh", "db", "5432", "--", "/usr/local/bin/app"]


