FROM golang:1.23 AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y gcc libc6-dev

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
# Compilando o binário estático
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/app .

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    netcat-openbsd curl

COPY --from=builder /usr/local/bin/app /usr/local/bin/app
COPY .env /app/.env
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh

RUN chmod +x /usr/local/bin/wait-for-it.sh

EXPOSE 8082

CMD ["wait-for-it.sh", "db", "5432", "--", "/usr/local/bin/app"]

