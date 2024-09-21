FROM golang:1.23

WORKDIR /app



COPY go.mod go.sum ./
RUN go mod tidy
COPY . .
RUN go build -o main .

RUN apt-get update && apt-get install -y netcat-openbsd

COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

EXPOSE 8080

CMD ["wait-for-it.sh", "db", "5432", "--", "./main"]
