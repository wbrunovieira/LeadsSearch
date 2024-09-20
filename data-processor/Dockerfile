FROM golang:1.19

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o main .

RUN apt-get update && apt-get install -y netcat-openbsd

COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

EXPOSE 8081

CMD ["wait-for-it.sh", "db", "5432", "--", "./main"]
