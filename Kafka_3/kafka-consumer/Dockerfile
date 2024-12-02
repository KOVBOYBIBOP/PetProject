# Dockerfile
FROM golang:1.23.3

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o consumer_app main.go

CMD ["./consumer_app"]
