# Dockerfile
FROM golang:1.23.3

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o producer_app main.go

CMD ["./producer_app"]
