FROM golang:alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN go build -o cmd/controller/main ./cmd/controller/main.go

EXPOSE 8080

CMD ["/app/cmd/controller/main"]
