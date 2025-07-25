FROM golang:alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN go build -o cmd/loadbalancer/main ./cmd/loadbalancer/main.go

EXPOSE 9001

CMD ["/app/cmd/loadbalancer/main"]
