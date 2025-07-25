# Build stage
FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o client ./cmd/client/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/client /app/client

RUN apk add --no-cache bash

RUN chmod +x /app/client

ENV PATH="/app:${PATH}"

ENV CLIENT_BASE_URL=http://loadbalancer:9001

ENTRYPOINT ["bash"]