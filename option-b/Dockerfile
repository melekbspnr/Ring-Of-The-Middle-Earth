# Build the Go binary
FROM golang:1.22-alpine3.21 AS builder
RUN apk add --no-cache gcc musl-dev librdkafka-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# -tags dynamic: link against apk's musl librdkafka (vendor bundle targets glibc).
RUN CGO_ENABLED=1 GOOS=linux go build -tags dynamic -o /rotr-server ./cmd/server

# Runtime image
FROM alpine:3.21
RUN apk add --no-cache librdkafka

WORKDIR /app
COPY --from=builder /rotr-server /app/rotr-server

EXPOSE 8080
ENTRYPOINT ["/app/rotr-server"]
