# Multi-stage build: one image containing every binary. Services pick the
# binary with their command.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl && adduser -D -u 10001 stream
COPY --from=build /out/ /usr/local/bin/
USER stream
WORKDIR /home/stream
