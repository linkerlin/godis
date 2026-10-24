# syntax=docker/dockerfile:1

# Build stage
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/godis ./

# Runtime stage
FROM alpine:3.21
RUN adduser -D -H -u 10001 godis
WORKDIR /data
COPY --from=build /out/godis /usr/local/bin/godis
COPY example.conf /etc/godis/godis.conf
RUN mkdir -p /data/logs && chown -R godis:godis /data
USER godis
EXPOSE 6399
ENV CONFIG=/etc/godis/godis.conf
VOLUME ["/data"]
ENTRYPOINT ["godis"]
