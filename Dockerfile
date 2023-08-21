FROM golang:1.21.0-bookworm
COPY . /go/src/github.com/alphauslabs/jupiter/
WORKDIR /go/src/github.com/alphauslabs/jupiter/
RUN CGO_ENABLED=0 GOOS=linux go build -v -trimpath -installsuffix cgo -o jupiter .

FROM debian:stable-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /jupiter/
COPY --from=0 /go/src/github.com/alphauslabs/jupiter/jupiter .
ENTRYPOINT ["/jupiter/jupiter"]
CMD ["-lostostderr"]
