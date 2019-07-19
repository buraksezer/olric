FROM golang:latest
RUN mkdir -p /go/src/github.com/buraksezer/olric
ADD . /go/src/github.com/buraksezer/olric
WORKDIR /go/src/github.com/buraksezer/olric
RUN go build -o bin/olricd ./cmd/olricd
EXPOSE 3320 3322
ENTRYPOINT bin/olricd -c cmd/olricd/olricd.toml
