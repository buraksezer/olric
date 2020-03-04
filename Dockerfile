FROM golang:latest
ENV GO111MODULE=on
ADD . /build
WORKDIR /build
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o bin/olricd ./cmd/olricd
EXPOSE 3320 3322
ENTRYPOINT bin/olricd -c cmd/olricd/olricd.yaml
