FROM golang:latest
WORKDIR /src/
COPY . /src/
RUN go mod download
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /usr/bin/olricd /src/cmd/olricd
EXPOSE 3320 3322
ENTRYPOINT ["/usr/bin/olricd", "-c", "/src/cmd/olricd/olricd.yaml"]
