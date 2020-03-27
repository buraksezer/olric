FROM golang:latest as build
WORKDIR /src/
COPY . /src/
RUN go mod download
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /usr/bin/olricd /src/cmd/olricd

FROM gcr.io/distroless/base-debian10
COPY --from=build /usr/bin/olricd /usr/bin/olricd
COPY --from=build /src/cmd/olricd/olricd.yaml /etc/olricd.yaml

EXPOSE 3320 3322
ENTRYPOINT ["/usr/bin/olricd", "-c", "/etc/olricd.yaml"]
