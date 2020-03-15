FROM golang:1.14-alpine AS build
WORKDIR /src/
COPY . /src/
ENV GO111MODULE=on
RUN go mod download
RUN CGO_ENABLED=0 go build -o /usr/bin/olricd /src/cmd/olricd

FROM scratch
COPY --from=build /usr/bin/olricd /usr/bin/olricd
COPY --from=build /src/cmd/olricd/olricd.yaml /etc/olricd.yaml
EXPOSE 3320 3322
ENTRYPOINT ["/usr/bin/olricd", "-c", "/etc/olricd.yaml"]
