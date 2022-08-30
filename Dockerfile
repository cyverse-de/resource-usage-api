FROM golang:1.18 as build-root

WORKDIR /go/src/github.com/cyverse-de/resource-usage-api
COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build --buildvcs=false .
RUN go clean -cache -modcache
RUN cp ./resource-usage-api /bin/resource-usage-api

ENTRYPOINT ["resource-usage-api"]

EXPOSE 60000
