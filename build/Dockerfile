# syntax=docker/dockerfile:1

FROM golang:1.23.4 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /bin/mongodb-exporter ./cmd/mongodb-exporter

# runtime
FROM gcr.io/distroless/base-debian12
ENV LISTEN_ADDR=:9216
ENV CHECK_INTERVAL=5s
# defina MONGODB_URI no container/Pod
EXPOSE 9216
COPY --from=build /bin/mongodb-exporter /mongodb-exporter
USER 65532:65532
ENTRYPOINT ["/mongodb-exporter"]
