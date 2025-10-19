# build
FROM golang:1.22 as build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/mongodb-up-exporter .

# runtime
FROM gcr.io/distroless/base-debian12
ENV LISTEN_ADDR=:9216
ENV CHECK_INTERVAL=5s
# defina MONGODB_URI no container/Pod
EXPOSE 9216
COPY --from=build /bin/mongodb-up-exporter /mongodb-up-exporter
USER 65532:65532
ENTRYPOINT ["/mongodb-up-exporter"]