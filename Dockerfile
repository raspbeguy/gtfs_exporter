FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

# Docker sets TARGETOS and TARGETARCH from the target platform. The build stage
# runs on the native platform and cross-compiles, so no emulation is necessary.
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/gtfs_exporter .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gtfs_exporter /gtfs_exporter
# A default configuration so that the image starts without a mount. It sets no
# allowed_hosts, so the exporter warns at start. Mount your own file over this
# path for a real deployment.
COPY contrib/container-config.yml /etc/gtfs_exporter/config.yml
EXPOSE 10053
USER nonroot:nonroot
ENTRYPOINT ["/gtfs_exporter"]
CMD ["-config", "/etc/gtfs_exporter/config.yml"]
