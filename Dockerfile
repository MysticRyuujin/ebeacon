# syntax=docker/dockerfile:1

# Pin the build stage to the BUILD platform (the runner's native arch) and
# cross-compile to TARGETOS/TARGETARCH inside it. This avoids QEMU emulation
# of the Go compiler — a multi-arch build that would otherwise spend ~10
# minutes emulating arm64 instructions on an amd64 runner now finishes in
# the time of two native compiles.
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/ebeacon .

# Runtime: distroless/static includes CA certs and nothing else — no shell,
# no package manager, no glibc, no OS CVE surface. The binary is statically
# linked (CGO_ENABLED=0) so it needs no runtime libraries. This stage is
# unpinned so BuildKit pulls the per-target-platform variant of the base
# image; the COPY brings in the binary cross-compiled above.
FROM gcr.io/distroless/static-debian12
WORKDIR /app

COPY --from=build /out/ebeacon /app/ebeacon
COPY --from=build /src/ebeacon.example.yaml /app/ebeacon.yaml

EXPOSE 5555

USER nonroot:nonroot

ENTRYPOINT ["/app/ebeacon"]
CMD ["-config", "/app/ebeacon.yaml"]
