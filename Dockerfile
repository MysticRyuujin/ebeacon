# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ebeacon .

# Runtime: distroless/static includes CA certs and nothing else — no shell,
# no package manager, no glibc, no OS CVE surface. The binary is statically
# linked (CGO_ENABLED=0) so it needs no runtime libraries.
FROM gcr.io/distroless/static-debian12
WORKDIR /app

COPY --from=build /out/ebeacon /app/ebeacon
COPY --from=build /src/ebeacon.example.yaml /app/ebeacon.yaml

EXPOSE 5555

USER nonroot:nonroot

ENTRYPOINT ["/app/ebeacon"]
CMD ["-config", "/app/ebeacon.yaml"]
