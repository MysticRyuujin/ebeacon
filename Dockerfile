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

# Runtime: HTTPS to upstream beacon APIs needs root CAs.
# Pin to bookworm-slim rather than stable-slim so the base image doesn't
# silently change when Debian releases a new major version.
FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && update-ca-certificates

COPY --from=build /out/ebeacon /app/ebeacon
COPY --from=build /src/ebeacon.example.yaml /app/ebeacon.yaml

EXPOSE 5555

USER nobody:nogroup

ENTRYPOINT ["/app/ebeacon"]
CMD ["-config", "/app/ebeacon.yaml"]
