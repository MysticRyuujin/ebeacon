# Routing and Auth

This guide explains the request shapes eBeacon accepts, how auth works, and how to map external DNS patterns onto simple path and header inputs.

## Recommended Deployment Shape

For a single eBeacon deployment serving multiple beacon networks, prefer top-level `networks:` plus top-level `auth:`.

That keeps eBeacon focused on only four inputs:

- Network path prefix: `/{networkId}/eth/v1/...`
- Optional client route prefix after the network: `/{networkId}/{clientRoute}/eth/v1/...`
- Optional path auth prefix: `/{apiKey}/{networkId}/eth/v1/...`
- Optional upstream override header: `X-EBEACON-Use-Upstream: <upstreamId-or-glob>`

If you have HAProxy in front, translate DNS and host-based routing into those simple path/header forms before forwarding to eBeacon.

## URL Shapes

- Primary path: `/{networkId}/eth/v1/...`
- If exactly one network exists, prefix-free `"/eth/v1/..."` also works.
- Path-auth variant: `/{apiKey}/{networkId}/eth/v1/...`
- Client-route variant: `/{networkId}/{clientRoute}/eth/v1/...`

The client-route form also applies to the synthetic node health endpoint. For example, `/{networkId}/lighthouse/eth/v1/node/health` reports health for only the upstreams selected by that route.

## Authentication

When top-level `auth` is configured, clients can authenticate with any of:

- `X-EBEACON-Secret-Token: <secret>`
- `X-API-Key: <secret>`
- `Authorization: Bearer <secret>`
- Query parameter `?secret=<secret>`
- URL path segment via `/{apiKey}/{networkId}/eth/v1/...`

Secret values support environment expansion in config, for example:

```yaml
auth:
  secret: "${EBEACON_API_SECRET}"
```

## Path-embedded API Keys

For clients where setting HTTP headers is not possible, the API key can be embedded in the URL path:

```text
/{apiKey}/{networkId}/eth/v1/...
```

The embedded segment is stripped before the request is routed to the upstream.

## HAProxy Mapping

If DNS is only a user-facing concern, HAProxy should normalize requests before they hit eBeacon.

General rules:

- Derive the network from the hostname.
- Keep client auth headers like `X-API-Key` unchanged when forwarding to eBeacon.
- If auth is embedded in the public path, rewrite it into eBeacon's expected form: `/{apiKey}/{networkId}/...`.
- `X-EBEACON-Use-Upstream` and `?use-upstream=` are for eBeacon only; eBeacon strips them before calling upstream beacon nodes.

Desired external forms:

- `mainnet.beacon.example.com/`
- `mainnet.beacon.example.com/lighthouse/`
- `lighthouse.mainnet.beacon.example.com/`
- `mainnet.beacon.example.com/` with `X-EBEACON-Use-Upstream: lighthouse`

Recommended internal forms sent to eBeacon:

- `mainnet.beacon.example.com/` -> `/{mainnet}/...`
- `mainnet.beacon.example.com/lighthouse/...` -> `/mainnet/lighthouse/...`
- `lighthouse.mainnet.beacon.example.com/...` -> `/mainnet/...` plus `X-EBEACON-Use-Upstream: lighthouse`
- `sepolia.beacon.example.com/<apiKey>/eth/v1/...` -> `/<apiKey>/sepolia/eth/v1/...`

### Routing examples

Public request:

```text
GET https://sepolia.beacon.example.com/eth/v1/node/version
X-API-Key: premium-local-test-key
```

Forward to eBeacon as:

```text
GET http://ebeacon:5555/sepolia/eth/v1/node/version
X-API-Key: premium-local-test-key
```

Public request with path-auth:

```text
GET https://sepolia.beacon.example.com/premium-local-test-key/eth/v1/node/version
```

Forward to eBeacon as:

```text
GET http://ebeacon:5555/premium-local-test-key/sepolia/eth/v1/node/version
```

Public request with a hostname-selected upstream:

```text
GET https://lighthouse.sepolia.beacon.example.com/eth/v1/node/version
X-API-Key: premium-local-test-key
```

Forward to eBeacon as:

```text
GET http://ebeacon:5555/sepolia/eth/v1/node/version
X-API-Key: premium-local-test-key
X-EBEACON-Use-Upstream: lighthouse
```

### Header-auth HAProxy example

This pattern handles the normal case where the client sends `X-API-Key` and the hostname determines the network:

```haproxy
frontend fe_ebeacon
  bind :80

  http-request set-var(txn.network) req.hdr(host),lower,map(/etc/haproxy/ebeacon-network.map)
  http-request deny unless { var(txn.network) -m found }

  # Preserve the client X-API-Key header exactly as received.
  http-request set-path /%[var(txn.network)]%[path]
  default_backend be_ebeacon

backend be_ebeacon
  server ebeacon ebeacon:5555
```

With this config:

- `https://sepolia.beacon.example.com/eth/v1/node/version` becomes `http://ebeacon:5555/sepolia/eth/v1/node/version`
- If the client sent `X-API-Key`, it is forwarded to eBeacon unchanged.

### Path-auth HAProxy example

This pattern supports public URLs like `https://sepolia.beacon.example.com/<apiKey>/eth/v1/...`.

```haproxy
frontend fe_ebeacon
  bind :80

  http-request set-var(txn.network) req.hdr(host),lower,map(/etc/haproxy/ebeacon-network.map)
  http-request deny unless { var(txn.network) -m found }

  # Rewrite /<apiKey>/<rest> -> /<apiKey>/<network>/<rest>
  acl has_two_segments path_reg ^/[^/]+/.+
  http-request replace-path ^/([^/]+)/(.*)$ /\1/%[var(txn.network)]/\2 if has_two_segments
  default_backend be_ebeacon

backend be_ebeacon
  server ebeacon ebeacon:5555
```

With this config:

- `https://sepolia.beacon.example.com/premium-local-test-key/eth/v1/node/version`
  becomes `http://ebeacon:5555/premium-local-test-key/sepolia/eth/v1/node/version`

Do not rewrite path-auth into `/sepolia/<apiKey>/...`; eBeacon expects `/{apiKey}/{network}/...`.

### Hostname-selected upstream example

If you want `lighthouse.sepolia.beacon.example.com` to force one upstream while keeping the rest of the path unchanged:

```haproxy
frontend fe_ebeacon
  bind :80

  http-request set-var(txn.network) req.hdr(host),lower,map(/etc/haproxy/ebeacon-network.map)
  http-request deny unless { var(txn.network) -m found }

  acl host_lighthouse req.hdr(host),lower -m beg lighthouse.
  http-request set-header X-EBEACON-Use-Upstream lighthouse if host_lighthouse

  http-request set-path /%[var(txn.network)]%[path]
  default_backend be_ebeacon

backend be_ebeacon
  server ebeacon ebeacon:5555
```

### Web UI example

If you expose the Web UI on a separate hostname, preserve either header auth or the tokenized path form:

```text
https://status.beacon.example.com/                     -> http://ebeacon:5555/webui/
https://status.beacon.example.com/api/health          -> http://ebeacon:5555/webui/api/health
https://status.beacon.example.com/webui-local-test-key/api/health
                            -> http://ebeacon:5555/webui/webui-local-test-key/api/health
```

Example HAProxy rule:

```haproxy
frontend fe_ebeacon_ui
  bind :80
  acl host_status req.hdr(host),lower -i status.beacon.example.com
  http-request set-path /webui%[path] if host_status
  use_backend be_ebeacon if host_status

backend be_ebeacon
  server ebeacon ebeacon:5555
```

One straightforward HAProxy pattern is to map hostnames to networks, then optionally set an upstream header for client-specific subdomains:

```haproxy
frontend fe_ebeacon
    bind :80

    http-request set-var(txn.network) req.hdr(host),lower,map(/etc/haproxy/ebeacon-network.map)
    http-request deny unless { var(txn.network) -m found }

    acl host_lighthouse req.hdr(host),lower -m beg lighthouse.
    http-request set-header X-EBEACON-Use-Upstream lighthouse if host_lighthouse

    http-request set-path /%[var(txn.network)]%[path]
    default_backend be_ebeacon

backend be_ebeacon
    server ebeacon 127.0.0.1:5555
```

Example map file:

```text
mainnet.beacon.example.com mainnet
lighthouse.mainnet.beacon.example.com mainnet
hoodi.beacon.example.com hoodi
lighthouse.hoodi.beacon.example.com hoodi
sepolia.beacon.example.com sepolia
lighthouse.sepolia.beacon.example.com sepolia
```

If you prefer path-based upstream selection instead of the header, rewrite the client-specific hostname to `/mainnet/lighthouse/...` and use `routing.clientRoutes`.

## Forced Upstream Selection

Clients can request a specific upstream by name using:

- `X-EBEACON-Use-Upstream: <value>`
- Query parameter `?use-upstream=<value>`

`<value>` may be an **exact upstream ID**, a **client selector** like `client:nimbus`, or a **glob pattern** (e.g. `*lighthouse*`).

These selectors are hard selectors, just like `clientRoutes`:

- Exact upstream IDs use only that upstream.
- `client:<type>` retries only within that client type.
- Glob patterns retry only within the matched upstream set.
- If no matching selected upstream is available, eBeacon returns `503 Service Unavailable` instead of falling back to normal load balancing.

Selector-driven requests use a separate cache namespace from unselected traffic, so a generic cached response is never reused for a client-selected request.

Response headers include the selected upstream:

- `X-Ebeacon-Upstream`

## Routing Controls

### Client routes

`clientRoutes` map path prefixes to a single upstream (or `client:<type>`):

```yaml
routing:
  clientRoutes:
    - pathPrefix: "/teku/"
      upstreamId: teku
    - pathPrefix: "/nimbus/"
      upstreamId: "client:nimbus"
```

Client-route matches are hard selectors. eBeacon will only use the configured upstream or client type for that request, and returns `503 Service Unavailable` instead of silently failing over to a different client when the selected client is unavailable.

For standard Beacon API paths, eBeacon also supports zero-config selector inference: `/{networkId}/{selector}/eth/v...` will automatically route to a matching detected client type like `nimbus` or to an exact upstream ID if no client type match is available. Explicit `clientRoutes` still take precedence and remain useful for aliases and non-standard paths.

### Route rules

`routeRules` are ordered (first match wins), and can either route or deny:

```yaml
routing:
  routeRules:
    - pathPattern: "^/eth/v1/debug/.*"
      deny: true
    - pathPattern: "^/eth/v1/beacon/blocks$"
      methods: [POST]
      upstreamId: teku
```

### Blocked paths

`blockedPaths` is a denylist regex array for immediate `403` responses.

## Request Parameter Sanitization

eBeacon strips internal query keys before upstream calls and cache keys:

- `secret`
- `use-upstream`
- `token`

This prevents accidental credential forwarding and noisy cache keys.
