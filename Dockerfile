# Pinned by digest, not by tag: a tag can be moved, and both of these images end
# up inside a binary that holds cluster-admin on every downstream cluster. The tag
# stays alongside so the pin is readable and so Dependabot can bump the digest.
FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o k2a-token-sync ./cmd

# distroless/static-nonroot includes CA certificates (needed for HTTPS to each
# cluster's API server) and runs as uid 65532 (nonroot) by default.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
COPY --from=builder /build/k2a-token-sync /k2a-token-sync
ENTRYPOINT ["/k2a-token-sync"]
