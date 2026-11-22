# Pinned by digest, not by tag: a tag can be moved, and both of these images end
# up inside a binary that holds cluster-admin on every downstream cluster. The tag
# stays alongside so the pin is readable and so Dependabot can bump the digest.
FROM golang:1.27.4-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o k2a-token-sync ./cmd

# distroless/static-nonroot includes CA certificates (needed for HTTPS to each
# cluster's API server) and runs as uid 65532 (nonroot) by default.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931
COPY --from=builder /build/k2a-token-sync /k2a-token-sync
ENTRYPOINT ["/k2a-token-sync"]
