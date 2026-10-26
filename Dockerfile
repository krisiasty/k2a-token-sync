# Pinned by digest, not by tag: a tag can be moved, and both of these images end
# up inside a binary that holds cluster-admin on every downstream cluster. The tag
# stays alongside so the pin is readable and so Dependabot can bump the digest.
FROM golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o k2a-token-sync ./cmd

# distroless/static-nonroot includes CA certificates (needed for HTTPS to each
# cluster's API server) and runs as uid 65532 (nonroot) by default.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
COPY --from=builder /build/k2a-token-sync /k2a-token-sync
ENTRYPOINT ["/k2a-token-sync"]
