FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o r2a-cert-sync ./cmd

# distroless/static-nonroot includes CA certificates (required for HTTPS to the
# Rancher API) and runs as uid 65532 (nonroot) by default.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /build/r2a-cert-sync /r2a-cert-sync
ENTRYPOINT ["/r2a-cert-sync"]
