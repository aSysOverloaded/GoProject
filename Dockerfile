# Multi-stage: the toolchain never ships. The runtime image contains two
# static binaries and nothing else - no shell, no package manager, nothing
# for an attacker to pivot into.

FROM golang:1.26-alpine AS build

WORKDIR /src

# Copy the manifests alone first. This layer only invalidates when
# dependencies change, so ordinary source edits reuse the cached download.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a genuinely static binary, which is what lets the
# runtime stage be distroless/static rather than a full distro.
# -trimpath strips local filesystem paths; -s -w drop the symbol table.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/consumer ./cmd/consumer && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/producer ./cmd/producer && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/auditor ./cmd/auditor

# All three binaries live in one image. The producer runs as a Kubernetes
# Job, the consumer and auditor as Deployments; they differ only by command,
# so a single image keeps the build and the registry simple.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/consumer /consumer
COPY --from=build /out/producer /producer
COPY --from=build /out/auditor /auditor

# The auditor applies this on startup if the tables are not already there.
COPY --from=build /src/schema.sql /schema.sql

# distroless/static:nonroot ships uid 65532. Running as non-root is also what
# lets the Pod set runAsNonRoot: true.
USER 65532:65532

EXPOSE 2112

ENTRYPOINT ["/consumer"]
