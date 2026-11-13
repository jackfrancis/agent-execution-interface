# The AEI reference operator (manager). The controller is a separate Go module
# whose replace directives point at ../ and ../providers, so the build context
# MUST be the repo root:
#   docker build -f controller/Dockerfile -t localhost/aei-manager:dev .
#
# The build stage runs on the build machine's own platform and cross-compiles for the
# target (--platform), so building linux/amd64 on an arm64 laptop does not emulate
# the Go toolchain.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETARCH
WORKDIR /src
COPY . .
WORKDIR /src/controller
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -o /out/manager ./cmd/manager

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
