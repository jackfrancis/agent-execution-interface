# The AEI reference operator (manager). The controller is a separate Go module
# whose replace directives point at ../ and ../providers, so the build context
# MUST be the repo root:
#   docker build -f controller/Dockerfile -t localhost/aei-manager:dev .
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
WORKDIR /src/controller
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/manager ./cmd/manager

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
