FROM golang:1.26-alpine AS build

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

FROM golang:1.26-alpine AS test-runner

WORKDIR /workspace

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/market-relayer ./cmd/market-relayer
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/sink-printer ./cmd/sink-printer

FROM alpine:3.21

RUN apk add --no-cache ca-certificates curl gettext tzdata

COPY --from=build /out/market-relayer /usr/local/bin/market-relayer
COPY --from=build /out/sink-printer /usr/local/bin/sink-printer

ENTRYPOINT ["/usr/local/bin/market-relayer"]
CMD ["-config", "/etc/market-relayer/config.yaml"]
