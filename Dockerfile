FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN cargo build --release --locked

FROM golang:1.26-bookworm AS gateway-build
WORKDIR /src
COPY caddy-storage/go.mod caddy-storage/go.sum ./
RUN go mod download
COPY caddy-storage ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/caddy ./cmd/caddy

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/swarmlite /usr/local/bin/swarmlite
COPY --from=gateway-build /out/caddy /usr/local/bin/caddy
ENTRYPOINT ["/usr/local/bin/swarmlite"]
