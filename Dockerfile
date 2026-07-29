FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/swarmlite /usr/local/bin/swarmlite
ENTRYPOINT ["/usr/local/bin/swarmlite"]
