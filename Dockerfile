FROM node:24-alpine AS ui-build
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui ./
RUN npm run build

FROM rust:1.97-alpine AS build
RUN apk add --no-cache binutils cmake make musl-dev perl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY --from=ui-build /ui/dist /ui-dist
ENV SWARMLITE_UI_DIST=/ui-dist
RUN cargo build --release --locked \
    && install -D -m 0755 target/release/swarmlite /out/swarmlite \
    && if readelf --program-headers --wide /out/swarmlite | grep -q INTERP; then exit 1; fi \
    && if readelf --dynamic --wide /out/swarmlite 2>&1 | grep -q '(NEEDED)'; then exit 1; fi

FROM alpine:3.23 AS certificates
RUN apk add --no-cache ca-certificates

FROM scratch
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/swarmlite /usr/local/bin/swarmlite
ENTRYPOINT ["/usr/local/bin/swarmlite"]
