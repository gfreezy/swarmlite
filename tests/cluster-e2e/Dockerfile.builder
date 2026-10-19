FROM rust:1.97-alpine
RUN apk add --no-cache binutils cmake make musl-dev perl
WORKDIR /src
ENV SWARMLITE_UI_DIST=/ui-dist CARGO_TARGET_DIR=/target
# A real static Linux executable, without release LTO's unnecessary E2E cost.
ENV CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_OPT_LEVEL=1
CMD ["cargo", "build", "--release", "--locked", "-j", "6"]
