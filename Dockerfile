#
# Stage 1: build the Rust binary.
#
FROM rust:1.94-slim-trixie@sha256:cf09adf8c3ebaba10779e5c23ff7fe4df4cccdab8a91f199b0c142c53fef3e1a AS builder

WORKDIR /build

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin wyoming-edge-tts

#
# Stage 2: runtime — distroless cc base for libssl + glibc, non-root.
#
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:8f960b7fc6a5d6e28bb07f982655925d6206678bd9a6cde2ad00ddb5e2077d78

COPY --from=builder /build/target/release/wyoming-edge-tts /usr/local/bin/wyoming-edge-tts

ENV PORT=10210

EXPOSE 10210
ENTRYPOINT ["/usr/local/bin/wyoming-edge-tts"]
