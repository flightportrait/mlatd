# mlatd — build:  docker build -t mlatd .
FROM rust:1.97-slim-trixie AS build
WORKDIR /src
# make: jemalloc's build (tikv-jemalloc-sys) runs its configure + make.
RUN apt-get update && apt-get install -y --no-install-recommends make \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release -p mlatd

FROM debian:trixie-slim
COPY --from=build /src/target/release/mlatd /usr/local/bin/mlatd
# Plain binary, all behavior via flags; see compose.example.yml for wiring.
ENTRYPOINT ["/usr/local/bin/mlatd"]
