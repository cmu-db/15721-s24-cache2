FROM rust:1.76.0-bookworm AS chef
# We only pay the installation cost once,
# it will be cached from the second build onwards
RUN cargo install cargo-chef

WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
ENV ROCKET_PROFILE=development
RUN cargo install --path .

# We do not need the Rust toolchain to run the binary!
FROM redis:7.2.4
COPY --from=builder /usr/local/cargo/bin/* /usr/local/bin
COPY docker-entrypoint.sh redis.conf Rocket.toml /usr/local/bin/
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh

CMD ["/usr/local/bin/docker-entrypoint.sh"]

