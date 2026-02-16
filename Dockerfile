# ---------- build stage ----------
FROM rust:latest AS builder

WORKDIR /app

# 避免重复编译依赖
COPY Cargo.toml Cargo.lock ./
# 真正编译
COPY src ./src
RUN cargo build --release

# ---------- runtime stage ----------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/tg2web /app/server

ENV RUST_LOG=info

EXPOSE 80

CMD ["./server"]