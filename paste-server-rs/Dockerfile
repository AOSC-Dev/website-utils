FROM rust:1-alpine AS builder

WORKDIR /app
RUN apk add --no-cache musl-dev
COPY . .
RUN cargo build --release

FROM alpine:latest AS runner

WORKDIR /app
COPY --from=builder /app/target/release/paste-server-rs .
EXPOSE 2334
CMD ["./paste-server-rs"]
