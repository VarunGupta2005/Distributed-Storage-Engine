# ---------------------------------------------------------
# STAGE 1: Shared Build Environment
# ---------------------------------------------------------
FROM ubuntu:22.04 AS builder

# Install build toolchain and C++ dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libssl-dev \
    libpqxx-dev \
    libpq-dev

WORKDIR /app

COPY CMakeLists.txt .
COPY src/ ./src/

RUN mkdir build && cd build && \
    cmake .. && \
    make -j$(nproc)

# ---------------------------------------------------------
# STAGE 2: Master Node Runtime
# ---------------------------------------------------------
FROM ubuntu:22.04 AS master_runtime

# Install OpenSSL, libpq (C runtime), and libpqxx (C++ runtime)
RUN apt-get update && apt-get install -y \
    libssl3 \
    libpq5 \
    libpqxx-6.4 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/build/master_server /app/master_server

ENTRYPOINT ["/app/master_server"]
CMD ["8081"]

# ---------------------------------------------------------
# STAGE 3: Worker Node Runtime
# ---------------------------------------------------------
FROM ubuntu:22.04 AS worker_runtime

# Worker ONLY requires OpenSSL
RUN apt-get update && apt-get install -y \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/build/worker_server /app/worker_server

RUN mkdir -p /app/data

ENTRYPOINT ["/app/worker_server"]
CMD ["9001"]