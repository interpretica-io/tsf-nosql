# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# The image ci/core-test.sh builds the agent-side core in, natively for
# the host architecture: every client library the drivers link, with
# development headers. The DataStax C/C++ driver for Cassandra is not
# packaged by Ubuntu, so it is built from source; the rest comes from
# apt. The suite's build image (tsf-nosql-ts) installs the same set.

FROM ubuntu:24.04

ARG CASS_DRIVER_VERSION=2.17.1

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake curl git pkg-config \
        libhiredis-dev libmongoc-dev libbson-dev libmemcached-dev \
        libcurl4-openssl-dev libjansson-dev libssl-dev libuv1-dev zlib1g-dev \
        && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/cpp-driver.tar.gz \
        "https://github.com/datastax/cpp-driver/archive/refs/tags/${CASS_DRIVER_VERSION}.tar.gz" && \
    mkdir -p /tmp/cpp-driver && \
    tar -C /tmp/cpp-driver --strip-components=1 -xzf /tmp/cpp-driver.tar.gz && \
    cmake -S /tmp/cpp-driver -B /tmp/cpp-driver/build \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCASS_BUILD_STATIC=OFF -DCASS_BUILD_SHARED=ON \
        -DCASS_BUILD_EXAMPLES=OFF -DCASS_BUILD_TESTS=OFF \
        -DCMAKE_CXX_FLAGS="-Wno-error" && \
    cmake --build /tmp/cpp-driver/build -j"$(nproc)" && \
    cmake --install /tmp/cpp-driver/build && \
    ldconfig && rm -rf /tmp/cpp-driver /tmp/cpp-driver.tar.gz
