#!/usr/bin/env python3
# Copyright 2026, DragonflyDB authors.  All rights reserved.
# See LICENSE for licensing terms.
"""Dense-owner no-regression guard for the pub/sub fanout measurement."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys

from perfloop_pubsub import Failure, Shape, checked_path, emit, require, throughput_sample


DENSE_SHAPE = Shape(16, 16, 1, 64)


def dense_sample(
    binary: pathlib.Path, loader: pathlib.Path, duration_ms: int, connections: int
) -> dict[str, float]:
    return {
        **throughput_sample(binary, loader, DENSE_SHAPE, duration_ms, connections),
        "pool_threads": float(DENSE_SHAPE.threads),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dragonfly", required=True)
    parser.add_argument("--loader", required=True)
    parser.add_argument("--duration-ms", type=int, default=1000)
    parser.add_argument("--connections", type=int, default=1)
    args = parser.parse_args()

    try:
        binary = checked_path(args.dragonfly, "Dragonfly binary")
        loader = checked_path(args.loader, "native load driver")
        require(args.duration_ms > 0, "--duration-ms must be positive")
        require(args.connections > 0, "--connections must be positive")
        emit(dense_sample(binary, loader, args.duration_ms, args.connections))
    except (Failure, OSError, subprocess.SubprocessError) as exc:
        print(f"perfloop_pubsub_dense: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
