#!/usr/bin/env python3
# Copyright 2026, DragonflyDB authors.  All rights reserved.
# See LICENSE for licensing terms.
"""Additional neighboring-shape guards for the pub/sub fanout measurement.

This nonshipping companion reuses the delivery-draining workload in
perfloop_pubsub.py. It keeps the sparse target separate from the two distinct
risks introduced by direct owner dispatch: a dense owner layout, where no
empty worker callbacks remain to remove, and a payload/subscriber-heavy layout,
where range construction must not displace message delivery cost.
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from typing import Callable

from perfloop_pubsub import (
    Failure,
    Shape,
    callback_count_sample,
    checked_path,
    emit,
    require,
    throughput_sample,
)


WIDTH_SHAPES = (
    Shape(16, 1, 1, 64),
    Shape(16, 2, 1, 64),
    Shape(16, 4, 1, 64),
    Shape(16, 8, 1, 64),
    Shape(16, 16, 1, 64),
)
HEAVY_SHAPE = Shape(16, 2, 4, 4096)
CALLBACK_WIDTH_SHAPES = tuple(shape for shape in WIDTH_SHAPES)


def width_sample(
    binary: pathlib.Path, loader: pathlib.Path, duration_ms: int, connections: int
) -> dict[str, float]:
    metrics: dict[str, float] = {}
    samples = [
        throughput_sample(binary, loader, shape, duration_ms, connections) for shape in WIDTH_SHAPES
    ]

    def collect(metric: str, reducer: Callable[[list[float]], float]) -> None:
        values = [sample[metric] for sample in samples]
        metrics[f"width_sweep_{metric}"] = sum(values) / len(values)
        metrics[f"width_sweep_{metric}_worst"] = reducer(values)
        for shape, value in zip(WIDTH_SHAPES, values):
            metrics[f"{metric}_{shape.suffix}"] = value

    collect("server_cpu_usec_per_publish", max)
    collect("publish_p50_ms", max)
    collect("publish_p99_ms", max)
    collect("publish_ops_per_sec", min)
    return metrics


def heavy_sample(
    binary: pathlib.Path, loader: pathlib.Path, duration_ms: int, connections: int
) -> dict[str, float]:
    sample = throughput_sample(binary, loader, HEAVY_SHAPE, duration_ms, connections)
    return {
        **sample,
        "pool_threads": float(HEAVY_SHAPE.threads),
    }


def callback_width_sample(binary: pathlib.Path, publications: int) -> dict[str, float]:
    metrics = callback_count_sample(binary, publications, CALLBACK_WIDTH_SHAPES)
    for shape in CALLBACK_WIDTH_SHAPES:
        entries = metrics[f"brief_callbacks_per_publish_{shape.suffix}"]
        require(
            shape.owners <= entries <= shape.threads,
            f"callback entries {entries} fell outside [{shape.owners}, {shape.threads}] for {shape.suffix}",
        )
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("width", "heavy", "callback-width"), required=True)
    parser.add_argument("--dragonfly", required=True)
    parser.add_argument("--loader")
    parser.add_argument("--duration-ms", type=int, default=1000)
    parser.add_argument("--connections", type=int, default=1)
    parser.add_argument("--publications", type=int, default=4)
    args = parser.parse_args()

    try:
        binary = checked_path(args.dragonfly, "Dragonfly binary")
        require(args.duration_ms > 0, "--duration-ms must be positive")
        require(args.connections > 0, "--connections must be positive")
        if args.mode == "callback-width":
            require(args.publications > 0, "--publications must be positive")
            emit(callback_width_sample(binary, args.publications))
            print("callback_width_sweep_ok=1")
        else:
            loader = checked_path(args.loader or "", "native load driver")
            if args.mode == "width":
                emit(width_sample(binary, loader, args.duration_ms, args.connections))
            else:
                emit(heavy_sample(binary, loader, args.duration_ms, args.connections))
    except (Failure, OSError, subprocess.SubprocessError) as exc:
        print(f"perfloop_pubsub_scope: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
