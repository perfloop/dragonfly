#!/usr/bin/env python3
"""Convert the native HNSW churn benchmark's JSON rows to proof JSONL."""

import json
import sys
from pathlib import Path

OPS_PER_CHURN = 1024
TIME_TO_NS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


ADD_BENCHMARKS = {
    "copied-add": "BM_HnswDeletedSlotChurnAddCopied",
    "borrowed-add": "BM_HnswDeletedSlotChurnAddBorrowed",
}
KNN_BENCHMARKS = {
    "copied-knn": "BM_HnswDeletedSlotChurnKnnCopied",
    "borrowed-knn": "BM_HnswDeletedSlotChurnKnnBorrowed",
}


def ReadRow(path: str, benchmark_name: str) -> dict:
    payload = json.loads(Path(path).read_text())
    rows = [
        row
        for row in payload["benchmarks"]
        if row["name"] == benchmark_name and row["run_type"] == "iteration"
    ]
    if len(rows) != 1:
        raise ValueError(f"expected one iteration row for {benchmark_name}, got {len(rows)}")
    return rows[0]


def Emit(metric: str, value: float) -> None:
    print(json.dumps({"metric": metric, "value": value}))


def EmitAdd(mode: str, benchmark_json: str) -> None:
    row = ReadRow(benchmark_json, ADD_BENCHMARKS[mode])
    unit = row["time_unit"]
    if unit not in TIME_TO_NS:
        raise ValueError(f"unsupported time unit: {unit}")

    required = ("add_p99_ns", "graph_nodes", "deleted_nodes", "graph_bytes", "resize_retry_count")
    if any(metric not in row for metric in required):
        raise ValueError(f"missing add metrics: {row}")

    prefix = mode.removesuffix("-add")
    scale = TIME_TO_NS[unit]
    Emit(f"hnsw_{prefix}_churn_add_cpu_ns_per_op", row["cpu_time"] * scale / OPS_PER_CHURN)
    Emit(f"hnsw_{prefix}_churn_add_wall_ns_per_op", row["real_time"] * scale / OPS_PER_CHURN)
    Emit(f"hnsw_{prefix}_churn_add_p99_ns", row["add_p99_ns"])
    Emit(f"hnsw_{prefix}_cur_element_count", row["graph_nodes"])
    Emit(f"hnsw_{prefix}_deleted_tombstones", row["deleted_nodes"])
    Emit(f"hnsw_{prefix}_graph_bytes", row["graph_bytes"])
    Emit(f"hnsw_{prefix}_resize_retry_count", row["resize_retry_count"])


def EmitKnn(mode: str, timing_json: str, distance_json: str) -> None:
    benchmark = KNN_BENCHMARKS[mode]
    timing_row = ReadRow(timing_json, benchmark)
    distance_row = ReadRow(distance_json, benchmark)
    unit = timing_row["time_unit"]
    if unit not in TIME_TO_NS:
        raise ValueError(f"unsupported time unit: {unit}")
    if "distance_computations_per_query" not in distance_row:
        raise ValueError(f"missing distance counter: {distance_row}")

    prefix = mode.removesuffix("-knn")
    scale = TIME_TO_NS[unit]
    Emit(f"hnsw_{prefix}_churn_knn_wall_ns_per_query", timing_row["real_time"] * scale)
    Emit(f"hnsw_{prefix}_churn_knn_cpu_ns_per_query", timing_row["cpu_time"] * scale)
    Emit(
        f"hnsw_{prefix}_churn_knn_distance_computations_per_query",
        distance_row["distance_computations_per_query"],
    )


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: hnsw_churn_proof.py MODE BENCHMARK_JSON [DISTANCE_JSON]")

    mode = sys.argv[1]
    if mode in ADD_BENCHMARKS and len(sys.argv) == 3:
        EmitAdd(mode, sys.argv[2])
        return
    if mode in KNN_BENCHMARKS and len(sys.argv) == 4:
        EmitKnn(mode, sys.argv[2], sys.argv[3])
        return
    raise SystemExit("mode and JSON argument count do not match")


if __name__ == "__main__":
    main()
