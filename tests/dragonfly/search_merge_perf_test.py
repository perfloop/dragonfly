# Copyright 2026, DragonflyDB authors.  All rights reserved.
#
# Reproducible integration coverage for the ordered scored-search merge path.
# The large tests are intentionally opt-in: they seed enough documents on each
# shard to make the coordinator's merge work observable.

import argparse
import glob
import json
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import subprocess
import time

import pytest
import redis
import requests


INDEX = "merge_idx"
KEY_PREFIX = "bench:"
PRIMARY_SHARDS = 15
PRIMARY_DOCS_PER_SHARD = 2048
PRIMARY_OFFSET = 1024
PRIMARY_LIMIT = 128
PROFILE_ITERATIONS = 1500
TRACKED_ALLOCATION_MIN = 16 * 1024
TRACKED_ALLOCATION_MAX = 1024 * 1024


def _dragonfly_binary() -> Path:
    configured = os.environ.get("DRAGONFLY_PATH")
    if configured:
        path = Path(configured).resolve()
    else:
        path = Path(__file__).resolve().parents[2] / "build-dbg" / "dragonfly"
    assert path.is_file(), f"Dragonfly binary does not exist: {path}"
    return path


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class DragonflyServer:
    """A minimal server owner for tests that need a profile/log from one process."""

    def __init__(self, binary: Path, work_dir: Path, shard_count: int):
        self.work_dir = work_dir
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.work_dir / "dragonfly.log"
        self._log = self.log_path.open("w", encoding="utf-8")
        self.port = _reserve_port()
        threads = 1 if shard_count == 1 else shard_count + 1
        args = [
            str(binary),
            f"--port={self.port}",
            f"--proactor_threads={threads}",
            f"--num_shards={shard_count}",
            "--maxmemory=5gb",
            "--dbfilename=",
            f"--dir={self.work_dir}",
            f"--shard_round_robin_prefix={KEY_PREFIX}",
            "--alsologtostderr",
        ]
        self.proc = subprocess.Popen(args, stdout=self._log, stderr=subprocess.STDOUT)
        self.client = redis.Redis(
            host="127.0.0.1",
            port=self.port,
            decode_responses=False,
            socket_connect_timeout=0.2,
            socket_timeout=5,
        )

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                self._log.flush()
                raise AssertionError(
                    f"Dragonfly exited with {self.proc.returncode}:\n{self.log_path.read_text()}"
                )
            try:
                if self.client.ping():
                    return
            except redis.RedisError:
                time.sleep(0.05)
        self._log.flush()
        raise AssertionError(f"Dragonfly did not become ready:\n{self.log_path.read_text()}")

    def close(self) -> None:
        try:
            self.client.execute_command("SHUTDOWN", "NOSAVE")
        except (OSError, redis.RedisError):
            pass
        finally:
            self.client.close()

        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self._log.close()


def _start_server(tmp_path: Path, name: str, shard_count: int) -> DragonflyServer:
    return DragonflyServer(_dragonfly_binary(), tmp_path / name, shard_count)


def _create_text_index(client: redis.Redis) -> None:
    assert client.execute_command(
        "FT.CREATE",
        INDEX,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        KEY_PREFIX,
        "SCHEMA",
        "content",
        "TEXT",
    ) in (b"OK", "OK")


def _score_text(doc_id: int) -> str:
    # alpha and beta have the same document frequency and per-document term
    # frequency. Alternating them keeps the benchmark input runtime-varying
    # without changing the number of candidates or the requested page shape.
    tier = doc_id % 8
    return "alpha " * (tier + 1) + "beta " * (tier + 1) + "filler " * (8 - tier)


def _seed_text_index(client: redis.Redis, shard_count: int, docs_per_shard: int) -> int:
    _create_text_index(client)
    document_count = shard_count * docs_per_shard
    for first in range(0, document_count, 512):
        pipe = client.pipeline(transaction=False)
        for doc_id in range(first, min(first + 512, document_count)):
            pipe.hset(
                f"{KEY_PREFIX}{doc_id:05d}",
                mapping={"content": _score_text(doc_id)},
            )
        pipe.execute()
    return document_count


def _scored_search_command(term: bytes, offset: int, limit: int) -> tuple:
    return (
        "FT.SEARCH",
        INDEX,
        term,
        "NOCONTENT",
        "WITHSCORES",
        "SCORER",
        "BM25STD",
        "LIMIT",
        str(offset),
        str(limit),
    )


def _run_scored_search(
    client: redis.Redis, term: bytes, offset: int, limit: int, expected_total: int
) -> int:
    reply = client.execute_command(*_scored_search_command(term, offset, limit))
    assert int(reply[0]) == expected_total
    assert len(reply) == 1 + 2 * limit
    # Parse data from the response on every invocation. This makes the timed
    # operation include a real reply boundary rather than a discarded request.
    return int(reply[0]) ^ len(reply[1]) ^ int(float(reply[2]) * 1_000_000_000)


def _search_reply_symbol(binary: Path) -> tuple[int, int]:
    symbols = subprocess.run(
        ["nm", "-S", "-n", "--demangle", str(binary)],
        check=True,
        text=True,
        capture_output=True,
    ).stdout
    candidates = []
    for line in symbols.splitlines():
        fields = line.split(None, 3)
        if (
            len(fields) == 4
            and fields[3].startswith("dfly::(anonymous namespace)::SearchReply(")
            and "[clone .cold]" not in fields[3]
        ):
            candidates.append((int(fields[0], 16), int(fields[1], 16)))
    assert len(candidates) == 1, f"could not find one SearchReply symbol: {candidates}"
    return candidates[0]


def _search_reply_cpu_profile(binary: Path, profile: Path, operations: int) -> dict:
    # -symbolize=none keeps this bounded: only SearchReply's own address range
    # needs attribution, rather than symbolizing a large production executable.
    raw = subprocess.run(
        ["go", "tool", "pprof", "-symbolize=none", "-raw", str(profile)],
        check=True,
        text=True,
        capture_output=True,
    ).stdout
    symbol_start, symbol_size = _search_reply_symbol(binary)

    mappings = {}
    binary_mapping = None
    mapping_re = re.compile(
        r"^(\d+):\s+(0x[0-9a-f]+)/0x[0-9a-f]+/(0x[0-9a-f]+)(?:\s+(.*))?$"
    )
    for line in raw.splitlines():
        match = mapping_re.match(line)
        if not match:
            continue
        mapping_id = int(match.group(1))
        mappings[mapping_id] = (int(match.group(2), 16), int(match.group(3), 16))
        if (match.group(4) or "").strip().endswith(binary.name):
            binary_mapping = mapping_id
    assert binary_mapping is not None, f"pprof did not map {binary.name}"

    in_search_reply = {}
    location_re = re.compile(r"^\s+(\d+):\s+(0x[0-9a-f]+)\s+M=(\d+)")
    for line in raw.splitlines():
        match = location_re.match(line)
        if not match:
            continue
        location_id, address, mapping_id = (
            int(match.group(1)),
            int(match.group(2), 16),
            int(match.group(3)),
        )
        if mapping_id != binary_mapping:
            in_search_reply[location_id] = False
            continue
        mapping_start, mapping_offset = mappings[mapping_id]
        virtual_address = address - mapping_start + mapping_offset
        in_search_reply[location_id] = symbol_start <= virtual_address < symbol_start + symbol_size

    inclusive_ns = 0
    inclusive_samples = 0
    self_ns = 0
    sample_re = re.compile(r"^\s+(\d+)\s+(\d+):\s+([0-9 ]+)$")
    for line in raw.splitlines():
        match = sample_re.match(line)
        if not match:
            continue
        count = int(match.group(1))
        nanoseconds = int(match.group(2))
        locations = [int(value) for value in match.group(3).split()]
        if in_search_reply.get(locations[0], False):
            self_ns += nanoseconds
        if any(in_search_reply.get(location, False) for location in locations):
            inclusive_ns += nanoseconds
            inclusive_samples += count

    assert inclusive_samples > 0, "CPU profile did not observe SearchReply"
    return {
        "search_reply_cpu_us_per_op": inclusive_ns / operations / 1_000,
        "search_reply_cpu_samples": inclusive_samples,
        "search_reply_self_cpu_us_per_op": self_ns / operations / 1_000,
    }


def _capture_cpu_profile(
    server: DragonflyServer,
    operation,
    operations: int,
) -> dict:
    shutil.rmtree("/tmp/profile", ignore_errors=True)
    on = requests.get(f"http://127.0.0.1:{server.port}/profilez?profile=on", timeout=30)
    assert on.status_code == 200
    for _ in range(operations):
        operation()
    off = requests.get(
        f"http://127.0.0.1:{server.port}/profilez?profile=off",
        timeout=30,
        allow_redirects=False,
    )
    assert off.status_code == 301
    profiles = [Path(path) for path in glob.glob("/tmp/profile/*.prof")]
    assert profiles, "profilez did not produce a CPU profile"
    return _search_reply_cpu_profile(
        _dragonfly_binary(), max(profiles, key=lambda candidate: candidate.stat().st_mtime), operations
    )


def _store_profile_result(values: dict) -> None:
    output = os.environ.get("PERFLOOP_SEARCH_MERGE_PROFILE")
    assert output, "PERFLOOP_SEARCH_MERGE_PROFILE must name the profile result file"
    path = Path(output)
    prior = json.loads(path.read_text()) if path.exists() else {}
    prior.update(values)
    path.write_text(json.dumps(prior, sort_keys=True))


def _allocation_blocks(log_path: Path) -> tuple[int, int, int]:
    bytes_in_search_reply = 0
    allocations_in_search_reply = 0
    tracked_allocations = 0
    current_size = None
    current_stack = []
    allocation_re = re.compile(r"allocation_tracker\.cc:88\] Allocating (\d+) bytes")

    def consume_current() -> None:
        nonlocal bytes_in_search_reply, allocations_in_search_reply, tracked_allocations
        if current_size is None:
            return
        tracked_allocations += 1
        if "SearchReply" in "\n".join(current_stack):
            bytes_in_search_reply += current_size
            allocations_in_search_reply += 1

    for line in log_path.read_text().splitlines():
        match = allocation_re.search(line)
        if match:
            consume_current()
            current_size = int(match.group(1))
            current_stack = [line]
        elif current_size is not None and "allocation_tracker.cc:" in line:
            consume_current()
            current_size = None
            current_stack = []
        elif current_size is not None:
            current_stack.append(line)
    consume_current()
    return bytes_in_search_reply, allocations_in_search_reply, tracked_allocations


def _benchmark_shape(
    benchmark,
    tmp_path: Path,
    name: str,
    shard_count: int,
    docs_per_shard: int,
    offset: int,
    limit: int,
    profile: bool,
) -> None:
    server = _start_server(tmp_path, name, shard_count)
    try:
        total = _seed_text_index(server.client, shard_count, docs_per_shard)
        terms = (b"alpha", b"beta")
        request_number = 0
        checksum = 0

        def operation() -> int:
            nonlocal request_number, checksum
            checksum ^= _run_scored_search(
                server.client,
                terms[request_number % len(terms)],
                offset,
                limit,
                total,
            )
            request_number += 1
            return checksum

        for _ in range(20):
            operation()
        benchmark(operation)
        assert request_number > 20

        if profile:
            _store_profile_result(_capture_cpu_profile(server, operation, PROFILE_ITERATIONS))
    finally:
        server.close()


@pytest.mark.large
def test_ordered_score_merge_primary_profiled(benchmark, tmp_path):
    _benchmark_shape(
        benchmark,
        tmp_path,
        "primary",
        PRIMARY_SHARDS,
        PRIMARY_DOCS_PER_SHARD,
        PRIMARY_OFFSET,
        PRIMARY_LIMIT,
        profile=True,
    )


@pytest.mark.large
@pytest.mark.parametrize(
    "name, shard_count, docs_per_shard, offset, limit",
    [
        ("four_shards", 4, 2048, 1024, 128),
        ("low_candidates", 15, 256, 1024, 128),
        ("deep_page", 8, 4608, 4096, 256),
    ],
)
def test_ordered_score_merge_shape(
    benchmark,
    tmp_path,
    name: str,
    shard_count: int,
    docs_per_shard: int,
    offset: int,
    limit: int,
):
    _benchmark_shape(
        benchmark,
        tmp_path,
        name,
        shard_count,
        docs_per_shard,
        offset,
        limit,
        profile=False,
    )


@pytest.mark.large
def test_search_reply_allocation_profile(tmp_path):
    server = _start_server(tmp_path, "allocation-profile", PRIMARY_SHARDS)
    try:
        total = _seed_text_index(server.client, PRIMARY_SHARDS, PRIMARY_DOCS_PER_SHARD)
        command = _scored_search_command(b"alpha", PRIMARY_OFFSET, PRIMARY_LIMIT)
        for _ in range(10):
            _run_scored_search(server.client, b"alpha", PRIMARY_OFFSET, PRIMARY_LIMIT, total)

        assert server.client.execute_command(
            "MEMORY",
            "TRACK",
            "ADD",
            str(TRACKED_ALLOCATION_MIN),
            str(TRACKED_ALLOCATION_MAX),
            "1",
        ) in (b"OK", "OK")
        reply = server.client.execute_command(*command)
        assert int(reply[0]) == total
        assert server.client.execute_command("MEMORY", "TRACK", "CLEAR") in (b"OK", "OK")
    finally:
        server.close()

    allocation_bytes, allocation_count, tracked_allocations = _allocation_blocks(server.log_path)
    assert tracked_allocations > 0, "allocation tracker did not record the measured query"
    _store_profile_result(
        {
            "search_reply_large_alloc_bytes_per_op": allocation_bytes,
            "search_reply_large_allocations_per_op": allocation_count,
        }
    )


def _create_mixed_index(client: redis.Redis) -> None:
    assert client.execute_command(
        "FT.CREATE",
        INDEX,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        KEY_PREFIX,
        "SCHEMA",
        "content",
        "TEXT",
        "rank",
        "NUMERIC",
        "SORTABLE",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "1",
        "DISTANCE_METRIC",
        "L2",
    ) in (b"OK", "OK")


def _seed_mixed_index(client: redis.Redis, shard_count: int, docs_per_shard: int) -> int:
    _create_mixed_index(client)
    document_count = shard_count * docs_per_shard
    for first in range(0, document_count, 128):
        pipe = client.pipeline(transaction=False)
        for doc_id in range(first, min(first + 128, document_count)):
            pipe.hset(
                f"{KEY_PREFIX}{doc_id:05d}",
                mapping={
                    "content": _score_text(doc_id),
                    "rank": str((doc_id * 17) % 1000),
                    "vec": struct.pack("<f", float(doc_id)),
                },
            )
        pipe.execute()
    return document_count


def _pair(tmp_path: Path, name: str) -> tuple[DragonflyServer, DragonflyServer]:
    reference = _start_server(tmp_path, f"{name}-reference", 1)
    distributed = _start_server(tmp_path, f"{name}-distributed", 4)
    return reference, distributed


def test_scored_merge_differential_and_fallback_branches(tmp_path):
    reference, distributed = _pair(tmp_path, "fallbacks")
    try:
        total = _seed_mixed_index(reference.client, 4, 200)
        assert _seed_mixed_index(distributed.client, 4, 200) == total

        scored = (
            "FT.SEARCH",
            INDEX,
            "alpha",
            "NOCONTENT",
            "WITHSCORES",
            "SCORER",
            "BM25STD",
            "LIMIT",
            "7",
            "25",
        )
        normalized = (*scored[:6], "BM25STD.NORM", *scored[7:])
        # Equal-frequency tiers exercise score/key ties and a nonzero offset.
        assert reference.client.execute_command(*scored) == distributed.client.execute_command(*scored)
        assert reference.client.execute_command(*normalized) == distributed.client.execute_command(*normalized)

        sortby = (
            "FT.SEARCH",
            INDEX,
            "alpha",
            "NOCONTENT",
            "WITHSCORES",
            "WITHSORTKEYS",
            "SORTBY",
            "rank",
            "DESC",
            "LIMIT",
            "7",
            "25",
        )
        assert reference.client.execute_command(*sortby) == distributed.client.execute_command(*sortby)

        query_vector = struct.pack("<f", 330.123)
        knn = (
            "FT.SEARCH",
            INDEX,
            b"*=>[KNN 40 @vec $query_vec AS distance]",
            "PARAMS",
            "2",
            "query_vec",
            query_vector,
            "DIALECT",
            "2",
            "NOCONTENT",
            "WITHSCORES",
            "LIMIT",
            "0",
            "15",
        )
        # The fractional vector query deliberately avoids equal-distance ties.
        assert reference.client.execute_command(*knn) == distributed.client.execute_command(*knn)

        knn_with_sortby = (
            *knn[:-3],
            "WITHSORTKEYS",
            "SORTBY",
            "rank",
            "DESC",
            "LIMIT",
            "0",
            "15",
        )
        assert reference.client.execute_command(*knn_with_sortby) == distributed.client.execute_command(
            *knn_with_sortby
        )

        # No scorer and no WITHSCORES intentionally leaves local result order
        # unmarked. This assertion pins the existing flattened fallback rather
        # than allowing an ordered-score optimization to route this branch.
        unscored = distributed.client.execute_command(
            "FT.SEARCH", INDEX, "alpha", "NOCONTENT", "LIMIT", "7", "25"
        )
        assert int(unscored[0]) == total
        expected = [f"{KEY_PREFIX}{doc_id:05d}".encode() for doc_id in range(28, 28 + 4 * 25, 4)]
        assert unscored[1:] == expected
    finally:
        reference.close()
        distributed.close()


def test_scored_merge_expired_document_differential(tmp_path):
    reference, distributed = _pair(tmp_path, "expired")
    try:
        total = _seed_text_index(reference.client, 4, 100)
        assert _seed_text_index(distributed.client, 4, 100) == total
        expired_key = f"{KEY_PREFIX}00031"
        assert reference.client.pexpire(expired_key, 20)
        assert distributed.client.pexpire(expired_key, 20)
        time.sleep(0.08)

        # Do not use NOCONTENT here: the rendering path must load and reject the
        # expired entry before score/key selection is returned to the client.
        command = (
            "FT.SEARCH",
            INDEX,
            "alpha",
            "WITHSCORES",
            "SCORER",
            "BM25STD",
            "LIMIT",
            "0",
            "100",
        )
        one_shard = reference.client.execute_command(*command)
        multi_shard = distributed.client.execute_command(*command)
        assert one_shard == multi_shard
        assert int(multi_shard[0]) == total - 1
        assert expired_key.encode() not in multi_shard[1::3]
    finally:
        reference.close()
        distributed.close()


def _emit_profile(path: Path) -> None:
    result = json.loads(path.read_text())
    metrics = (
        "search_reply_cpu_us_per_op",
        "search_reply_large_alloc_bytes_per_op",
        "search_reply_large_allocations_per_op",
    )
    for metric in metrics:
        assert metric in result, f"missing profile metric {metric}"
        print(json.dumps({"metric": metric, "value": result[metric]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit-profile", type=Path, required=True)
    args = parser.parse_args()
    _emit_profile(args.emit_profile)
