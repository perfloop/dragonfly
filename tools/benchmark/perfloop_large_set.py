#!/usr/bin/env python3
"""Measure server-side copied bytes for a standard pipelined RESP SET workload.

The load generator is Redis' standard ``redis-benchmark`` client.  It uses the
same 30-request pipeline and 100,000-key space as this repository's documented
memtier Kubernetes benchmark, while sampling ordinary 8 KiB, 16 KiB, 32 KiB,
and 64 KiB SET payloads.  It does not send handcrafted RESP frames or a
capacity-training warmup.  The fresh-connection workload is a separate control
for the fragmented large-bulk fallback.
"""

import argparse
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Final


PRIMARY_VALUE_SIZES: Final = (8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024)
PRIMARY_REQUESTS_PER_SIZE: Final = 1200
# A normal long-lived client isolates a per-connection receive-buffer metric.
PRIMARY_CONNECTIONS: Final = 1
PIPELINE_DEPTH: Final = 30
KEYSPACE: Final = 100_000
FRAGMENTED_CONTROL_CONNECTIONS: Final = 8
RUN_DIR_NAME: Final = ".perfloop-large-set"


class Server:
    def __init__(self, root: Path, run_dir: Path):
        self.root = root
        self.run_dir = run_dir
        self.port = choose_port()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_path = run_dir / "server.log"
        self.log_file = None
        self.counter_path = run_dir / "memcpy-counter.bin"
        self.copy_bytes: int | None = None

    def __enter__(self) -> "Server":
        binary = self.root / ".perfloop-build" / "dragonfly"
        counter_library = self.root / ".perfloop-build" / "perfloop_memcpy_counter.so"
        if not binary.is_file():
            raise RuntimeError(f"missing server binary: {binary}")
        if not counter_library.is_file():
            raise RuntimeError(f"missing memcpy counter library: {counter_library}")

        env = os.environ.copy()
        preload = env.get("LD_PRELOAD")
        env["LD_PRELOAD"] = f"{counter_library}:{preload}" if preload else str(counter_library)
        env["PERFLOOP_MEMCPY_COUNTER_PATH"] = str(self.counter_path)

        self.log_file = self.log_path.open("wb")
        self.process = subprocess.Popen(
            [
                str(binary),
                f"--port={self.port}",
                "--bind=127.0.0.1",
                "--proactor_threads=1",
                "--num_shards=1",
                "--primary_port_http_enabled=false",
                "--dbfilename=",
                "--logtostderr",
            ],
            cwd=self.root,
            env=env,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log_file.flush()
                raise RuntimeError(
                    f"Dragonfly exited during startup ({self.process.returncode}):\n"
                    f"{self.log_path.read_text(errors='replace')}"
                )
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2) as conn:
                    conn.sendall(b"PING\r\n")
                    if conn.recv(16) == b"+PONG\r\n":
                        return self
            except OSError:
                time.sleep(0.05)
        self.log_file.flush()
        raise RuntimeError(
            "Dragonfly did not become ready:\n" f"{self.log_path.read_text(errors='replace')}"
        )

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.log_file is not None:
            self.log_file.close()
        try:
            _, self.copy_bytes = struct.unpack("<QQ", self.counter_path.read_bytes())
        except (OSError, struct.error) as error:
            raise RuntimeError(f"could not read memcpy counter: {error}") from error


def choose_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def fresh_run_dir(root: Path) -> Path:
    run_dir = root / RUN_DIR_NAME
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir()
    return run_dir


def run_redis_benchmark(
    root: Path,
    port: int,
    *,
    connections: int,
    requests: int,
    pipeline: int,
    data_size: int,
) -> None:
    binary = root / ".perfloop-redis-src" / "src" / "redis-benchmark"
    if not binary.is_file():
        raise RuntimeError(f"missing standard benchmark binary: {binary}")

    result = subprocess.run(
        [
            str(binary),
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-c",
            str(connections),
            "-n",
            str(requests),
            "-P",
            str(pipeline),
            "-d",
            str(data_size),
            "-t",
            "set",
            "-r",
            str(KEYSPACE),
            "-q",
        ],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(f"redis-benchmark failed ({result.returncode}):\n{result.stdout[-4000:]}")


def measure_standard_range(root: Path) -> float:
    with Server(root, fresh_run_dir(root)) as server:
        for data_size in PRIMARY_VALUE_SIZES:
            run_redis_benchmark(
                root,
                server.port,
                connections=PRIMARY_CONNECTIONS,
                requests=PRIMARY_REQUESTS_PER_SIZE,
                pipeline=PIPELINE_DEPTH,
                data_size=data_size,
            )
    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    return server.copy_bytes / (len(PRIMARY_VALUE_SIZES) * PRIMARY_REQUESTS_PER_SIZE)


def measure_fragmented_control(root: Path) -> float:
    with Server(root, fresh_run_dir(root)) as server:
        run_redis_benchmark(
            root,
            server.port,
            connections=FRAGMENTED_CONTROL_CONNECTIONS,
            requests=FRAGMENTED_CONTROL_CONNECTIONS,
            pipeline=1,
            data_size=1024 * 1024,
        )
    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    return server.copy_bytes / FRAGMENTED_CONTROL_CONNECTIONS


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", choices=("standard-range", "fragmented-1m"), required=True)
    args = parser.parse_args()

    try:
        root = Path.cwd()
        if args.workload == "standard-range":
            metric = "standard_pipelined_8k_64k_resp_set_copy_bytes_per_op"
            value = measure_standard_range(root)
        else:
            metric = "fresh_connection_1m_resp_set_copy_bytes_per_op"
            value = measure_fragmented_control(root)
        print(json.dumps({"metric": metric, "value": value}, allow_nan=False))
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"large SET harness failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
