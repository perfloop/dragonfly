#!/usr/bin/env python3
"""Measure copied bytes while Dragonfly's native SET benchmark runs.

The workload generator is the repository's ``dfly_bench`` client, not a
hand-written RESP sender.  Its 8 KiB--64 KiB value sweep uses runtime-varying
payload sizes and its pipeline depth matches the 30-request pipeline used by
``tools/benchmark/k8s-benchmark-job.yaml``.  The separate fresh-connection
workload is a control for the fragmented large-bulk fallback.
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


NATIVE_RANGE_OPS: Final = 512
NATIVE_RANGE_PIPELINE: Final = 30
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


def run_native_bench(
    root: Path,
    port: int,
    *,
    connections: int,
    requests_per_connection: int,
    pipeline: int,
    data_size: str,
) -> None:
    binary = root / ".perfloop-build" / "dfly_bench"
    if not binary.is_file():
        raise RuntimeError(f"missing native benchmark binary: {binary}")

    env = os.environ.copy()
    # Match the repository's focused native-test fallback on kernels without io_uring.
    env["FLAGS_force_epoll"] = "true"
    result = subprocess.run(
        [
            str(binary),
            "--h=127.0.0.1",
            f"--p={port}",
            f"--c={connections}",
            f"--n={requests_per_connection}",
            f"--pipeline={pipeline}",
            f"--d={data_size}",
            "--ratio=1:0",
            "--qps=0",
            "--key_dist=S",
            "--key_minimum=0",
            "--key_maximum=65535",
            "--random_data=true",
            "--tcp_nodelay=true",
            "--probe_cluster=false",
        ],
        cwd=root,
        env=env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(f"dfly_bench failed ({result.returncode}):\n{result.stdout[-4000:]}")


def measure_native_range(root: Path) -> float:
    with Server(root, fresh_run_dir(root)) as server:
        run_native_bench(
            root,
            server.port,
            connections=1,
            requests_per_connection=NATIVE_RANGE_OPS,
            pipeline=NATIVE_RANGE_PIPELINE,
            data_size="8192:65536",
        )
    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    return server.copy_bytes / NATIVE_RANGE_OPS


def measure_fragmented_control(root: Path) -> float:
    with Server(root, fresh_run_dir(root)) as server:
        run_native_bench(
            root,
            server.port,
            connections=FRAGMENTED_CONTROL_CONNECTIONS,
            requests_per_connection=1,
            pipeline=1,
            data_size="1048576",
        )
    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    return server.copy_bytes / FRAGMENTED_CONTROL_CONNECTIONS


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", choices=("native-range", "fragmented-1m"), required=True)
    args = parser.parse_args()

    try:
        root = Path.cwd()
        if args.workload == "native-range":
            metric = "native_pipelined_8k_64k_resp_set_copy_bytes_per_op"
            value = measure_native_range(root)
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
