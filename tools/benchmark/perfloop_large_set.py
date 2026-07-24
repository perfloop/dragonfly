#!/usr/bin/env python3
"""Exercise large raw RESP SETs with Redis's standard redis-benchmark client.

This measurement-only helper starts the locally built server, consumes the standard
client's CSV report, and emits one JSONL row per requested metric. Its --check mode
covers fragmented and pipelined raw frames so a change to request-buffer ownership
must preserve wire-level SET/GET semantics.
"""

import argparse
import csv
import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Final


VALUE_SIZE: Final = 1 << 20
REQUEST_COUNT: Final = 512
KEY_COUNT: Final = 16
RUN_DIR_NAME: Final = ".perfloop-large-set"


def repo_root() -> Path:
    # This file is copied to PERFLOOP_BENCH_BIN.  The controller invokes it from
    # the measured checkout, so __file__ is intentionally not used to locate it.
    return Path.cwd()


def command_frame(*parts: bytes) -> bytes:
    frame = bytearray(f"*{len(parts)}\r\n".encode())
    for part in parts:
        frame.extend(f"${len(part)}\r\n".encode())
        frame.extend(part)
        frame.extend(b"\r\n")
    return bytes(frame)


class RespReader:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buffer = bytearray()

    def _fill(self, minimum: int) -> None:
        while len(self.buffer) < minimum:
            chunk = self.sock.recv(64 * 1024)
            if not chunk:
                raise RuntimeError("server closed the RESP connection")
            self.buffer.extend(chunk)

    def _line(self) -> bytes:
        while True:
            pos = self.buffer.find(b"\r\n")
            if pos >= 0:
                line = bytes(self.buffer[:pos])
                del self.buffer[: pos + 2]
                return line
            self._fill(len(self.buffer) + 1)

    def simple(self) -> bytes:
        line = self._line()
        if not line.startswith(b"+"):
            raise RuntimeError(f"expected a simple-string response, got {line[:100]!r}")
        return line[1:]

    def bulk(self) -> bytes:
        line = self._line()
        if not line.startswith(b"$"):
            raise RuntimeError(f"expected a bulk-string response, got {line[:100]!r}")
        length = int(line[1:])
        if length < 0:
            raise RuntimeError("expected a present bulk-string response")
        self._fill(length + 2)
        value = bytes(self.buffer[:length])
        if self.buffer[length : length + 2] != b"\r\n":
            raise RuntimeError("bulk-string response has no CRLF terminator")
        del self.buffer[: length + 2]
        return value


def send_set(sock: socket.socket, key: bytes, value: bytes) -> None:
    sock.sendall(command_frame(b"SET", key, value))
    if RespReader(sock).simple() != b"OK":
        raise RuntimeError("SET did not return OK")


def get_value(sock: socket.socket, key: bytes) -> bytes:
    sock.sendall(command_frame(b"GET", key))
    return RespReader(sock).bulk()


def patterned_value(size: int, seed: int) -> bytes:
    return bytes((index + seed) % 256 for index in range(size))


def choose_port() -> int:
    # Dragonfly does not expose the port selected by --port=-1 to this client.
    # Reserving an ephemeral loopback port immediately before exec makes collision
    # vanishingly unlikely while keeping independent controller samples isolated.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class Server:
    def __init__(self, root: Path, run_dir: Path):
        self.root = root
        self.run_dir = run_dir
        self.port = choose_port()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_path = run_dir / "server.log"
        self.log_file = None

    def __enter__(self) -> "Server":
        binary = self.root / ".perfloop-build" / "dragonfly"
        if not binary.is_file():
            raise RuntimeError(f"missing server binary: {binary}")

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
                    conn.sendall(command_frame(b"PING"))
                    if RespReader(conn).simple() == b"PONG":
                        return self
            except OSError:
                time.sleep(0.05)
        self.log_file.flush()
        raise RuntimeError(
            "Dragonfly did not become ready:\n"
            f"{self.log_path.read_text(errors='replace')}"
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


def fresh_run_dir(root: Path) -> Path:
    run_dir = root / RUN_DIR_NAME
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir()
    return run_dir


def check_fragmented_set(port: int) -> None:
    value = patterned_value(256 * 1024, 17)
    request = command_frame(b"SET", b"fragmented", value)
    with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
        # Deliberately separate the header and many value chunks.  The small
        # pauses make the request cross several server reads rather than merely
        # relying on TCP packetization to do so.
        for offset in range(0, len(request), 8191):
            conn.sendall(request[offset : offset + 8191])
            time.sleep(0.001)
        reader = RespReader(conn)
        if reader.simple() != b"OK":
            raise RuntimeError("fragmented SET did not return OK")
        conn.sendall(command_frame(b"GET", b"fragmented"))
        if reader.bulk() != value:
            raise RuntimeError("fragmented SET value did not survive GET")


def check_pipeline_lifetime(port: int) -> None:
    values = {
        f"pipeline:{index}".encode(): patterned_value(32 * 1024, index) for index in range(12)
    }
    request = b"".join(command_frame(b"SET", key, value) for key, value in values.items())
    with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
        conn.sendall(request)
        reader = RespReader(conn)
        for _ in values:
            if reader.simple() != b"OK":
                raise RuntimeError("pipelined SET did not return OK")
        for key, value in values.items():
            conn.sendall(command_frame(b"GET", key))
            if reader.bulk() != value:
                raise RuntimeError(f"pipelined SET value for {key!r} did not survive GET")

        replacement = patterned_value(64 * 1024, 91)
        conn.sendall(command_frame(b"SET", b"pipeline:0", replacement))
        if reader.simple() != b"OK":
            raise RuntimeError("large SET overwrite did not return OK")
        conn.sendall(command_frame(b"GET", b"pipeline:0"))
        if reader.bulk() != replacement:
            raise RuntimeError("large SET overwrite did not survive GET")


def run_check(root: Path) -> None:
    run_dir = fresh_run_dir(root)
    with Server(root, run_dir) as server:
        check_fragmented_set(server.port)
        check_pipeline_lifetime(server.port)
    print("large-set correctness check passed")


def require_clean_benchmark(report_path: Path) -> tuple[float, float]:
    try:
        with report_path.open(newline="") as report_file:
            rows = list(csv.DictReader(report_file))
    except (OSError, csv.Error) as error:
        raise RuntimeError(f"could not parse redis-benchmark CSV report: {error}") from error

    set_rows = [row for row in rows if row.get("test") == "SET"]
    if len(set_rows) != 1:
        raise RuntimeError(f"expected one SET CSV result, got {len(set_rows)}: {rows!r}")

    try:
        ops_per_sec = float(set_rows[0]["rps"])
        p99_ms = float(set_rows[0]["p99_latency_ms"])
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError(f"unexpected redis-benchmark CSV result: {set_rows[0]!r}") from error

    if ops_per_sec <= 0 or p99_ms < 0:
        raise RuntimeError(f"invalid redis-benchmark metrics: ops/sec={ops_per_sec}, p99={p99_ms}")
    return ops_per_sec, p99_ms


def run_sample(root: Path, workload: str) -> None:
    run_dir = fresh_run_dir(root)
    bench = root / ".perfloop-redis-src" / "src" / "redis-benchmark"
    if not bench.is_file():
        raise RuntimeError(f"missing redis-benchmark binary: {bench}")

    pipeline = 1 if workload == "single" else 16
    metric_prefix = f"large_resp_set_{workload}"
    report_path = run_dir / "redis-benchmark.csv"
    stderr_path = run_dir / "redis-benchmark.stderr"

    with Server(root, run_dir) as server:
        with report_path.open("w", newline="") as output, stderr_path.open("wb") as errors:
            result = subprocess.run(
                [
                    str(bench),
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(server.port),
                    "-c",
                    "1",
                    "-n",
                    str(REQUEST_COUNT),
                    "-P",
                    str(pipeline),
                    "-d",
                    str(VALUE_SIZE),
                    "-r",
                    str(KEY_COUNT),
                    "--seed",
                    "20260308",
                    "-t",
                    "set",
                    "--csv",
                ],
                cwd=root,
                stdout=output,
                stderr=errors,
                timeout=180,
                check=False,
            )
        if result.returncode != 0:
            raise RuntimeError(
                f"redis-benchmark exited {result.returncode}:\n"
                f"stdout:\n{report_path.read_text(errors='replace')}\n"
                f"stderr:\n{stderr_path.read_text(errors='replace')}"
            )
        ops_per_sec, p99_ms = require_clean_benchmark(report_path)

    print(
        json.dumps(
            {"metric": f"{metric_prefix}_ops_per_sec", "value": ops_per_sec}, allow_nan=False
        )
    )
    print(json.dumps({"metric": f"{metric_prefix}_p99_ms", "value": p99_ms}, allow_nan=False))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", choices=("check", "single", "pipeline"), required=True)
    args = parser.parse_args()

    try:
        root = repo_root()
        if args.workload == "check":
            run_check(root)
        else:
            run_sample(root, args.workload)
    except (OSError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"large SET harness failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
