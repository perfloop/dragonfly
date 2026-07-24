#!/usr/bin/env python3
"""Exercise complete 24 KiB raw RESP SET frames against a local Dragonfly server.

This measurement-only helper uses a single raw RESP connection so the complete-frame
fast path is exercised rather than a client-side chunking policy. It emits one JSONL
row per requested metric. Its --check mode covers fragmented and pipelined raw frames
so a change to request-buffer ownership must preserve wire-level SET/GET semantics.
Its --copy-profile mode runs a controlled large-SET experiment with a measurement-only
memcpy interposer.
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


# A 24 KiB bulk causes Dragonfly's parser-hinted receive buffer to round up to
# 32 KiB. Subsequent 24 KiB RESP frames fit in that buffer as complete bulks;
# a 32 KiB bulk plus RESP framing spills into the next receive and exercises
# only the fragmented fallback.
VALUE_SIZE: Final = 24 * 1024
REQUEST_COUNT: Final = 512
KEY_COUNT: Final = 16
COPY_PROFILE_SET_COUNT: Final = 16
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
    def __init__(self, root: Path, run_dir: Path, copy_counter: bool = False):
        self.root = root
        self.run_dir = run_dir
        self.copy_counter = copy_counter
        self.port = choose_port()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_path = run_dir / "server.log"
        self.log_file = None
        self.counter_path: Path | None = None
        self.copy_bytes: int | None = None
        self.copy_calls: int | None = None

    def __enter__(self) -> "Server":
        binary = self.root / ".perfloop-build" / "dragonfly"
        if not binary.is_file():
            raise RuntimeError(f"missing server binary: {binary}")

        env = None
        if self.copy_counter:
            counter_library = self.root / ".perfloop-build" / "perfloop_memcpy_counter.so"
            if not counter_library.is_file():
                raise RuntimeError(f"missing memcpy counter library: {counter_library}")
            self.counter_path = self.run_dir / "memcpy-counter.bin"
            env = os.environ.copy()
            preload = env.get("LD_PRELOAD")
            env["LD_PRELOAD"] = (
                f"{counter_library}:{preload}" if preload else str(counter_library)
            )
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
        if self.counter_path is not None:
            try:
                raw_counter = self.counter_path.read_bytes()
                self.copy_calls, self.copy_bytes = struct.unpack("<QQ", raw_counter)
            except (OSError, struct.error) as error:
                raise RuntimeError(f"could not read memcpy counter: {error}") from error


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


def collect_copy_bytes(root: Path, run_dir: Path, set_count: int) -> int:
    run_dir.mkdir()
    server = Server(root, run_dir, copy_counter=True)
    with server:
        if set_count:
            with socket.create_connection(("127.0.0.1", server.port), timeout=5) as conn:
                for index in range(set_count):
                    send_set(
                        conn,
                        f"copy-profile:{index % KEY_COUNT}".encode(),
                        patterned_value(VALUE_SIZE, index),
                    )
    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    return server.copy_bytes


def run_copy_profile(root: Path) -> None:
    run_dir = fresh_run_dir(root)
    idle_bytes = collect_copy_bytes(root, run_dir / "idle", 0)
    large_set_bytes = collect_copy_bytes(root, run_dir / "large-set", COPY_PROFILE_SET_COUNT)
    delta = large_set_bytes - idle_bytes
    minimum_delta = COPY_PROFILE_SET_COUNT * VALUE_SIZE * 3 // 2
    if delta < minimum_delta:
        raise RuntimeError(
            "large SET copy profile did not observe the parser and large-string copies: "
            f"idle={idle_bytes} large-set={large_set_bytes} delta={delta} "
            f"minimum={minimum_delta}"
        )
    print(
        "large-set copy-profile check passed: "
        f"idle={idle_bytes} large-set={large_set_bytes} delta={delta}"
    )


def percentile_99_ms(samples: list[float]) -> float:
    if not samples:
        raise RuntimeError("no latency samples were collected")
    rank = (99 * len(samples) + 99) // 100 - 1
    return sorted(samples)[rank] * 1000


def run_raw_set_client(port: int, workload: str) -> tuple[float, float]:
    keys = [f"large-set:{index}".encode() for index in range(KEY_COUNT)]
    values = [patterned_value(VALUE_SIZE, 17 + index) for index in range(KEY_COUNT)]
    frames = [command_frame(b"SET", key, value) for key, value in zip(keys, values)]
    pipeline = 1 if workload == "single" else 16
    latencies: list[float] = []

    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        reader = RespReader(conn)
        start = time.perf_counter()
        for offset in range(0, REQUEST_COUNT, pipeline):
            request_count = min(pipeline, REQUEST_COUNT - offset)
            request = b"".join(
                frames[(offset + index) % KEY_COUNT] for index in range(request_count)
            )
            before = time.perf_counter()
            conn.sendall(request)
            for _ in range(request_count):
                if reader.simple() != b"OK":
                    raise RuntimeError("SET did not return OK")
            elapsed = time.perf_counter() - before
            # A pipeline has one observable response interval; divide it across its
            # requests rather than presenting the batch time as per-request latency.
            latencies.extend([elapsed / request_count] * request_count)
        elapsed = time.perf_counter() - start

    if elapsed <= 0:
        raise RuntimeError("raw SET benchmark recorded no elapsed time")
    return REQUEST_COUNT / elapsed, percentile_99_ms(latencies)


def run_sample(root: Path, workload: str) -> None:
    run_dir = fresh_run_dir(root)
    metric_prefix = f"complete_24k_resp_set_{workload}"

    server = Server(root, run_dir, copy_counter=True)
    with server:
        ops_per_sec, p99_ms = run_raw_set_client(server.port, workload)

    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    copy_bytes_per_op = server.copy_bytes / REQUEST_COUNT

    print(
        json.dumps(
            {"metric": f"{metric_prefix}_ops_per_sec", "value": ops_per_sec}, allow_nan=False
        )
    )
    print(json.dumps({"metric": f"{metric_prefix}_p99_ms", "value": p99_ms}, allow_nan=False))
    print(
        json.dumps(
            {"metric": f"{metric_prefix}_copy_bytes_per_op", "value": copy_bytes_per_op},
            allow_nan=False,
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workload", choices=("check", "copy-profile", "single", "pipeline"), required=True
    )
    args = parser.parse_args()

    try:
        root = repo_root()
        if args.workload == "check":
            run_check(root)
        elif args.workload == "copy-profile":
            run_copy_profile(root)
        else:
            run_sample(root, args.workload)
    except (OSError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"large SET harness failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
