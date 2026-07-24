#!/usr/bin/env python3
"""Exercise complete 24 KiB raw RESP SET frames against a local Dragonfly server.

This measurement-only helper uses a single raw RESP connection so the complete-frame
fast path is exercised rather than a client-side chunking policy. It emits one JSONL
row per requested metric. Deterministic parser and string-family tests are invoked
separately by the proof command for correctness; this helper is measurement-only.
"""

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


def run_raw_set_client(port: int) -> None:
    keys = [f"large-set:{index}".encode() for index in range(KEY_COUNT)]
    values = [patterned_value(VALUE_SIZE, 17 + index) for index in range(KEY_COUNT)]
    frames = [command_frame(b"SET", key, value) for key, value in zip(keys, values)]

    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        reader = RespReader(conn)
        for index in range(REQUEST_COUNT):
            conn.sendall(frames[index % KEY_COUNT])
            if reader.simple() != b"OK":
                raise RuntimeError("SET did not return OK")


def run_sample(root: Path) -> None:
    run_dir = fresh_run_dir(root)
    server = Server(root, run_dir, copy_counter=True)
    with server:
        run_raw_set_client(server.port)

    if server.copy_bytes is None:
        raise RuntimeError("memcpy counter did not report copy bytes")
    copy_bytes_per_op = server.copy_bytes / REQUEST_COUNT
    print(
        json.dumps(
            {"metric": "complete_24k_resp_set_copy_bytes_per_op", "value": copy_bytes_per_op},
            allow_nan=False,
        )
    )


def main() -> int:
    try:
        root = repo_root()
        run_sample(root)
    except (OSError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"large SET harness failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
