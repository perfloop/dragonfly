#!/usr/bin/env python3
# Copyright 2026, DragonflyDB authors.  All rights reserved.
# See LICENSE for licensing terms.
"""Measurement-chain driver for sparse-owner pub/sub delivery.

Dragonfly has no native pub/sub benchmark that both provisions and drains a
controlled subscriber layout.  This driver creates that layout with raw RESP,
uses the repository build artifacts, consumes every PUBLISH reply and every
subscriber message, and emits proof JSONL for a single controller sample.

The callback-count mode is intentionally diagnostic rather than timed: it
links the adjacent counter into a -finstrument-functions build and counts the
compiled DispatchBrief callback invokers that originated in
ChannelStore::SendMessages. Its sparse-shape option picks an independent,
runtime-selected topology for each sample, so a controller samples the stated
pool-width, owner-count, subscriber-count, and payload-size distribution rather
than a cached fixed configuration. The throughput mode uses the adjacent native
socket load generator because dfly_bench unconditionally selects io_uring on
Linux, while this environment deliberately runs Dragonfly's supported epoll
path.  Neither helper is part of an upstream product change.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import pathlib
import re
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable


CHANNEL = b"perfloop-pubsub"


class Failure(RuntimeError):
    """A workload or observable-result assertion failed."""


class IncompleteResponse(Exception):
    """The RESP buffer needs more network bytes."""


@dataclass(frozen=True)
class RespError:
    message: bytes


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Failure(message)


def encode_command(parts: Iterable[str | bytes | int]) -> bytes:
    encoded = [
        (
            str(part).encode()
            if isinstance(part, int)
            else part.encode() if isinstance(part, str) else part
        )
        for part in parts
    ]
    result = bytearray(f"*{len(encoded)}\r\n".encode())
    for part in encoded:
        result.extend(f"${len(part)}\r\n".encode())
        result.extend(part)
        result.extend(b"\r\n")
    return bytes(result)


def parse_response(buffer: bytearray, position: int = 0) -> tuple[Any, int]:
    if position >= len(buffer):
        raise IncompleteResponse

    kind = buffer[position]
    line_end = buffer.find(b"\r\n", position + 1)
    if line_end < 0:
        raise IncompleteResponse
    line = bytes(buffer[position + 1 : line_end])
    next_position = line_end + 2

    if kind == ord(b"+"):
        return line, next_position
    if kind == ord(b"-"):
        return RespError(line), next_position
    if kind == ord(b":"):
        try:
            return int(line), next_position
        except ValueError as exc:
            raise Failure(f"invalid RESP integer {line!r}") from exc
    if kind == ord(b"$"):
        try:
            length = int(line)
        except ValueError as exc:
            raise Failure(f"invalid RESP bulk length {line!r}") from exc
        if length == -1:
            return None, next_position
        if length < -1:
            raise Failure(f"negative RESP bulk length {length}")
        end = next_position + length
        if len(buffer) < end + 2:
            raise IncompleteResponse
        if buffer[end : end + 2] != b"\r\n":
            raise Failure("malformed RESP bulk terminator")
        return bytes(buffer[next_position:end]), end + 2
    if kind == ord(b"*"):
        try:
            count = int(line)
        except ValueError as exc:
            raise Failure(f"invalid RESP array length {line!r}") from exc
        if count == -1:
            return None, next_position
        if count < -1:
            raise Failure(f"negative RESP array length {count}")
        values: list[Any] = []
        cursor = next_position
        for _ in range(count):
            value, cursor = parse_response(buffer, cursor)
            values.append(value)
        return values, cursor
    raise Failure(f"unsupported RESP marker {chr(kind)!r}")


class RespConnection:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buffer = bytearray()
        self.closed = False

    @classmethod
    def connect(cls, host: str, port: int, receive_buffer: int | None = None) -> "RespConnection":
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if receive_buffer is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        sock.settimeout(3.0)
        sock.connect((host, port))
        return cls(sock)

    def command(self, *parts: str | bytes | int, timeout: float = 10.0) -> Any:
        try:
            self.sock.sendall(encode_command(parts))
        except OSError as exc:
            raise Failure(f"failed to send {' '.join(map(str, parts[:2]))}: {exc}") from exc
        response = self.read_response(timeout)
        if response is None:
            raise Failure(f"timed out waiting for {' '.join(map(str, parts[:2]))}")
        if isinstance(response, RespError):
            raise Failure(f"server error for {' '.join(map(str, parts[:2]))}: {response.message!r}")
        return response

    def read_response(self, timeout: float) -> Any | None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                value, consumed = parse_response(self.buffer)
                del self.buffer[:consumed]
                return value
            except IncompleteResponse:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self.sock.settimeout(remaining)
                try:
                    received = self.sock.recv(64 * 1024)
                except socket.timeout:
                    return None
                except OSError as exc:
                    if self.closed:
                        return None
                    raise Failure(f"failed to read RESP response: {exc}") from exc
                if not received:
                    if self.closed:
                        return None
                    raise Failure("server closed a live RESP connection")
                self.buffer.extend(received)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.sock.close()
        except OSError:
            pass


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class DragonflyServer:
    def __init__(
        self,
        binary: pathlib.Path,
        threads: int,
        publish_buffer_limit: str = "1gb",
        environment: dict[str, str] | None = None,
        admin_port: int | None = None,
    ):
        self.binary = binary.resolve()
        self.threads = threads
        self.publish_buffer_limit = publish_buffer_limit
        self.environment = environment
        self.admin_port = admin_port
        self.port = free_port()
        self.work_dir = tempfile.TemporaryDirectory(prefix="perfloop-pubsub-server-")
        self.process: subprocess.Popen[bytes] | None = None
        self.log_path = pathlib.Path(self.work_dir.name) / "dragonfly.log"
        self._log: Any | None = None

    def start(self) -> None:
        command = [
            str(self.binary),
            f"--port={self.port}",
            "--bind=127.0.0.1",
            f"--proactor_threads={self.threads}",
            "--proactor_affinity_mode=off",
            "--force_epoll",
            "--version_check=false",
            f"--publish_buffer_limit={self.publish_buffer_limit}",
            f"--dir={self.work_dir.name}",
        ]
        if self.admin_port is not None:
            command.extend((f"--admin_port={self.admin_port}", "--no_tls_on_admin_port"))

        env = os.environ.copy()
        if self.environment:
            env.update(self.environment)
        self._log = self.log_path.open("wb")
        self.process = subprocess.Popen(
            command, stdout=self._log, stderr=subprocess.STDOUT, env=env
        )

        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise Failure(f"Dragonfly exited during startup:\n{self.logs()}")
            try:
                connection = RespConnection.connect("127.0.0.1", self.port)
                require(connection.command("PING") == b"PONG", "PING did not return PONG")
                connection.close()
                return
            except (OSError, Failure):
                time.sleep(0.05)
        raise Failure(f"Dragonfly did not accept PING within 20 seconds:\n{self.logs()}")

    @property
    def pid(self) -> int:
        require(self.process is not None, "server has not started")
        return self.process.pid

    def logs(self) -> str:
        try:
            if self._log:
                self._log.flush()
            return self.log_path.read_text(errors="replace")[-8000:]
        except OSError:
            return "<no server log available>"

    def stop(self) -> None:
        if self.process is None:
            self.work_dir.cleanup()
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=15.0)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5.0)
        if self._log is not None:
            self._log.close()
        self.work_dir.cleanup()
        self.process = None


def client_tid(inspector: RespConnection, client_id: int) -> int:
    for _ in range(20):
        response = inspector.command("CLIENT", "LIST")
        require(isinstance(response, bytes), "CLIENT LIST did not return a bulk string")
        for record in response.decode(errors="replace").splitlines():
            fields = dict(field.split("=", 1) for field in record.split() if "=" in field)
            if fields.get("id") == str(client_id) and "tid" in fields:
                return int(fields["tid"])
        time.sleep(0.01)
    raise Failure(f"CLIENT LIST never exposed subscriber id {client_id}")


@dataclass
class Subscriber:
    connection: RespConnection
    tid: int


def subscribe(connection: RespConnection, command: str, channel: bytes) -> None:
    response = connection.command(command, channel)
    require(
        isinstance(response, list) and len(response) == 3, f"{command} acknowledgement malformed"
    )
    require(response[0] in (b"subscribe", b"psubscribe"), f"unexpected {command} acknowledgement")


def make_sparse_subscribers(
    server: DragonflyServer,
    owners: int,
    subscribers_per_owner: int,
    channel: bytes = CHANNEL,
) -> tuple[list[Subscriber], list[int], RespConnection]:
    inspector = RespConnection.connect("127.0.0.1", server.port)
    selected_tids: list[int] = []
    per_tid: dict[int, int] = defaultdict(int)
    subscribers: list[Subscriber] = []
    maximum_attempts = max(owners * subscribers_per_owner * server.threads * 4, server.threads * 8)

    try:
        for _ in range(maximum_attempts):
            if len(selected_tids) == owners and all(
                per_tid[tid] == subscribers_per_owner for tid in selected_tids
            ):
                break

            candidate = RespConnection.connect("127.0.0.1", server.port)
            try:
                connection_id = candidate.command("CLIENT", "ID")
                require(isinstance(connection_id, int), "CLIENT ID did not return an integer")
                tid = client_tid(inspector, connection_id)

                retain = False
                if len(selected_tids) < owners:
                    if tid not in selected_tids:
                        selected_tids.append(tid)
                        retain = True
                elif tid in selected_tids and per_tid[tid] < subscribers_per_owner:
                    retain = True

                if retain:
                    subscribe(candidate, "SUBSCRIBE", channel)
                    per_tid[tid] += 1
                    subscribers.append(Subscriber(candidate, tid))
                    candidate = None  # ownership moved into subscribers
            finally:
                if candidate is not None:
                    candidate.close()
    except Exception:
        for subscriber in subscribers:
            subscriber.connection.close()
        inspector.close()
        raise

    if len(selected_tids) != owners or any(
        per_tid[tid] != subscribers_per_owner for tid in selected_tids
    ):
        for subscriber in subscribers:
            subscriber.connection.close()
        inspector.close()
        raise Failure(
            f"could not create {subscribers_per_owner} subscribers on each of {owners} owner threads; "
            f"observed {dict(per_tid)}"
        )

    return subscribers, selected_tids, inspector


def runtime_payload(sequence: int, payload_bytes: int) -> bytes:
    prefix = f"{sequence:016x}-{time.monotonic_ns():016x}".encode()
    require(
        payload_bytes >= len(prefix), "payload shape is too small for its runtime-varying prefix"
    )
    return prefix + (b"x" * (payload_bytes - len(prefix)))


def expect_message(response: Any, channel: bytes, payload: bytes) -> None:
    require(isinstance(response, list) and len(response) == 3, "pub/sub message response malformed")
    require(response[0] == b"message", f"expected message response, got {response!r}")
    require(response[1] == channel and response[2] == payload, "pub/sub message payload changed")


def publish_and_verify_order(
    server: DragonflyServer,
    subscribers: list[Subscriber],
    owners: int,
    subscribers_per_owner: int,
    publications: int,
    payload_bytes: int,
) -> None:
    publisher = RespConnection.connect("127.0.0.1", server.port)
    payloads: list[bytes] = []
    expected_recipients = owners * subscribers_per_owner
    try:
        for sequence in range(publications):
            payload = runtime_payload(sequence, payload_bytes)
            payloads.append(payload)
            recipients = publisher.command("PUBLISH", CHANNEL, payload)
            require(
                recipients == expected_recipients,
                "PUBLISH reply did not equal matching subscriptions",
            )

        # Read only after all replies have completed.  This makes the payload and
        # subscription snapshot cross the asynchronous proactor handoff before
        # the consumer observes them.
        for subscriber in subscribers:
            for payload in payloads:
                expect_message(subscriber.connection.read_response(10.0), CHANNEL, payload)
    finally:
        publisher.close()


class SubscriberDrainer:
    def __init__(self, subscriber: Subscriber, channel: bytes = CHANNEL):
        self.subscriber = subscriber
        self.channel = channel
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.count = 0
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                response = self.subscriber.connection.read_response(0.25)
                if response is None:
                    continue
                require(
                    isinstance(response, list) and len(response) == 3,
                    "drainer saw malformed pub/sub response",
                )
                require(
                    response[0] == b"message" and response[1] == self.channel,
                    f"drainer saw unexpected response {response!r}",
                )
                with self.lock:
                    self.count += 1
        except BaseException as exc:  # propagate thread failures into the workload
            if not self.stop_event.is_set():
                self.error = exc

    def delivered(self) -> int:
        with self.lock:
            return self.count

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=3.0)
        self.subscriber.connection.close()
        if self.thread.is_alive():
            raise Failure("subscriber drainer did not stop")
        if self.error is not None:
            raise Failure(f"subscriber drainer failed: {self.error}") from self.error


def callback_addresses(binary: pathlib.Path) -> str:
    elf_header = subprocess.check_output(["readelf", "-h", str(binary)], text=True)
    require(
        "EXEC (Executable file)" in elf_header,
        "counter build must be non-PIE for stable function addresses",
    )
    symbols = subprocess.check_output(["nm", "-C", "--defined-only", str(binary)], text=True)
    addresses: list[str] = []
    for line in symbols.splitlines():
        if (
            "ChannelStore::SendMessages" not in line
            or "internal_invoker" not in line
            or "::invoke(" not in line
            or "[clone .cold]" in line
        ):
            continue
        fields = line.split(maxsplit=2)
        if len(fields) == 3 and fields[1].lower() == "t":
            addresses.append(fields[0])
    require(addresses, "did not find instrumented SendMessages brief callback invokers")
    require(
        len(addresses) <= 32, f"too many SendMessages brief callback invokers: {len(addresses)}"
    )
    return ",".join(addresses)


@dataclass(frozen=True)
class Shape:
    threads: int
    owners: int
    subscribers_per_owner: int
    payload_bytes: int

    @property
    def suffix(self) -> str:
        return f"t{self.threads}_h{self.owners}_s{self.subscribers_per_owner}_b{self.payload_bytes}"


CALLBACK_SHAPES = (
    Shape(4, 1, 1, 64),
    Shape(8, 1, 1, 64),
    Shape(16, 1, 1, 64),
    Shape(8, 2, 1, 64),
    Shape(8, 4, 1, 64),
    Shape(8, 2, 4, 64),
    Shape(8, 2, 1, 256),
    Shape(8, 2, 1, 4096),
)


def read_counter(path: pathlib.Path) -> int:
    require(path.exists(), "instrumented Dragonfly did not write its callback counter")
    try:
        return int(path.read_text().strip())
    except ValueError as exc:
        raise Failure(f"invalid callback counter contents: {path.read_text()!r}") from exc


def callback_count_sample(
    binary: pathlib.Path, publications: int, shapes: tuple[Shape, ...] = CALLBACK_SHAPES
) -> dict[str, float]:
    require(shapes, "callback sample needs at least one sparse shape")
    target_addresses = callback_addresses(binary)
    metrics: dict[str, float] = {}
    callback_counts: list[float] = []
    empty_counts: list[float] = []

    with tempfile.TemporaryDirectory(prefix="perfloop-pubsub-counter-") as raw_directory:
        directory = pathlib.Path(raw_directory)
        for shape in shapes:
            counter_path = directory / f"{shape.suffix}.count"
            server = DragonflyServer(
                binary,
                threads=shape.threads,
                environment={
                    "PERFLOOP_COUNTER_ADDRS": target_addresses,
                    "PERFLOOP_COUNTER_FILE": str(counter_path),
                },
            )
            subscribers: list[Subscriber] = []
            inspector: RespConnection | None = None
            try:
                server.start()
                subscribers, owner_tids, inspector = make_sparse_subscribers(
                    server, shape.owners, shape.subscribers_per_owner
                )
                publish_and_verify_order(
                    server,
                    subscribers,
                    owners=len(owner_tids),
                    subscribers_per_owner=shape.subscribers_per_owner,
                    publications=publications,
                    payload_bytes=shape.payload_bytes,
                )
            finally:
                for subscriber in subscribers:
                    subscriber.connection.close()
                if inspector is not None:
                    inspector.close()
                server.stop()

            entries = read_counter(counter_path)
            require(
                entries % publications == 0,
                f"callback entries {entries} were not divisible by {publications} publications",
            )
            per_publish = entries / publications
            require(
                per_publish >= shape.owners,
                f"only {per_publish} callback entries for {shape.owners} recipient owner threads",
            )
            empty_per_publish = per_publish - shape.owners
            suffix = shape.suffix
            metrics[f"brief_callbacks_per_publish_{suffix}"] = per_publish
            metrics[f"empty_brief_callbacks_per_publish_{suffix}"] = empty_per_publish
            metrics[f"recipient_owner_threads_{suffix}"] = float(shape.owners)
            callback_counts.append(per_publish)
            empty_counts.append(empty_per_publish)

    metrics["brief_callbacks_per_publish"] = sum(callback_counts) / len(callback_counts)
    metrics["empty_brief_callbacks_per_publish"] = sum(empty_counts) / len(empty_counts)
    if len(shapes) == 1:
        shape = shapes[0]
        metrics["pool_threads"] = float(shape.threads)
        metrics["recipient_owner_threads"] = float(shape.owners)
        metrics["subscriptions_per_owner"] = float(shape.subscribers_per_owner)
        metrics["payload_bytes"] = float(shape.payload_bytes)
    return metrics


def random_sparse_callback_shape() -> Shape:
    """Choose one sparse callback topology for this independent sample.

    Keeping four owner ranges below every selected pool width makes the
    recipient topology sparse, while varying all four workload dimensions makes
    the callback-count result an actual distribution over the claimed path.
    """

    return Shape(
        threads=secrets.choice((12, 13, 14, 15, 16)),
        owners=secrets.choice((1, 2, 3, 4)),
        subscribers_per_owner=secrets.choice((1, 4)),
        payload_bytes=secrets.choice((64, 256, 4096)),
    )


def process_cpu_usec(pid: int) -> float:
    stat = pathlib.Path(f"/proc/{pid}/stat").read_text()
    # The second field is parenthesized and may contain spaces.  Everything after
    # its closing paren begins at kernel field 3, so utime/stime are indices 11/12.
    remainder = stat.rsplit(") ", 1)[1].split()
    ticks = int(remainder[11]) + int(remainder[12])
    return ticks * 1_000_000.0 / os.sysconf(os.sysconf_names["SC_CLK_TCK"])


def run_loader(
    loader: pathlib.Path,
    port: int,
    payload_bytes: int,
    expected_recipients: int,
    duration_ms: int,
    connections: int,
) -> dict[str, float]:
    command = [
        str(loader.resolve()),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--payload-bytes",
        str(payload_bytes),
        "--expected-recipients",
        str(expected_recipients),
        "--duration-ms",
        str(duration_ms),
        "--connections",
        str(connections),
    ]
    result = subprocess.run(
        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
    )
    if result.returncode != 0:
        raise Failure(f"native PUBLISH load driver failed ({result.returncode}):\n{result.stderr}")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    require(len(lines) == 1, f"native load driver emitted unexpected stdout: {result.stdout!r}")
    try:
        values = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise Failure(f"native load driver did not emit JSON: {lines[0]!r}") from exc
    require(
        all(key in values for key in ("count", "p50_ms", "p99_ms", "ops_per_sec")),
        "native load driver omitted a required result",
    )
    return {key: float(value) for key, value in values.items()}


def wait_for_delivery(drainers: list[SubscriberDrainer], publications: int) -> None:
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if all(drainer.delivered() >= publications for drainer in drainers):
            return
        time.sleep(0.01)
    observed = [drainer.delivered() for drainer in drainers]
    raise Failure(f"subscribers did not drain all {publications} messages: {observed}")


def throughput_sample(
    binary: pathlib.Path,
    loader: pathlib.Path,
    shape: Shape,
    duration_ms: int,
    connections: int,
) -> dict[str, float]:
    server = DragonflyServer(binary, threads=shape.threads, publish_buffer_limit="1gb")
    subscribers: list[Subscriber] = []
    drainers: list[SubscriberDrainer] = []
    inspector: RespConnection | None = None
    try:
        server.start()
        subscribers, owner_tids, inspector = make_sparse_subscribers(
            server, shape.owners, shape.subscribers_per_owner
        )
        drainers = [SubscriberDrainer(subscriber) for subscriber in subscribers]
        for drainer in drainers:
            drainer.start()

        cpu_before = process_cpu_usec(server.pid)
        values = run_loader(
            loader,
            server.port,
            shape.payload_bytes,
            expected_recipients=len(subscribers),
            duration_ms=duration_ms,
            connections=connections,
        )
        publications = int(values["count"])
        require(publications > 0, "load driver completed without a PUBLISH")
        wait_for_delivery(drainers, publications)
        cpu_after = process_cpu_usec(server.pid)
        require(
            all(drainer.delivered() == publications for drainer in drainers),
            "a subscriber observed duplicate or missing PUBLISH delivery",
        )

        return {
            "publish_p50_ms": values["p50_ms"],
            "publish_p99_ms": values["p99_ms"],
            "publish_ops_per_sec": values["ops_per_sec"],
            "server_cpu_usec_per_publish": (cpu_after - cpu_before) / publications,
            "recipient_owner_threads": float(len(owner_tids)),
            "subscriptions_per_owner": float(shape.subscribers_per_owner),
            "payload_bytes": float(shape.payload_bytes),
        }
    finally:
        for drainer in drainers:
            drainer.stop()
        for subscriber in subscribers:
            subscriber.connection.close()
        if inspector is not None:
            inspector.close()
        server.stop()


def verify_delivery_and_order(binary: pathlib.Path) -> None:
    server = DragonflyServer(binary, threads=4)
    subscribers: list[Subscriber] = []
    inspector: RespConnection | None = None
    extra_connections: list[RespConnection] = []
    try:
        server.start()
        subscribers, owner_tids, inspector = make_sparse_subscribers(
            server, owners=2, subscribers_per_owner=1
        )
        publish_and_verify_order(
            server,
            subscribers,
            owners=len(owner_tids),
            subscribers_per_owner=1,
            publications=32,
            payload_bytes=256,
        )

        # An exact plus pattern subscription on the same connection must receive
        # two messages, while PUBLISH returns the exact number of subscription
        # entries.  This is the observable one-enqueue-per-subscription contract.
        plain = RespConnection.connect("127.0.0.1", server.port)
        dual = RespConnection.connect("127.0.0.1", server.port)
        extra_connections.extend((plain, dual))
        subscribe(plain, "SUBSCRIBE", b"verify-topic")
        subscribe(dual, "SUBSCRIBE", b"verify-topic")
        subscribe(dual, "PSUBSCRIBE", b"verify-*")
        publisher = RespConnection.connect("127.0.0.1", server.port)
        extra_connections.append(publisher)
        payload = runtime_payload(1000, 128)
        require(
            publisher.command("PUBLISH", b"verify-topic", payload) == 3,
            "exact and pattern subscriptions did not contribute three recipients",
        )
        expect_message(plain.read_response(10.0), b"verify-topic", payload)
        dual_messages = [dual.read_response(10.0), dual.read_response(10.0)]
        require(
            {tuple(message) for message in dual_messages}
            == {
                (b"message", b"verify-topic", payload),
                (b"pmessage", b"verify-*", b"verify-topic", payload),
            },
            "duplicate exact/pattern delivery did not preserve both message forms",
        )
        unsubscribe = plain.command("UNSUBSCRIBE")
        require(
            isinstance(unsubscribe, list)
            and unsubscribe[0] == b"unsubscribe"
            and unsubscribe[2] == 0,
            "UNSUBSCRIBE acknowledgement was malformed",
        )
        require(
            publisher.command("PUBLISH", b"verify-topic", runtime_payload(1001, 128)) == 2,
            "unsubscribed connection still counted as a recipient",
        )
        require(
            plain.read_response(0.3) is None, "message arrived after UNSUBSCRIBE acknowledgement"
        )

        # Each source connection publishes a monotonic sequence concurrently.
        # Global interleaving is intentionally unconstrained; each source's order
        # must survive the per-proactor queue handoff.
        ordered = RespConnection.connect("127.0.0.1", server.port)
        extra_connections.append(ordered)
        subscribe(ordered, "SUBSCRIBE", b"order-topic")
        errors: list[BaseException] = []

        def publish_source(source: int) -> None:
            connection = RespConnection.connect("127.0.0.1", server.port)
            try:
                for sequence in range(32):
                    payload = f"source-{source}:{sequence}".encode()
                    require(
                        connection.command("PUBLISH", b"order-topic", payload) == 1,
                        "ordered PUBLISH reply was not one",
                    )
            except BaseException as exc:
                errors.append(exc)
            finally:
                connection.close()

        workers = [threading.Thread(target=publish_source, args=(source,)) for source in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=15.0)
        require(
            not any(worker.is_alive() for worker in workers), "concurrent publisher did not finish"
        )
        if errors:
            raise Failure(f"concurrent publisher failed: {errors[0]}") from errors[0]

        sequences: dict[int, list[int]] = {0: [], 1: []}
        for _ in range(64):
            response = ordered.read_response(10.0)
            require(
                isinstance(response, list) and response[0] == b"message",
                "ordered delivery malformed",
            )
            source_text, sequence_text = response[2].decode().split(":", 1)
            source = int(source_text.removeprefix("source-"))
            sequences[source].append(int(sequence_text))
        require(
            sequences == {0: list(range(32)), 1: list(range(32))},
            f"per-publisher order changed across proactor queues: {sequences}",
        )
    finally:
        for connection in extra_connections:
            connection.close()
        for subscriber in subscribers:
            subscriber.connection.close()
        if inspector is not None:
            inspector.close()
        server.stop()


def queue_subscriber_bytes(info: bytes) -> int:
    for line in info.decode(errors="replace").splitlines():
        if line.startswith("dispatch_queue_subscriber_bytes:"):
            return int(line.split(":", 1)[1])
    raise Failure("INFO did not expose dispatch_queue_subscriber_bytes")


def verify_backpressure_and_close(binary: pathlib.Path) -> None:
    server = DragonflyServer(binary, threads=2, publish_buffer_limit="100")
    slow: RespConnection | None = None
    control: RespConnection | None = None
    try:
        server.start()
        slow = RespConnection.connect("127.0.0.1", server.port, receive_buffer=1024)
        subscribe(slow, "SUBSCRIBE", b"blocked-topic")
        control = RespConnection.connect("127.0.0.1", server.port)
        payload = b"x" * 100_000
        total_workers = 4
        publications_per_worker = 16
        completed = 0
        completed_lock = threading.Lock()
        errors: list[BaseException] = []

        def flood() -> None:
            nonlocal completed
            connection = RespConnection.connect("127.0.0.1", server.port)
            try:
                for _ in range(publications_per_worker):
                    recipients = connection.command(
                        "PUBLISH", b"blocked-topic", payload, timeout=30.0
                    )
                    require(
                        isinstance(recipients, int) and recipients >= 0,
                        "backpressure PUBLISH reply malformed",
                    )
                    with completed_lock:
                        completed += 1
            except BaseException as exc:
                errors.append(exc)
            finally:
                connection.close()

        workers = [threading.Thread(target=flood) for _ in range(total_workers)]
        for worker in workers:
            worker.start()

        observed_backpressure = False
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            info = control.command("INFO")
            require(isinstance(info, bytes), "INFO did not return a bulk string")
            queued = queue_subscriber_bytes(info)
            with completed_lock:
                finished_before_close = completed
            if queued >= 100 and finished_before_close < total_workers * publications_per_worker:
                observed_backpressure = True
                break
            time.sleep(0.05)
        require(
            observed_backpressure,
            "slow subscriber did not create the configured per-thread publish backpressure",
        )

        # Closing a subscribed connection while producers are parked must wake the
        # budget waiters and must not dereference its stale subscription pointer.
        slow.close()
        slow = None
        for worker in workers:
            worker.join(timeout=30.0)
        require(
            not any(worker.is_alive() for worker in workers),
            "publishers remained blocked after slow subscriber disconnected",
        )
        if errors:
            raise Failure(
                f"publisher failed across slow-subscriber close: {errors[0]}"
            ) from errors[0]
        with completed_lock:
            require(
                completed == total_workers * publications_per_worker,
                "not every publisher completed after slow subscriber close",
            )
        require(
            control.command("PING") == b"PONG",
            "server did not remain live after close/backpressure",
        )
    finally:
        if slow is not None:
            slow.close()
        if control is not None:
            control.close()
        server.stop()


def verify(binary: pathlib.Path) -> None:
    verify_delivery_and_order(binary)
    verify_backpressure_and_close(binary)
    print("pubsub_correctness_and_backpressure_ok")


def http_get(port: int, path: str) -> None:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=20.0)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        response.read()
    finally:
        connection.close()


def sampled_channel_store_cpu_samples(binary: pathlib.Path, profile: pathlib.Path) -> int:
    """Return cumulative profile samples whose stack contains SendMessages.

    Gperftools writes a legacy profile with executable program counters.  The Go
    pprof renderer preserves the PCs but does not resolve all C++ functions in
    this binary, so resolve only the sampled executable locations with the
    repository toolchain's addr2line.  This keeps the check about the real CPU
    profile rather than a textual rendering accident.
    """
    raw_result = subprocess.run(
        ["go", "tool", "pprof", "-raw", str(binary), str(profile)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=120.0,
    )
    require(raw_result.returncode == 0, f"go tool pprof -raw failed: {raw_result.stderr}")

    sample_weights: dict[int, int] = defaultdict(int)
    locations: dict[int, str] = {}
    in_samples = False
    in_locations = False
    for line in raw_result.stdout.splitlines():
        if line == "Samples:":
            in_samples, in_locations = True, False
            continue
        if line == "Locations":
            in_samples, in_locations = False, True
            continue
        if line == "Mappings":
            in_locations = False
        if in_samples and ":" in line:
            prefix, location_text = line.split(":", 1)
            counts = prefix.split()
            if not counts:
                continue
            try:
                weight = int(counts[0])
                for location_id in location_text.split():
                    sample_weights[int(location_id)] += weight
            except ValueError:
                continue
        if in_locations:
            match = re.match(r"\s*(\d+): (0x[0-9a-f]+) M=1", line)
            if match:
                locations[int(match.group(1))] = match.group(2)

    sampled_ids = [location_id for location_id in sample_weights if location_id in locations]
    require(sampled_ids, "CPU profile had no sampled Dragonfly executable locations")
    addresses = [locations[location_id] for location_id in sampled_ids]
    symbol_result = subprocess.run(
        ["addr2line", "-C", "-f", "-e", str(binary), *addresses],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=120.0,
    )
    require(symbol_result.returncode == 0, f"addr2line failed: {symbol_result.stderr}")
    lines = symbol_result.stdout.splitlines()
    require(len(lines) == 2 * len(addresses), "addr2line returned an incomplete symbol listing")

    return sum(
        sample_weights[location_id]
        for index, location_id in enumerate(sampled_ids)
        if "ChannelStore::SendMessages" in lines[index * 2]
    )


def profile_sample(binary: pathlib.Path, loader: pathlib.Path) -> None:
    admin_port = free_port()
    before = set(pathlib.Path("/tmp/profile").glob("dragonfly_*.prof"))
    server = DragonflyServer(binary, threads=8, admin_port=admin_port)
    subscribers: list[Subscriber] = []
    drainers: list[SubscriberDrainer] = []
    inspector: RespConnection | None = None
    profile_files: list[pathlib.Path] = []
    try:
        server.start()
        subscribers, _, inspector = make_sparse_subscribers(
            server, owners=1, subscribers_per_owner=1
        )
        drainers = [SubscriberDrainer(subscriber) for subscriber in subscribers]
        for drainer in drainers:
            drainer.start()
        http_get(admin_port, "/profilez?profile=on")
        values = run_loader(
            loader,
            server.port,
            payload_bytes=64,
            expected_recipients=1,
            duration_ms=10_000,
            connections=2,
        )
        wait_for_delivery(drainers, int(values["count"]))
        # Profilez redirects after stopping.  The raw profile is written before
        # the optional legacy pprof-to-SVG conversion, so no external pprof is
        # needed for the assertion below.
        http_get(admin_port, "/profilez?profile=off")
        profile_files = sorted(
            set(pathlib.Path("/tmp/profile").glob("dragonfly_*.prof")) - before,
            key=lambda path: path.stat().st_mtime,
        )
        require(profile_files, "profilez did not produce a raw CPU profile")
        profile = profile_files[-1]
        samples = sampled_channel_store_cpu_samples(binary, profile)
        require(
            samples > 0,
            "CPU profile did not contain ChannelStore::SendMessages under sparse PUBLISH load",
        )
        print(f"profile_channel_store_samples={samples}")
        print("profile_channel_store_present=1")
    finally:
        for drainer in drainers:
            drainer.stop()
        for subscriber in subscribers:
            subscriber.connection.close()
        if inspector is not None:
            inspector.close()
        server.stop()
        for profile in profile_files:
            try:
                profile.unlink()
            except OSError:
                pass


def emit(metrics: dict[str, float]) -> None:
    for metric, value in metrics.items():
        print(
            json.dumps({"metric": metric, "value": value}, separators=(",", ":"), allow_nan=False)
        )


def checked_path(value: str, name: str) -> pathlib.Path:
    path = pathlib.Path(value)
    require(path.is_file(), f"{name} does not exist: {path}")
    require(os.access(path, os.X_OK), f"{name} is not executable: {path}")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=("callback-count", "throughput", "verify", "profile"), required=True
    )
    parser.add_argument("--dragonfly", required=True)
    parser.add_argument("--loader")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--owners", type=int, default=2)
    parser.add_argument("--subscribers-per-owner", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--duration-ms", type=int, default=2000)
    parser.add_argument("--connections", type=int, default=2)
    parser.add_argument("--publications", type=int, default=8)
    parser.add_argument(
        "--random-sparse-callback-shape",
        action="store_true",
        help="sample one runtime-selected sparse pool/owner/subscriber/payload shape",
    )
    args = parser.parse_args()

    try:
        binary = checked_path(args.dragonfly, "Dragonfly binary")
        if args.mode == "callback-count":
            require(args.publications > 0, "--publications must be positive")
            shapes = (
                (random_sparse_callback_shape(),)
                if args.random_sparse_callback_shape
                else CALLBACK_SHAPES
            )
            emit(callback_count_sample(binary, args.publications, shapes))
        elif args.mode == "throughput":
            loader = checked_path(args.loader or "", "native load driver")
            shape = Shape(args.threads, args.owners, args.subscribers_per_owner, args.payload_bytes)
            require(shape.owners <= shape.threads, "owners cannot exceed proactor threads")
            emit(throughput_sample(binary, loader, shape, args.duration_ms, args.connections))
        elif args.mode == "verify":
            verify(binary)
        else:
            loader = checked_path(args.loader or "", "native load driver")
            profile_sample(binary, loader)
    except (Failure, OSError, subprocess.SubprocessError) as exc:
        print(f"perfloop_pubsub: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
