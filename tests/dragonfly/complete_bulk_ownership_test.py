import asyncio

from .instance import DflyInstance


VALUE_SIZE = 24 * 1024


def resp_command(*parts: bytes) -> bytes:
    frame = bytearray(f"*{len(parts)}\r\n".encode())
    for part in parts:
        frame.extend(f"${len(part)}\r\n".encode())
        frame.extend(part)
        frame.extend(b"\r\n")
    return bytes(frame)


def payload(seed: int) -> bytes:
    return bytes((index + seed) % 256 for index in range(VALUE_SIZE))


async def read_simple(reader: asyncio.StreamReader) -> None:
    assert await reader.readline() == b"+OK\r\n"


async def read_bulk(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readline()
    assert header.startswith(b"$")
    size = int(header[1:-2])
    value_with_crlf = await reader.readexactly(size + 2)
    assert value_with_crlf[-2:] == b"\r\n"
    return value_with_crlf[:-2]


async def test_complete_bulk_set_survives_connection_buffer_reuse(df_server: DflyInstance):
    """Keep values correct after ParseRedis consumes and reuses its receive buffer."""
    reader, writer = await asyncio.open_connection(
        "127.0.0.1", df_server.port, limit=2 * VALUE_SIZE
    )
    values = {f"complete-bulk:{index}".encode(): payload(index) for index in range(4)}

    try:
        # The warmup grows the parser-hinted receive buffer. Every following 24 KiB
        # frame fits in its 32 KiB capacity and can take the complete-bulk route.
        writer.write(resp_command(b"SET", b"complete-bulk:warmup", payload(99)))
        await writer.drain()
        await read_simple(reader)

        # Send two waves before consuming replies. ParseRedis can consume and reuse
        # its IoBuf while earlier ParsedCommands are still queued for dispatch.
        writer.write(b"".join(resp_command(b"SET", key, value) for key, value in values.items()))
        writer.write(b"".join(resp_command(b"SET", key, value) for key, value in values.items()))
        await writer.drain()
        for _ in range(2 * len(values)):
            await read_simple(reader)

        for key, expected in values.items():
            writer.write(resp_command(b"GET", key))
            await writer.drain()
            assert await read_bulk(reader) == expected
    finally:
        writer.close()
        await writer.wait_closed()
