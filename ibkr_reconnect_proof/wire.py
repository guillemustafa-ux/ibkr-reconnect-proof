"""Framing helpers for the TWS API socket protocol.

The wire format is trivial by design: every message is a 4-byte big-endian
length prefix followed by NUL-terminated string fields. The initial client
hello is the literal bytes ``API\\0`` followed by one framed version-range
payload. Everything here mirrors what ``ib_async.client.Client`` sends and
what its ``Decoder`` expects to receive.
"""

from __future__ import annotations

import asyncio
import struct

API_HELLO = b"API\0"

# Outgoing (client -> server) message type ids used by this project.
MSG_PLACE_ORDER = 3
MSG_CANCEL_ORDER = 4
MSG_REQ_OPEN_ORDERS = 5
MSG_REQ_ACCT_DATA = 6
MSG_REQ_EXECUTIONS = 7
MSG_REQ_IDS = 8
MSG_REQ_ALL_OPEN_ORDERS = 16
MSG_REQ_CURRENT_TIME = 49
MSG_REQ_POSITIONS = 61
MSG_START_API = 71
MSG_REQ_ACCT_UPDATES_MULTI = 76
MSG_REQ_COMPLETED_ORDERS = 99


def encode_frame(fields: list) -> bytes:
    """Encode fields as one length-prefixed, NUL-terminated message."""
    payload = b"".join(str(f).encode() + b"\0" for f in fields)
    return struct.pack(">I", len(payload)) + payload


def decode_payload(payload: bytes) -> list[str]:
    """Split a frame payload into its fields (drops the trailing empty)."""
    parts = payload.decode(errors="backslashreplace").split("\0")
    parts.pop()  # every field ends with NUL, so the last split element is ""
    return parts


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    """Read one length-prefixed frame payload; raises on EOF."""
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">I", header)
    return await reader.readexactly(length)
