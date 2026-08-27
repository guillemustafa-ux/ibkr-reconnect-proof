"""Chaos TCP proxy for a REAL IB Gateway (paper account).

Sits between the client and the gateway, forwards every byte, and watches
the client->gateway stream for the placeOrder frame (message type 3). When
the armed fault triggers, it cuts both sockets at the worst possible moment:

``before``
    The placeOrder frame is read from the client and NEVER forwarded.
    The client's TCP write succeeded; the gateway never saw the order.

``after``
    The placeOrder frame is forwarded to the gateway, then both sockets are
    killed before the acknowledgement can travel back.

This is phase 2 of the proof: the same client + ledger + recovery code that
passes the fake-broker suite, demonstrated against Interactive Brokers'
real paper infrastructure. See scripts/live_drill.py.
"""

from __future__ import annotations

import argparse
import asyncio

from .wire import API_HELLO, MSG_PLACE_ORDER, decode_payload, read_frame


class ChaosProxy:
    def __init__(self, target_host: str, target_port: int) -> None:
        self.target_host = target_host
        self.target_port = target_port
        self.port = 0
        self._fault: str | None = None
        self._server: asyncio.Server | None = None
        self.triggered: asyncio.Event = asyncio.Event()

    def arm(self, mode: str) -> None:
        """Arm a one-shot fault: 'before' or 'after' the placeOrder forward."""
        assert mode in ("before", "after")
        self._fault = mode
        self.triggered.clear()

    async def start(self, listen_port: int = 0) -> None:
        self._server = await asyncio.start_server(
            self._handle, "127.0.0.1", listen_port
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server:
            self._server.close()

    async def _handle(self, client_reader, client_writer):
        try:
            target_reader, target_writer = await asyncio.open_connection(
                self.target_host, self.target_port
            )
        except OSError:
            client_writer.close()
            return

        async def pump_target_to_client():
            try:
                while data := await target_reader.read(65536):
                    client_writer.write(data)
                    await client_writer.drain()
            except (ConnectionError, OSError):
                pass

        pump = asyncio.create_task(pump_target_to_client())
        try:
            hello = await client_reader.readexactly(len(API_HELLO))
            target_writer.write(hello)
            while True:
                payload = await read_frame(client_reader)
                fields = decode_payload(payload)
                is_place = fields and fields[0].isdigit() \
                    and int(fields[0]) == MSG_PLACE_ORDER
                if is_place and self._fault == "before":
                    self._fault = None
                    print(f"[chaos] placeOrder id={fields[1]} DROPPED"
                          " before reaching the gateway; cutting the wire")
                    break
                frame = len(payload).to_bytes(4, "big") + payload
                target_writer.write(frame)
                await target_writer.drain()
                if is_place and self._fault == "after":
                    self._fault = None
                    print(f"[chaos] placeOrder id={fields[1]} forwarded;"
                          " cutting the wire before the ack returns")
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            pump.cancel()
            for writer in (client_writer, target_writer):
                try:
                    writer.transport.abort()
                except Exception:
                    pass
            self.triggered.set()


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-port", type=int, default=4003)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", type=int, default=4002)
    parser.add_argument("--fault", choices=["before", "after"], default="after")
    args = parser.parse_args()

    proxy = ChaosProxy(args.target_host, args.target_port)
    proxy.arm(args.fault)
    await proxy.start(args.listen_port)
    print(f"[chaos] proxying 127.0.0.1:{proxy.port} ->"
          f" {args.target_host}:{args.target_port}, one-shot fault: {args.fault}")
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(_main())
