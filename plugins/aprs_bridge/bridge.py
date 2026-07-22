from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Callable

from .config import BridgeConfig
from .protocol import ax25, aprs_message, kiss
from .protocol.errors import ProtocolError
from . import registry


class RfToMeshBridge:
    """Phase 2: RF -> mesh, one-way. Delivers APRS messages addressed to a
    registered callsign to that callsign's mapped Meshtastic node, and
    sends an RF ACK back over the TNC if the message carried a message
    number. Runs on the TNC RX thread via on_ax25_frame(); mesh delivery is
    scheduled onto the MeshDash event loop since connection_manager.sendText
    is a coroutine."""

    def __init__(
        self,
        cfg: BridgeConfig,
        registry_conn: sqlite3.Connection,
        connection_manager,
        event_loop: asyncio.AbstractEventLoop,
        logger: logging.Logger,
        transport_send: Callable[[bytes], bool],
    ) -> None:
        self._cfg = cfg
        self._registry_conn = registry_conn
        self._cm = connection_manager
        self._loop = event_loop
        self._logger = logger
        self._transport_send = transport_send

    def on_ax25_frame(self, ax25_bytes: bytes) -> None:
        try:
            frame = ax25.parse_ui_frame(ax25_bytes)
        except ProtocolError as exc:
            self._logger.debug("aprs_bridge: ignoring non-UI/APRS frame: %s", exc)
            return

        if not aprs_message.is_message(frame.info):
            return

        try:
            message = aprs_message.decode_message(frame.info)
        except ProtocolError as exc:
            self._logger.debug("aprs_bridge: malformed APRS message from %s: %s", frame.source, exc)
            return

        node_id = registry.lookup_node_for_callsign(self._registry_conn, message.addressee)
        if node_id is None:
            self._logger.info(
                "aprs_bridge: dropping APRS message for unregistered callsign %r", message.addressee
            )
            return

        self._logger.info(
            "aprs_bridge: RF->mesh %s -> %s (node %s): %r",
            frame.source, message.addressee, node_id, message.text,
        )
        asyncio.run_coroutine_threadsafe(self._deliver(node_id, message.text), self._loop)

        if message.msgno is not None:
            self._send_ack(frame.source, message.msgno)

    async def _deliver(self, node_id: str, text: str) -> None:
        if not self._cm.is_ready.is_set():
            self._logger.warning("aprs_bridge: connection_manager not ready; dropping message to %s", node_id)
            return
        try:
            await self._cm.sendText(
                text,
                destinationId=node_id,
                channelIndex=self._cfg.mesh_channel_index,
            )
        except Exception:
            self._logger.exception("aprs_bridge: sendText to %s failed", node_id)

    def _send_ack(self, rf_recipient_callsign: str, msgno: str) -> None:
        ack_info = aprs_message.build_ack(rf_recipient_callsign, msgno)
        ax25_frame = ax25.build_ui_frame(
            self._cfg.aprs_tocall,
            self._cfg.gateway_callsign,
            self._cfg.digi_path,
            ack_info,
        )
        kiss_frame = kiss.encode_frame(ax25_frame, port=self._cfg.kiss_port)
        if not self._transport_send(kiss_frame):
            self._logger.warning("aprs_bridge: failed to send RF ack to %s", rf_recipient_callsign)
