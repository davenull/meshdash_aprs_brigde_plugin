from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any, Callable

from . import commands, registry
from .ack_tracker import AckTracker, MsgnoGenerator
from .commands import CommandError
from .config import BridgeConfig
from .protocol import ax25, aprs_message, kiss
from .protocol.dedupe import DedupeCache
from .protocol.errors import ProtocolError
from .protocol.ratelimit import RateLimiter

_MAX_OUTBOUND_TEXT = 67  # APRS message text limit; enforced again defensively here.


class MeshToRfBridge:
    """Mesh -> RF. Handles !register/!unregister DM commands (no RF
    involved) and forwards CALLSIGN: prefixed (or last-correspondent)
    DMs from registered mesh nodes out over RF as APRS messages, tracked
    for ACK/retransmit via ack_tracker.

    Hard invariants enforced here (see CLAUDE.md):
    - Only a sender with a current registration may reach RF. No
      registration -> no transmission, ever.
    - Only genuine direct messages addressed to the gateway node reach
      this far at all (_handle_packet returns early on anything else) --
      broadcast/channel content, encrypted-default-channel included, is
      never inspected for RF-gating purposes. There is deliberately no
      channel-index check on top of that: confirmed on real hardware that
      Meshtastic DMs don't carry usable channel-encryption metadata (a DM
      sent from a non-default channel context was still tagged channel 0),
      so a channel allowlist can't discriminate anything for DMs.
    - The AX.25 source on every outbound frame is the gateway's own
      callsign; the registered operator's callsign is embedded in the
      message text for attribution ("user callsign in the payload path").
    """

    def __init__(
        self,
        cfg: BridgeConfig,
        registry_conn: sqlite3.Connection,
        connection_manager,
        meshtastic_data,
        event_loop: asyncio.AbstractEventLoop,
        logger: logging.Logger,
        transport_send: Callable[[bytes], bool],
        dedupe: DedupeCache,
        ack_tracker: AckTracker,
        rate_limiter: RateLimiter,
        msgno_generator: MsgnoGenerator,
    ) -> None:
        self._cfg = cfg
        self._registry_conn = registry_conn
        self._cm = connection_manager
        self._meshtastic_data = meshtastic_data
        self._loop = event_loop
        self._logger = logger
        self._transport_send = transport_send
        self._dedupe = dedupe
        self._ack_tracker = ack_tracker
        self._rate_limiter = rate_limiter
        self._msgno_generator = msgno_generator

    def on_mesh_packet(self, packet: dict, interface: Any = None) -> None:
        try:
            self._handle_packet(packet)
        except Exception:
            self._logger.exception("aprs_bridge: mesh packet handling raised")

    def _handle_packet(self, packet: dict) -> None:
        decoded = packet.get("decoded", {}) or {}
        portnum = str(decoded.get("portnum", ""))
        if "TEXT_MESSAGE" not in portnum:
            return

        from_id = packet.get("fromId") or packet.get("from_id")
        to_id = packet.get("toId") or packet.get("to_id")
        if not from_id or not to_id:
            return

        local_id = getattr(self._meshtastic_data, "local_node_id", None)
        if not local_id or to_id != local_id:
            return  # Not a DM addressed to us; broadcasts/other traffic are not commands.

        text = (decoded.get("text") or "").strip()
        if not text:
            return

        # Prefer the mesh packet id (Meshtastic's own dedup key) when
        # present; fall back to (from_id, text) for synthetic/test
        # packets or the rare packet missing an id.
        packet_id = packet.get("id")
        signature = ("mesh", packet_id) if packet_id is not None else ("mesh", from_id, text)
        if self._dedupe.seen_or_mark(signature):
            self._logger.debug("aprs_bridge: dropping duplicate mesh packet %r", signature)
            return

        self._handle_dm(from_id, text)

    def _handle_dm(self, from_id: str, text: str) -> None:
        # Registration bookkeeping never touches RF and works on any
        # channel -- it's how a node gets onto the allowlist in the first
        # place, so it can't itself require prior registration.
        try:
            new_callsign = commands.parse_register_command(text)
        except CommandError as exc:
            self._reply(from_id, f"Register failed: {exc}")
            return
        if new_callsign is not None:
            registry.add_registration(self._registry_conn, new_callsign, from_id)
            self._logger.info("aprs_bridge: registered %s -> node %s", new_callsign, from_id)
            self._reply(from_id, f"Registered {new_callsign} to this node.")
            return

        if commands.is_unregister_command(text):
            removed = registry.remove_registration_by_node(self._registry_conn, from_id)
            if removed is None:
                self._reply(from_id, "This node has no active registration.")
            else:
                self._logger.info("aprs_bridge: unregistered %s (node %s)", removed, from_id)
                self._reply(from_id, f"Unregistered {removed}.")
            return

        # Everything from here on can reach RF, so the registration
        # invariant applies -- this is the DM path's real (and only
        # remaining) RF gate.
        sender_callsign = registry.lookup_callsign_for_node(self._registry_conn, from_id)
        if sender_callsign is None:
            self._reply(from_id, "Not registered. DM '!register CALLSIGN-SSID' first.")
            return

        if not self._rate_limiter.allow(sender_callsign):
            self._logger.warning(
                "aprs_bridge: rate limit exceeded for %s; dropping mesh->RF request", sender_callsign
            )
            self._reply(from_id, "Rate limit exceeded; message not sent. Try again shortly.")
            return

        addressee, message_text = commands.parse_outbound_request(text)
        if addressee is None:
            addressee = registry.get_last_correspondent(self._registry_conn, sender_callsign)
        if addressee is None:
            self._reply(
                from_id,
                "No recipient given and no prior correspondent. Use 'CALLSIGN: message'.",
            )
            return

        self._send_to_rf(sender_callsign, addressee, message_text)
        registry.set_last_correspondent(self._registry_conn, sender_callsign, addressee)

    def _send_to_rf(self, sender_callsign: str, addressee: str, message_text: str) -> None:
        msgno = self._msgno_generator.next()
        prefix = f"{sender_callsign}: "
        # Reserve room for the trailing "{msgno" (1 + up to 5 chars) the
        # same way encode_message will append it, so the combined field
        # never exceeds the 67-char APRS message text limit.
        suffix_len = 1 + len(msgno)
        available = _MAX_OUTBOUND_TEXT - len(prefix) - suffix_len
        if available <= 0:
            self._logger.warning(
                "aprs_bridge: sender callsign %r alone leaves no room for message text",
                sender_callsign,
            )
            return
        text = prefix + message_text[:available]

        try:
            info = aprs_message.encode_message(addressee, text, msgno=msgno)
        except ProtocolError as exc:
            self._logger.warning("aprs_bridge: could not encode outbound APRS message: %s", exc)
            return

        ax25_frame = ax25.build_ui_frame(
            self._cfg.aprs_tocall, self._cfg.gateway_callsign, self._cfg.digi_path, info
        )
        kiss_frame = kiss.encode_frame(ax25_frame, port=self._cfg.kiss_port)
        if self._transport_send(kiss_frame):
            self._logger.info(
                "aprs_bridge: mesh->RF %s -> %s (msg %s): %r",
                sender_callsign, addressee, msgno, message_text,
            )
            self._ack_tracker.track(msgno, addressee, kiss_frame)
            # Mark our own transmission's signature so a TNC echo /
            # digipeat loopback is recognized as self-originated on RX,
            # not re-processed (and not double-counted against the
            # recipient's own rate-limit bucket) as fresh RF traffic.
            self._dedupe.mark((self._cfg.gateway_callsign, addressee, text, msgno))
        else:
            self._logger.warning("aprs_bridge: failed to send mesh->RF message to %s", addressee)

    def _reply(self, node_id: str, text: str) -> None:
        asyncio.run_coroutine_threadsafe(self._send_reply(node_id, text), self._loop)

    async def _send_reply(self, node_id: str, text: str) -> None:
        if not self._cm.is_ready.is_set():
            self._logger.warning(
                "aprs_bridge: connection_manager not ready; dropping reply to %s", node_id
            )
            return
        try:
            await self._cm.sendText(
                text, destinationId=node_id, channelIndex=self._cfg.mesh_channel_index
            )
        except Exception:
            self._logger.exception("aprs_bridge: reply sendText to %s failed", node_id)
