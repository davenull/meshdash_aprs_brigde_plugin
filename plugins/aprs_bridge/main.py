import asyncio
import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
from typing import List, Optional

# MeshDash's core loads this file standalone via
# importlib.util.spec_from_file_location(f"plugin_{pid}", entry_file), i.e.
# outside of any package context -- relative imports (`from .config import
# ...`) fail here with "attempted relative import with no known parent
# package" (verified against the real loader). The rest of this plugin's
# code DOES live inside the aprs_bridge package (alongside this file), so we
# put plugins/ on sys.path and use absolute imports instead, exactly like
# tests/ does via pytest's pythonpath=["plugins"] setting. Everything below
# this point is cheap (path string manipulation only, no I/O, no third-party
# imports), so it's safe at module scope.
_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PLUGINS_ROOT = os.path.dirname(_PLUGIN_DIR)
if _PLUGINS_ROOT not in sys.path:
    sys.path.insert(0, _PLUGINS_ROOT)

# fastapi/pydantic, like pypubsub, are real MeshDash core dependencies --
# already present in its venv, not something our setup.py installs -- so
# they're safe to import at module scope like the rest of this block.
# aprs_bridge.config/registry/commands have zero third-party dependencies of
# their own (json/os/dataclasses/sqlite3/re stdlib only), so they're safe at
# module scope too; only protocol.kiss/ax25 and aprs_message (which need
# kiss3/ax253) stay deferred into _bootstrap.
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from aprs_bridge import commands, registry
from aprs_bridge.config import ConfigError, load_config

core_context: dict = {}
plugin_router = APIRouter()

SENTINEL = os.path.join(_PLUGIN_DIR, ".setup_complete")
SETUP_SCRIPT = os.path.join(_PLUGIN_DIR, "setup.py")
CONFIG_PATH = os.path.join(_PLUGIN_DIR, "config.json")

_state: dict = {}


def _run_setup(logger) -> bool:
    try:
        result = subprocess.run(
            [sys.executable, SETUP_SCRIPT],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except Exception as exc:
        logger.error("aprs_bridge: setup.py failed to run: %s", exc)
        return False
    if result.returncode != 0:
        logger.error("aprs_bridge: setup.py failed: %s", result.stderr)
        return False
    return True


async def _watchdog_heartbeat(context: dict) -> None:
    wd = context.get("plugin_watchdog")
    pid = context.get("plugin_id")
    logger = context["logger"]
    while True:
        try:
            await asyncio.sleep(30)
            if wd is not None and pid:
                wd[pid] = time.time()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("aprs_bridge: watchdog heartbeat error")


async def _ack_poll_loop(ack_tracker, logger) -> None:
    """Periodically checks for due ACK retransmits / exhausted sends.
    ack_tracker.poll() can do a blocking socket write via transport_send,
    so it's offloaded to a thread rather than run directly on the event
    loop -- matches CLAUDE.md's "DB access from async handlers goes
    through asyncio.to_thread" caution, applied here to socket I/O."""
    while True:
        try:
            await asyncio.sleep(10)
            await asyncio.to_thread(ack_tracker.poll)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("aprs_bridge: ack poll loop error")


def _bootstrap(context: dict) -> None:
    """Runs in its own background thread so init_plugin() can return well
    within its 15s timeout even on first run, when setup.py may need to
    pip-install kiss3/ax253/aprs3 (verified: the core marks the plugin
    'crashed' if init_plugin doesn't return within 15s)."""
    logger = context["logger"]

    if not os.path.exists(SENTINEL):
        logger.info("aprs_bridge: running first-time dependency setup")
        if not _run_setup(logger):
            logger.error("aprs_bridge: setup failed; bridge will not start")
            return

    try:
        from aprs_bridge.ack_tracker import AckTracker, MsgnoGenerator
        from aprs_bridge.transport import TncTransport
        from aprs_bridge.bridge import RfToMeshBridge
        from aprs_bridge.mesh_bridge import MeshToRfBridge
        from aprs_bridge.protocol.dedupe import DedupeCache
        from aprs_bridge.protocol.ratelimit import RateLimiter
        # pypubsub is already a MeshDash core dependency (not something we
        # install ourselves), but it's still third-party, so it's imported
        # here rather than at module scope like everything else in this
        # try block.
        from pubsub import pub
    except Exception:
        logger.exception("aprs_bridge: failed to import plugin modules")
        return

    try:
        cfg = load_config(CONFIG_PATH)
    except ConfigError as exc:
        logger.error("aprs_bridge: invalid config, bridge disabled: %s", exc)
        return

    registry_conn = registry.init_db(cfg.registry_db_path)
    cm = context["connection_manager"]
    meshtastic_data = context["meshtastic_data"]
    loop = context["event_loop"]

    # RfToMeshBridge needs a way to send bytes back out over the TNC, but
    # TncTransport needs the bridge's on_ax25_frame as its RX callback --
    # each depends on the other's instance. A small mutable holder breaks
    # the cycle without relying on closure-capture ordering.
    transport_holder: dict = {}

    def _transport_send(data: bytes) -> bool:
        transport = transport_holder.get("transport")
        return transport.send(data) if transport is not None else False

    # Shared between both bridge directions: dedupe catches a loop/echo
    # regardless of which direction re-hears it, and ack_tracker's
    # mesh->RF sends are acked by RF->mesh's RX path (see bridge.py's
    # docstring). Each direction gets its own RateLimiter -- the two
    # sides consume distinct shared resources (mesh channel load vs RF
    # airtime) even though the configured numeric limits are the same.
    dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_sec)

    def _notify_mesh_sender(node_id: str, text: str) -> None:
        async def _send() -> None:
            if not cm.is_ready.is_set():
                logger.warning(
                    "aprs_bridge: connection_manager not ready; dropping ack notification to %s", node_id
                )
                return
            try:
                await cm.sendText(text, destinationId=node_id, channelIndex=cfg.mesh_channel_index)
            except Exception:
                logger.exception("aprs_bridge: ack notification sendText to %s failed", node_id)

        asyncio.run_coroutine_threadsafe(_send(), loop)

    def _on_acked(msgno: str, addressee: str, node_id: str) -> None:
        _notify_mesh_sender(node_id, f"{addressee} acked your message.")

    def _on_exhausted(msgno: str, addressee: str, node_id: str) -> None:
        _notify_mesh_sender(
            node_id,
            f"No ack from {addressee} after {cfg.ack_max_attempts} tries; "
            "message may not have been received.",
        )

    ack_tracker = AckTracker(
        transport_send=_transport_send,
        logger=logger,
        retry_intervals=cfg.ack_retry_intervals_sec,
        max_attempts=cfg.ack_max_attempts,
        on_acked=_on_acked,
        on_exhausted=_on_exhausted,
    )
    msgno_generator = MsgnoGenerator()
    rf_to_mesh_limiter = RateLimiter(
        direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
        direction_capacity=cfg.rate_limit_burst,
        per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
        per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
    )
    mesh_to_rf_limiter = RateLimiter(
        direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
        direction_capacity=cfg.rate_limit_burst,
        per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
        per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
    )

    bridge = RfToMeshBridge(
        cfg=cfg,
        registry_conn=registry_conn,
        connection_manager=cm,
        meshtastic_data=meshtastic_data,
        event_loop=loop,
        logger=logger,
        transport_send=_transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=rf_to_mesh_limiter,
    )
    transport = TncTransport(
        host=cfg.tnc_host,
        port=cfg.tnc_port,
        on_frame=bridge.on_ax25_frame,
        logger=logger,
    )
    transport_holder["transport"] = transport
    transport.start()

    mesh_bridge = MeshToRfBridge(
        cfg=cfg,
        registry_conn=registry_conn,
        connection_manager=cm,
        meshtastic_data=meshtastic_data,
        event_loop=loop,
        logger=logger,
        transport_send=_transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=mesh_to_rf_limiter,
        msgno_generator=msgno_generator,
    )
    asyncio.run_coroutine_threadsafe(_ack_poll_loop(ack_tracker, logger), loop)
    # Always unsubscribe before subscribing (wrapped in try/except) to
    # avoid double-registration if init_plugin ever runs again without a
    # full process restart -- matches the pattern MeshDash's own plugins
    # (mesh_ping, tcp_proxy) use for the same reason.
    try:
        pub.unsubscribe(mesh_bridge.on_mesh_packet, "meshtastic.receive")
    except Exception:
        pass
    try:
        pub.subscribe(mesh_bridge.on_mesh_packet, "meshtastic.receive")
    except Exception:
        logger.exception("aprs_bridge: pub.subscribe(meshtastic.receive) failed")

    _state["cfg"] = cfg
    _state["transport"] = transport
    _state["registry_conn"] = registry_conn
    _state["bridge"] = bridge
    _state["mesh_bridge"] = mesh_bridge
    _state["dedupe"] = dedupe
    _state["ack_tracker"] = ack_tracker
    _state["meshtastic_data"] = meshtastic_data
    logger.info("aprs_bridge: bridge started (TNC %s:%d)", cfg.tnc_host, cfg.tnc_port)


def init_plugin(context: dict) -> None:
    core_context.update(context)
    logger = context["logger"]
    loop = context.get("event_loop")

    if loop is not None:
        asyncio.run_coroutine_threadsafe(_watchdog_heartbeat(context), loop)
    else:
        logger.warning("aprs_bridge: no event_loop in context; watchdog heartbeat disabled")

    threading.Thread(
        target=_bootstrap, args=(context,), daemon=True, name="aprs-bridge-bootstrap"
    ).start()
    logger.info("aprs_bridge: init_plugin returning; bootstrap running in background")


# --- HTTP API (used by static/index.html) ---
#
# Route handlers read from _state, which _bootstrap populates once ready
# (usually within a second or two of restart; longer only on a first-ever
# run that needs to pip-install kiss3/ax253/aprs3). Every handler treats
# a missing _state key as "not ready yet" rather than assuming bootstrap
# has completed, since a request can arrive before it has.


class RegistrationRequest(BaseModel):
    callsign: str
    node_id: str


class ConfigUpdateRequest(BaseModel):
    tnc_mode: str
    tnc_host: str
    tnc_port: int
    kiss_port: int = 0
    gateway_callsign: str
    aprs_tocall: str = "APZBRD"
    digi_path: List[str] = Field(default_factory=lambda: ["WIDE1-1", "WIDE2-1"])
    mesh_channel_index: int = 0
    registry_db_path: Optional[str] = None
    dedupe_ttl_sec: float = 30.0
    rate_limit_per_min: float = 20.0
    rate_limit_burst: float = 10.0
    per_callsign_rate_limit_per_min: float = 6.0
    per_callsign_rate_limit_burst: float = 3.0
    ack_retry_intervals_sec: List[float] = Field(default_factory=lambda: [30.0, 60.0, 120.0])
    ack_max_attempts: int = 4
    mesh_fanout_delay_sec: float = 4.0


@plugin_router.get("/status")
async def get_status():
    cfg = _state.get("cfg")
    if cfg is None:
        return {"status": "starting"}

    conn = _state.get("registry_conn")
    transport = _state.get("transport")
    ack_tracker = _state.get("ack_tracker")
    registrations = await asyncio.to_thread(registry.list_registrations, conn) if conn else []

    return {
        "status": "running",
        "tnc_connected": transport.is_connected() if transport else False,
        "tnc_mode": cfg.tnc_mode,
        "tnc_host": cfg.tnc_host,
        "tnc_port": cfg.tnc_port,
        "gateway_callsign": cfg.gateway_callsign,
        "registration_count": len(registrations),
        "pending_acks": ack_tracker.pending_count() if ack_tracker else 0,
    }


def _known_mesh_nodes() -> list:
    meshtastic_data = _state.get("meshtastic_data")
    if meshtastic_data is None:
        return []
    local_id = getattr(meshtastic_data, "local_node_id", None)
    nodes = getattr(meshtastic_data, "nodes", {}) or {}
    out = []
    for node_id, nd in nodes.items():
        if node_id == local_id:
            continue  # the gateway's own radio isn't a registerable mesh user
        user = (nd.get("user") or {}) if isinstance(nd, dict) else {}
        long_name = user.get("longName") or nd.get("long_name") or node_id
        short_name = user.get("shortName") or nd.get("short_name") or node_id[-4:]
        out.append({
            "node_id": node_id,
            "long_name": long_name,
            "short_name": short_name,
            "last_heard": nd.get("lastHeard") or nd.get("last_heard") or 0,
        })
    out.sort(key=lambda n: -(n["last_heard"] or 0))
    return out


@plugin_router.get("/mesh-nodes")
async def list_mesh_nodes_endpoint():
    """Powers the node picker on the registration form -- sourced fresh
    from MeshDash's own live node list on every call, not cached, so a
    just-heard-from device shows up without a page reload."""
    nodes = await asyncio.to_thread(_known_mesh_nodes)
    return {"nodes": nodes}


@plugin_router.get("/registrations")
async def list_registrations_endpoint():
    conn = _state.get("registry_conn")
    if conn is None:
        raise HTTPException(503, "bridge not ready yet")
    rows = await asyncio.to_thread(registry.list_registrations, conn)
    return {
        "registrations": [
            {"callsign": r.callsign, "node_id": r.node_id, "created_at": r.created_at}
            for r in rows
        ]
    }


@plugin_router.post("/registrations")
async def add_registration_endpoint(req: RegistrationRequest):
    conn = _state.get("registry_conn")
    if conn is None:
        raise HTTPException(503, "bridge not ready yet")

    callsign = req.callsign.strip().upper()
    if not commands.is_valid_callsign(callsign):
        raise HTTPException(400, f"{req.callsign!r} doesn't look like a valid callsign-SSID (e.g. W4BRD-13)")

    node_id = req.node_id.strip()
    if not node_id.startswith("!"):
        raise HTTPException(400, "node_id must start with '!' (e.g. '!aabbccdd')")

    await asyncio.to_thread(registry.add_registration, conn, callsign, node_id)
    return {"status": "ok", "callsign": callsign, "node_id": node_id}


@plugin_router.delete("/registrations/by-node/{node_id}")
async def remove_registration_by_node_endpoint(node_id: str):
    """Removes a single device's registration. A callsign can have
    several registered devices, so removal is keyed by device (node_id),
    not by callsign -- see registry.remove_registration for the
    (separate, bulk) "remove every device under this callsign" action."""
    conn = _state.get("registry_conn")
    if conn is None:
        raise HTTPException(503, "bridge not ready yet")
    removed = await asyncio.to_thread(registry.remove_registration_by_node, conn, node_id)
    if removed is None:
        raise HTTPException(404, f"no registration found for node {node_id!r}")
    return {"status": "ok", "callsign": removed, "node_id": node_id}


def _read_raw_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _config_differs(saved: dict, running: dict) -> bool:
    """Compares only keys present in saved (raw JSON on disk, which may
    omit defaulted fields) against running (the fully-resolved, currently
    active BridgeConfig) -- so a config.json edit that hasn't been picked
    up by a restart yet is visible as a real difference."""
    for key, value in saved.items():
        if key not in running:
            continue
        running_value = running[key]
        if isinstance(running_value, tuple):
            running_value = list(running_value)
        if value != running_value:
            return True
    return False


@plugin_router.get("/config")
async def get_config():
    try:
        saved = await asyncio.to_thread(_read_raw_config)
    except Exception as exc:
        raise HTTPException(500, f"failed to read config.json: {exc}")

    running_cfg = _state.get("cfg")
    running = dataclasses.asdict(running_cfg) if running_cfg is not None else None
    restart_required = running is not None and _config_differs(saved, running)

    return {"saved": saved, "running": running, "restart_required": restart_required}


@plugin_router.post("/config")
async def update_config(req: ConfigUpdateRequest):
    data = req.model_dump(exclude_none=True)
    tmp_path = CONFIG_PATH + ".tmp"

    def _write_and_validate():
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        try:
            load_config(tmp_path)  # raises ConfigError on anything invalid
        except ConfigError:
            os.remove(tmp_path)
            raise

    try:
        await asyncio.to_thread(_write_and_validate)
    except ConfigError as exc:
        raise HTTPException(400, str(exc))

    os.replace(tmp_path, CONFIG_PATH)  # same filesystem as CONFIG_PATH -> atomic
    return {
        "status": "ok",
        "restart_required": True,
        "message": "Config saved. Restart the MeshDash service for changes to take effect.",
    }
