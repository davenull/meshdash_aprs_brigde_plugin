import asyncio
import os
import subprocess
import sys
import threading
import time

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

core_context: dict = {}
# No plugin_router in Phase 2 (no HTTP routes). The core does
# `if hasattr(plugin_module, "plugin_router"): app.include_router(plugin_module.plugin_router, ...)`
# -- hasattr() is true even for a module-level `plugin_router = None`, which
# would then crash include_router(None, ...). Omit the name entirely rather
# than setting it to None.

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
        from aprs_bridge.config import ConfigError, load_config
        from aprs_bridge import registry
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
    ack_tracker = AckTracker(
        transport_send=_transport_send,
        logger=logger,
        retry_intervals=cfg.ack_retry_intervals_sec,
        max_attempts=cfg.ack_max_attempts,
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

    _state["transport"] = transport
    _state["registry_conn"] = registry_conn
    _state["bridge"] = bridge
    _state["mesh_bridge"] = mesh_bridge
    _state["dedupe"] = dedupe
    _state["ack_tracker"] = ack_tracker
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
