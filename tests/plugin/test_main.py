import asyncio
import importlib.util
import json
import logging
import os
import time
from types import SimpleNamespace

PLUGIN_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "plugins",
    "aprs_bridge",
)
MAIN_PY = os.path.join(PLUGIN_DIR, "main.py")


def _load_main_module():
    """Load main.py exactly the way MeshDash's real core loader does:
    importlib.util.spec_from_file_location(f"plugin_{pid}", entry_file),
    outside of any package context. This is the mechanism that broke plain
    relative imports during development -- load it the same way here so a
    regression would be caught by this test too."""
    spec = importlib.util.spec_from_file_location("plugin_aprs_bridge", MAIN_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_main_module_isolated(tmp_path):
    """Load main.py the same way the real core does, then redirect its
    file-path constants into tmp_path before init_plugin can act on them.
    Without this, calling the real init_plugin() writes .setup_complete /
    registrations.db into the actual plugin directory and dials out to the
    real TNC host as a side effect of running the test suite -- exactly
    what CLAUDE.md's "never assume a live radio in tests" rules out."""
    module = _load_main_module()

    (tmp_path / ".setup_complete").write_text("1")  # skip the real pip install
    config = {
        "tnc_mode": "kiss_tcp",
        "tnc_host": "127.0.0.1",
        "tnc_port": 1,  # nothing listens here; transport just retries quietly
        "gateway_callsign": "W4BRD-13",
        "registry_db_path": str(tmp_path / "registrations.db"),
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    module.SENTINEL = str(tmp_path / ".setup_complete")
    module.SETUP_SCRIPT = str(tmp_path / "setup.py")
    module.CONFIG_PATH = str(tmp_path / "config.json")
    return module


def _make_context(tmp_path, connection_manager, event_loop):
    watchdog: dict = {}
    return {
        "connection_manager": connection_manager,
        "meshtastic_data": SimpleNamespace(local_node_id="!local0001"),
        "db_manager": None,
        "node_registry": {},
        "event_loop": event_loop,
        "logger": logging.getLogger("plugin.aprs_bridge"),
        "plugin_watchdog": watchdog,
        "plugin_id": "aprs_bridge",
    }, watchdog


def test_main_module_imports_without_raising():
    # This alone catches: relative-import failures, blocking module-scope
    # work, and any module-scope exception (all of which crash plugin load
    # for every plugin on a real MeshDash instance, per CLAUDE.md).
    module = _load_main_module()
    assert hasattr(module, "init_plugin")
    # Phase 5: plugin_router is real now (registration + config API). It
    # must be an actual APIRouter, never None -- the core does
    # `if hasattr(plugin_module, "plugin_router"): app.include_router(plugin_module.plugin_router, ...)`,
    # and hasattr() is true even for `plugin_router = None`, which would
    # then crash include_router(None, ...).
    from fastapi import APIRouter

    assert isinstance(module.plugin_router, APIRouter)


def test_main_module_does_not_import_third_party_deps_at_module_scope():
    # kiss3/ax253/aprs3 must not be importable as a side effect of merely
    # importing main.py, since they may not be pip-installed yet on a
    # fresh plugin install (setup.py hasn't necessarily run).
    import sys

    for mod_name in ("kiss", "ax253", "aprs"):
        sys.modules.pop(mod_name, None)

    _load_main_module()

    for mod_name in ("kiss", "ax253", "aprs"):
        assert mod_name not in sys.modules, (
            f"importing main.py pulled in {mod_name!r} at module scope"
        )


def test_init_plugin_returns_promptly(fake_connection_manager, running_event_loop, tmp_path):
    # The real core gives init_plugin a hard 15s timeout on a background
    # thread and marks the plugin "crashed" if it's exceeded. Our
    # setup.py/import work must happen in main.py's own background thread,
    # not synchronously inside init_plugin.
    module = _load_main_module_isolated(tmp_path)
    context, _watchdog = _make_context(tmp_path, fake_connection_manager, running_event_loop)

    start = time.monotonic()
    module.init_plugin(context)
    elapsed = time.monotonic() - start

    assert elapsed < 2.0, f"init_plugin took {elapsed:.2f}s; must return almost immediately"


def test_init_plugin_starts_watchdog_heartbeat(fake_connection_manager, running_event_loop, tmp_path):
    module = _load_main_module_isolated(tmp_path)
    context, watchdog = _make_context(tmp_path, fake_connection_manager, running_event_loop)

    module.init_plugin(context)

    # _watchdog_heartbeat() sleeps 30s between pings, so we can't observe a
    # write to the watchdog dict within a fast test. Instead confirm the
    # coroutine was actually scheduled onto the running loop (not silently
    # dropped), which is the failure mode that would otherwise let a plugin
    # get marked "hung" after 120s in production.
    def _has_pending_task():
        return any(
            "_watchdog_heartbeat" in repr(t) for t in asyncio.all_tasks(loop=running_event_loop)
        )

    deadline = time.monotonic() + 2
    found = False
    while time.monotonic() < deadline:
        if _has_pending_task():
            found = True
            break
        time.sleep(0.02)
    assert found, "watchdog heartbeat coroutine was not scheduled onto the event loop"


def test_bootstrap_wires_up_mesh_bridge_and_subscribes_to_pubsub(
    fake_connection_manager, running_event_loop, tmp_path
):
    # Full, real bootstrap (not stubbed): proves the pub.subscribe wiring
    # added for Phase 3 actually works end-to-end, using pypubsub the same
    # way MeshDash's own mesh_ping/tcp_proxy plugins do.
    from pubsub import pub

    module = _load_main_module_isolated(tmp_path)
    context, _watchdog = _make_context(tmp_path, fake_connection_manager, running_event_loop)

    module.init_plugin(context)

    assert _wait_until(lambda: "mesh_bridge" in module._state, timeout=3)
    mesh_bridge = module._state["mesh_bridge"]
    try:
        assert pub.isSubscribed(mesh_bridge.on_mesh_packet, "meshtastic.receive")
    finally:
        pub.unsubscribe(mesh_bridge.on_mesh_packet, "meshtastic.receive")


def _wait_until(predicate, timeout=5, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()
