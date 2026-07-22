CLAUDE.md — MeshDash APRS ⇄ Meshtastic Bridge Plugin
What this project is

A MeshDash plugin (plugins/aprs_bridge/) that bidirectionally bridges APRS RF (via a KISS or AGW TNC) and a Meshtastic mesh. RF APRS messages addressed to registered callsigns are delivered to their mapped Meshtastic nodes; DMs from registered mesh nodes are transmitted as APRS messages on RF.

Reference material lives in docs/:

docs/RESEARCH.md — full architecture/protocol research. Read it before designing anything.
MeshDash source is vendored/cloned alongside — read existing plugins in plugins/ for real API usage before writing plugin code.
aprstastic (MIT) is the prior-art reference for callsign↔node registration; reuse its patterns.
Hard invariants — never violate, never "improve"
Legal (FCC Part 97 — these are law, not preferences)
Only messages from pre-registered, licensed callsigns may be gated to RF APRS. The registration table is the gate. No registration → no RF transmission. Ever.
Never forward encrypted content to RF. Content from default/encrypted Meshtastic channels does not go to RF APRS under any circumstances.
Every RF transmission must be attributable to a licensed callsign (gateway callsign in the AX.25 source field; user callsign in the payload path).
Do not add config flags that bypass any of the above. If a feature conflicts with these rules, the feature loses.
MeshDash plugin rules (violations destabilize the whole dashboard)
Never open a Meshtastic interface directly (meshtastic.SerialInterface, TCPInterface, etc.). MeshDash owns the radio connection.
Never call pub.subscribe. Read incoming packets via the injected meshtastic_data, the shared DB, or MeshDash's SSE packet events.
Transmit only via await connection_manager.sendText(...), and only after checking connection_manager.is_ready.is_set().
Plugin structure: manifest.json (with "watchdog": true) + main.py exposing init_plugin(context) and optionally plugin_router (FastAPI APIRouter).
Background threads: start them inside init_plugin, daemon=True. Heartbeat context["plugin_watchdog"][plugin_id] = time.time() at least every 120 s.
Call async code from threads only via asyncio.run_coroutine_threadsafe(coro, context["event_loop"]).
DB access from async handlers goes through await asyncio.to_thread(...).
Third-party deps (kiss3, ax253, aprs3) are installed by the plugin's setup.py + .setup_complete sentinel pattern and imported inside init_plugin, never at module scope. No blocking work at module scope.
Nothing at module import time may raise — an unhandled exception kills the dashboard for all plugins.
Architecture (see docs/RESEARCH.md for detail)
TNC layer: primary = KISS-over-TCP to graywolf (192.168.2.39:8001) via kiss3.TCPKISS; alternates = serial KISS (kiss3.SerialKISS) and AGW port 8000 (pyham_pe). Abstract behind a single TncTransport interface so all three are swappable via config.
Frames: AX.25 UI frames, control 0x03, PID 0xF0, encoded/decoded with ax253.
APRS messages: : + 9-char space-padded addressee + : + text + optional {NNN msg id. ACK = text ackNNN. Retransmit un-ACKed outbound messages on a decaying schedule; stop on ACK.
Mesh addressing convention: mesh users DM the gateway node with CALLSIGN: message text (aprstastic convention). Bare messages go to the sender's last correspondent.
Registration: !register CALLSIGN-SSID / !unregister via mesh DM, persisted in the plugin's own SQLite DB (separate file from MeshDash's DB).
Dedupe/loop prevention: short-TTL signature cache (source, addressee, text, msg-id) in both directions; tag and never re-gate self-originated traffic.
Rate limiting: per-direction and per-callsign token buckets. Mesh payload hard limit ~200 bytes. EU duty-cycle profile treats 10 %/hr as a hard cap.
Code standards
Python 3.9+ compatible (MeshDash's floor). Type hints everywhere.
No new runtime deps beyond: kiss3, ax253, aprs3 (and pyham_pe only if AGW support is being implemented). Ask before adding anything else.
Every protocol encode/decode function gets unit tests against known-good hex frames before it's used anywhere.
Log via the injected context["logger"], prefixed with the plugin id.
Config in config.json (TNC mode/host/port, gateway callsign-SSID, digi path default WIDE1-1,WIDE2-1, allowed channels, rate limits, region profile, APRS-IS toggle). Validate on load; fail loud with a clear log message and disable the bridge rather than running half-configured.
Testing
pytest for everything. Protocol layers (KISS framing, AX.25 encode/decode, APRS message parse/build, dedupe, rate limiter) must be testable with zero hardware and zero MeshDash — pure functions plus a mock KISS TCP server fixture.
MeshDash context objects (connection_manager, meshtastic_data, watchdog) get lightweight fakes in tests/conftest.py.
Integration testing target: Direwolf on an audio loopback. Never assume a live radio in tests.
Run the full test suite after every change; do not report a phase complete with failing tests.
Build phases — do them in order, one at a time
Protocol core + tests: KISS framing, AX.25 UI encode/decode, APRS message/ACK parse+build. No MeshDash code yet.
RF → mesh (one-way PoC): plugin skeleton, TNC RX thread, deliver APRS messages to registered nodes, send RF ACKs. Registration table read-only (seeded manually).
Registration + mesh → RF: !register/!unregister flow, CALLSIGN: parsing, AX.25 TX path, Part 97 gating enforced in code.
Reliability: ACK tracking both directions, retransmit schedule, dedupe, rate limiting/duty cycle.
UI + polish: static registration-management page, nav menu entry, config validation UX, APRS-IS optional path.

my callsign is w4brd, and the -13 ssid can be used for testing. The tnc has a radio that another tnc is monitoring so that I can help decode packets.

Do not start a later phase while an earlier one has failing tests or open TODOs.