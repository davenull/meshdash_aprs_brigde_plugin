# Building an APRS ⇄ Meshtastic Bridge Plugin for MeshDash

## TL;DR
- **Build it as a native MeshDash plugin** (a drop-in folder with `manifest.json` + `main.py`), because MeshDash R2.2+ has a mature, documented in-process Python/FastAPI plugin system that hands your code direct access to the live radio (`connection_manager.sendText()`), the packet stream (`meshtastic_data`/SSE), and its SQLite DB — no separate daemon, and it sidesteps the Meshtastic serial single-client limitation.
- **On the TNC side, standardize on KISS-over-TCP to Direwolf (port 8001) as your primary path**, with hardware serial KISS and Direwolf's AGW interface (port 8000) as alternates; use the maintained `kiss3` + `ax253` libraries for KISS/AX.25 framing and `aprs3`/`aprslib` for APRS message parsing, or `pyham_pe` if you use the AGW interface.
- **The single most important constraint is legal, not technical**: to gate Meshtastic traffic onto RF APRS you MUST (a) restrict gating to pre-registered, licensed callsigns only, and (b) never forward encrypted content — follow the proven aprstastic model (callsign↔node registration, clear-text only, FCC Part 97 attribution).

## Key Findings

### MeshDash architecture
- MeshDash is a **free, open-source (GPL-3.0-only), self-hosted single Python process**. Backend is **FastAPI/Python (3.9+)**, the frontend is a single-page app driven by Server-Sent Events (SSE), and storage is **SQLite** (WAL mode). Current stable is **R3.1.2 (released 2026-06-03)**. It runs on Raspberry Pi/Linux/WSL2 and the dashboard defaults to port 8000.
- It connects to Meshtastic radios via **USB Serial, TCP/WiFi (port 4403), BLE, WebSerial, and MQTT**, using the official `meshtastic` Python library internally (the plugin docs confirm `meshtastic` plus `portnums_pb2`, `admin_pb2`, `channel_pb2`, etc. are importable in-process). It supports up to 16 radios ("slots"), each with its own DB and connection manager.
- **It has a formal, documented plugin system** (introduced in R2.2, expanded through R3.x). Plugins are trusted, in-process extensions loaded at startup from `<install_dir>/plugins/`. This is the correct integration point for the bridge.

### MeshDash plugin system (the integration surface)
- Each plugin is a folder with a required `manifest.json` (`id` must match the directory name; `watchdog` is mandatory true/false; optional `router_prefix`, `static_prefix`, `nav_menu`, `bridge`, `permissions`) and a `main.py` entry point that may expose an `plugin_router` (a FastAPI `APIRouter`) and an `init_plugin(context)` function.
- `init_plugin(context)` injects every subsystem you need:
  - `connection_manager` — the live radio; transmit with `await cm.sendText(text, destinationId="!hex"|"^all", channelIndex=0)` and always guard with `cm.is_ready.is_set()`. `cm.interface` exposes the raw Meshtastic interface.
  - `meshtastic_data` — the live in-memory node/packet store (`.nodes`, formatted packet buffer, `local_node_id`, `channel_map`).
  - `db_manager` — the shared SQLite DB (tables incl. `nodes`, `packets`, `messages`); call from async handlers via `await asyncio.to_thread(...)`.
  - `node_registry` — per-slot access for multi-radio setups; `event_loop`, `logger`, `plugin_watchdog`, `plugin_id`.
- A persistent TNC socket needs a background thread. Threads must be **started inside `init_plugin`**, run as daemons, and — if `"watchdog": true` — heartbeat `context["plugin_watchdog"][pid] = time.time()` at least every 120 s. Call coroutines from threads via `asyncio.run_coroutine_threadsafe(coro, event_loop)`.
- **Critical porting rules** (from MeshDash's own "porting a standalone app" guide): do NOT open your own Meshtastic interface, and do NOT call `pub.subscribe` — MeshDash owns the connection and a second listener double-processes every packet or conflicts on the port. Read incoming packets via the injected `meshtastic_data`/DB, or subscribe to MeshDash's SSE `packet` events (e.g. from a bridge page or via `sse_starlette`), and transmit only through `connection_manager.sendText`.
- Non-core dependencies (`kiss3`, `ax253`, `aprs3`, optionally `pyham_pe`) are installed with the documented one-time `setup.py` + `.setup_complete` sentinel pattern (pip run as a subprocess) and imported **inside `init_plugin`**, never at module scope.
- A simpler fallback exists: MeshDash exposes `POST /api/hook` (send a text message to a node ID) plus a broad REST API and SSE stream — usable by an external script — but the in-process plugin is strictly better (no HTTP/auth overhead, direct object access, real-time RX).

### AGWPE (AGW) protocol — enough detail to implement a client
- AGWPE ("AGW Packet Engine", the TCP/IP API by SV2AGW) is served on **TCP port 8000** by Direwolf, ldsped, and AGWPE/Packet Engine Pro. It handles both connected-mode AX.25 sessions and UI/UNPROTO datagrams — a superset of KISS.
- Every message is a **36-byte header** followed by optional data. Multi-byte integers are **little-endian**. Field layout (confirmed against the LU7DID/SV2AGW tutorial and aprx's `agwpesocket.c` struct):
  - Offset **0**: Port — 4 bytes, 0-based radio-port index (only the low byte is meaningful).
  - Offset **4**: DataKind — 1 byte, ASCII frame-type letter (byte 5 reserved).
  - Offset **6**: PID — 1 byte (0x00 or 0xF0 for AX.25 UI; byte 7 reserved).
  - Offset **8**: CallFrom — 10-byte null-padded ASCII `CALLSIGN-SSID`.
  - Offset **18**: CallTo — 10 bytes, same format.
  - Offset **28**: DataLen — 4-byte little-endian uint (0 = no data follows).
  - Offset **32**: User/reserved — 4 bytes.
  - Data (DataLen bytes) follows immediately with no delimiters. On frames from AGWPE→app, CallFrom is the far end and CallTo is you; on app→AGWPE it is reversed.
- Key DataKind letters — app→engine: `P` login (Direwolf ignores it), `X`/`x` register/unregister callsign (needed only for connected mode; UNPROTO needs none), `G` port info, `g` port capabilities, `m` enable monitoring, `R` version, **`M` send UNPROTO/UI**, **`V` send UNPROTO VIA digipeaters**, `C`/`c` connect, `D` connected data, `d` disconnect, `K` send raw AX.25, `k` enable raw RX. Engine→app: `U` monitored UNPROTO received, `T` monitored own TX, `I`/`S` connected/supervisory, `K` raw, `X` registration result, `R` version.
- For APRS you primarily send **`M`** (direct) or **`V`** (with a path). Worked `M` example from the spec (PID 0xF0, port 0, To="NETME", DataLen 0x39): byte 4 = `4D` ('M'), byte 6 = `F0`, DataLen at offset 28 = `39 00 00 00` (little-endian). For **`V`**, the data area begins with a **1-byte digipeater count**, then N × 10-byte null-padded digi callsigns (e.g. `0x02` + "WIDE1-1\0…" + "WIDE2-1\0…" = 21 bytes), immediately followed by the APRS information payload; DataLen covers the VIA list *and* the payload. At least one digi must be listed for `V` (otherwise use `M`).
- Python library: **`pyham_pe`** (PyHam PE, MIT, Python 3.7+, by Martin Cooper KD6YAM) implements this cleanly — e.g. `engine.send_unproto(port, your_callsign, 'APRS', 'payload', ['WIDE1-1'])`, and a `ReceiveHandler` with `monitored_unproto(port, call_from, call_to, text, data)`. Enable monitoring explicitly to receive traffic.

### KISS protocol — framing and AX.25/APRS encoding
- KISS wraps AX.25 frames for a byte stream (serial or TCP). Special bytes: **FEND = 0xC0** (frame delimiter), **FESC = 0xDB**, **TFEND = 0xDC**, **TFESC = 0xDD**. A frame is `FEND | command-byte | payload | FEND`. The command byte's low nibble is the command (0x00 = data), high nibble is the port. Escaping: any 0xC0 in the payload → `0xDB 0xDC`; any 0xDB → `0xDB 0xDD` (the receiver reverses this). Back-to-back FENDs are not an empty frame.
- **Direwolf presents KISS-over-TCP on port 8001** (AGW on 8000; both can run simultaneously); hardware TNCs present serial KISS. Same frame format on both. Direwolf also offers a `-p` pseudo-terminal (`/tmp/kisstnc`) for programs that need a serial device.
- Inside a KISS data frame sits an **AX.25 UI frame** with no HDLC flags/FCS: address fields (7 bytes each — 6 callsign chars left-shifted one bit, plus an SSID byte carrying SSID in bits 1–4), **control 0x03** (UI), **PID 0xF0**, then the APRS information field. The final address in the list sets bit 0 (the extension/end bit); digipeater "has-been-repeated" state uses the high bit of the SSID byte.
- Python libraries: **`kiss3`** (python-aprs, Apache-2.0, Py3.6+, actively maintained) provides `SerialKISS` and `TCPKISS`; its companion **`ax253`** encodes/decodes AX.25 frames and addresses (ax25 or TNC2 formats). `aioax25` is an asyncio alternative that can auto-put a Kantronics KPC-3 into KISS mode and handle multi-port Direwolf — UI frames work; connected mode does not.

### AX.25 / APRS message format (APRS Protocol Reference 1.0.1, ch. 14)
- All APRS traffic uses AX.25 **UI frames**. The standard wide-area digipeater path is **WIDE1-1,WIDE2-1** (fill-in + wide digis, the "new N-paradigm").
- An **APRS message** is: `:` + a **fixed 9-character addressee** (callsign-SSID, space-padded to exactly 9) + `:` + message text + optional `{` + message number. Per the APRS Protocol Reference v1.0.1 ch.14 message-format table the fields are `: | 9-char addressee | : | 1 to 67 characters of message text | { | 1–5 char message no`. Text may not contain `|`, `~`, or `{`. Example: `:WU2Z     :Testing{003`. WB4APR's on-air spec states it directly: *"Station to station messages begin with a colon and a 9 character addressee name padded with spaces to a total of nine characters followed by a colon: `:W3XYZ____:one line message text......{345`."*
- **ACKs**: the recipient replies with a message whose text is `ack` + the message number, e.g. `:WU2Z     :ack003`. ACKs carry no message number of their own. A station only ACKs messages addressed to its own MYCALL — not group/bulletin addressees. A newer two-part `{mm}aa` identifier form also exists.
- APRS is connectionless: there are no link-layer retries — instead a message is re-sent on a decaying interval until an ACK arrives.

### Meshtastic side — programmatic messaging and limits
- **Python library** (`meshtastic`): `interface.sendText("...", destinationId="!hex" or "^all", channelIndex=0)`; receive via `pub.subscribe(onReceive, "meshtastic.receive")`, where a text packet exposes `packet['decoded']['text']` and `packet['fromId']`. **Inside a MeshDash plugin you do not do this directly** — use `connection_manager.sendText` and read MeshDash's packet feed instead.
- **Payload limit**: the application payload is **up to ~200 bytes** per Meshtastic's official Overview docs (*"Application Payload. — up to ~200 bytes"*); the LoRa packet max is 256 bytes (~237-byte usable payload), and MeshDash's own messaging UI enforces a ~230-character limit. Design for ~200 bytes and truncate/split long APRS text.
- **MQTT JSON path** (optional): a gateway node with a channel literally named `mqtt` and downlink enabled will inject to the mesh when you publish JSON like `{"from": <nodenum>, "type": "sendtext", "payload": "hi"}` to `msh/<REGION>/2/json/mqtt/`; inbound mesh text appears on `msh/.../2/json/<CHANNEL>/<nodeid>` with `payload.text` and `type:"text"`. Requires JSON enabled and, for legal RF, plaintext mode.
- **Duty cycle / rate limits**: per Meshtastic's official LoRa Configuration docs, *"EU_433 and EU_868 have to adhere to an hourly duty cycle limitation of 10%, calculated every minute on a rolling 1-hour basis."* The firmware halts TX when the limit is hit unless the operator sets Override Duty Cycle Limit, which the docs warn *"Set to true to ignore the hourly duty cycle limit in Europe, which could result in regulatory violations. By default, this is false."* At the default SF11 preset a ~10-char message occupies ~354 ms of airtime (blocking ~3.5 s afterward under a 10% cap); LoRa throughput is ~1 kbps, and the mesh floods rebroadcasts — so aggressive rate limiting is essential. Broadcast ACKs are suppressed by design to avoid flooding.

### Prior art
- **aprstastic** (afourney; MIT; PyPI `aprstastic`; Python) is the reference bidirectional Meshtastic↔APRS gateway for licensed hams. It runs on stock Meshtastic (LongFast, 915 MHz ISM — not the ham band/mode) and uses **pre-registered device-ID↔callsign-SSID mappings** so all gated traffic is attributable to a licensed operator. Registration is over-the-air: per its README and Hamradio.my, *"Send a message with `aprs?` to any public channel… Reply with the command `!register CALLSIGN-SSID`"*; registrations are optionally beaconed to **MESHID-01** for cross-gateway roaming. It behaves like an iGate and identifies to APRS-IS level-2 servers with software version **APZMAG** (per the README, *"APZ designates an experimental application in development"*; MAG = Meshtastic-APRS Gateway; seen on-air, e.g. `…>APZMAG,WIDE1-1,qAR`). Addressing uses the **"CALLSIGN: message"** convention (recipient callsign then colon) because all mesh DMs go to the gateway node; if omitted, it replies to the last correspondent. Compliance stance: only mapped/registered devices transit; random LongFast traffic does not; all APRS traffic leaves in clear text.
- **jaredquinn/meshtastic-bridge** — a plugin-based Meshtastic bridge (APRS-IS, MQTT, Prometheus, logging), built specifically because the Meshtastic API "is not designed to handle multiple clients connected… simultaneously" — reinforcing the single-connection rule.
- Community deployments (e.g. the **W3HZU-12 "APRS"** club gateway) confirm the register/unregister DM workflow in production.
- SSID conventions (aprs.org): `-1..-4` digipeaters, `-9` mobile, `-10` internet/iGate-style, `-15` generic; aprstastic assigns per-user SSIDs on the mapped callsign. Gateways commonly present as an iGate.

### Part 97 / legal (the key issue)
- On amateur RF, **encryption is prohibited**. 47 CFR §97.113(a)(4) forbids *"messages encoded for the purpose of obscuring their meaning, except as otherwise provided herein."* Meshtastic's default channels are AES-encrypted (default PSK `AQ==`), so **content from default/encrypted Meshtastic channels must never be gated onto RF APRS.** Meshtastic's own ham/"Licensed Operator" mode disables encryption and sets the callsign as the long name.
- **Station identification** (§97.119(a)): the licensed operator must ID by callsign at least every 10 minutes — every gated RF transmission must be attributable to (and carry) a licensed callsign.
- Practical rule to copy from aprstastic: forward to RF APRS only messages from **pre-registered, licensed callsigns**, in clear text, mapped to a specific node — keeping the RF side compliant and attributable. (Note the US APRS RF channel is 144.390 MHz and EU is 144.800 MHz, both amateur-only, whereas 915 MHz Meshtastic is license-free ISM in the US.)

## Details

### Recommended plugin architecture
Layout (`plugins/aprs_bridge/`): `manifest.json`; `main.py`; `setup.py` (pip-installs `kiss3`, `ax253`, `aprs3`/`aprslib`, optionally `pyham_pe`); `db.py` (own SQLite for callsign↔node mappings, dedupe cache, pending-ACK tracking); `config.json` (TNC host/port/mode, gateway callsign-SSID, digi path, allowed channels, rate limits, region profile); and a `static/` page for registration management. Declare `"watchdog": true` since you run a persistent socket loop.

Threads / loops:
1. **TNC RX thread** — opens the KISS-over-TCP socket (`kiss3.TCPKISS`), serial KISS (`kiss3.SerialKISS`), or AGW (`pyham_pe`), decodes KISS→AX.25→APRS, and for APRS *messages* addressed to a registered callsign schedules `connection_manager.sendText(...)` to the mapped node via `asyncio.run_coroutine_threadsafe`. When the inbound message carries a `{NNN` id, transmit an APRS `ackNNN` back on RF.
2. **Mesh→APRS worker** — consumes MeshDash's incoming packets (subscribe to SSE `packet` events, or poll `meshtastic_data`/the `messages` table). For DMs to the gateway node from a **registered** node, parse the `CALLSIGN: text` convention, build the AX.25 UI/APRS message frame (`ax253`), KISS-encode, and transmit via the TNC. Track mesh-side delivery ACKs (MeshDash surfaces `message_status_update`).
3. **Watchdog heartbeat** coroutine (required for `watchdog:true`).

Cross-network concerns:
- **Callsign↔node mapping**: store `node_id (!hex) ↔ callsign-SSID` in the plugin DB; support aprstastic-style `!register`/`!unregister` DM commands and/or a MeshDash UI page. Gate strictly on this table (attribution/legality).
- **Loop & dedupe prevention**: keep a short-TTL cache of message signatures (source, addressee, text, msg-id) in both directions and drop duplicates. APRS re-sends until ACKed and the mesh floods rebroadcasts — both create loops without dedupe. Never re-gate a message you just originated (tag your own traffic).
- **ACK handling**: RF→mesh — after delivering to the node, send APRS `ackNNN` back to the APRS sender. Mesh→RF — retransmit the APRS message on the decaying APRS schedule until an `ackNNN` is heard, then stop; surface the mesh delivery ACK to the user.
- **Rate limiting**: per-direction and per-callsign token buckets (MeshDash's docs even show a `deque`-based limiter). Respect the LoRa duty cycle (hard-cap at 10% for EU) and APRS channel congestion (keep to roughly one message per several seconds; use at most WIDE1-1,WIDE2-1).

### TNC options summary
- **Primary: Direwolf KISS-over-TCP (127.0.0.1:8001)** via `kiss3.TCPKISS` + `ax253` — simplest, robust, and what most APRS Python code targets.
- **Direwolf AGW (127.0.0.1:8000)** via `pyham_pe` — use if you want connected-mode later or prefer AGW monitoring/`send_unproto` semantics.
- **Hardware serial KISS TNC** via `kiss3.SerialKISS('/dev/ttyUSB0', 1200)` — for a real transceiver + hardware TNC; add the MeshDash Linux user to `dialout`.
- **APRS-IS (optional)** via `aprs3`/`aprslib` TCP to a tier-2 server (`rotate.aprs2.net:14580`) with a valid passcode — an internet-only path; observe APRS-IS gating etiquette; this is not RF.

### Relevant libraries and maturity
- `kiss3` (python-aprs, Apache-2.0, Py3.6+, active) — KISS serial + TCP.
- `ax253` (python-aprs) — AX.25/APRS frame + address encode/decode.
- `aprs3` (python-aprs) — APRS encode/decode over KISS or APRS-IS, sync + async.
- `aprslib` (Rossen Georgiev) — mature APRS-IS parser/encoder.
- `pyham_pe` (MIT, Py3.7+) — full AGWPE client (connected + UNPROTO); pairs with the `paracon` terminal as a worked example.
- `aioax25` — asyncio AX.25/APRS (UI frames; can auto-KISS a KPC-3).
- `meshtastic` — official library; used indirectly through MeshDash.

### Reference documents
- APRS Protocol Reference v1.0.1 — aprs.org/doc/APRS101.PDF (ch. 14 = Message/Bulletin/Announcement format); WB4APR on-air notes — aprs.org/APRS-docs/PROTOCOL.TXT; SSID conventions — aprs.org/aprs11/SSIDs.txt.
- AGWPE TCP/IP API tutorial (LU7DID/SV2AGW) — on7lds.net/42/sites/default/files/AGWPEAPI.HTM; qsl.net agwpeapi.txt; `pyham_pe` docs — pyham-pe.readthedocs.io.
- KISS spec — ax25.net/kiss.aspx; Meshtastic overview / LoRa config / MQTT — meshtastic.org/docs.
- Prior art — github.com/afourney/aprstastic; github.com/jaredquinn/meshtastic-bridge.
- MeshDash — plugin dev: meshdash.co.uk/docs/?page=plugin-development; REST API: meshdash.co.uk/docs/?page=api-core; webhook: meshdash.co.uk/webhook.php.

## Recommendations
1. **Start with a one-way RF→mesh proof of concept** as a MeshDash plugin: `setup.py` installs `kiss3` + `ax253`; `init_plugin` opens `kiss3.TCPKISS` to Direwolf :8001 in an RX thread; decode APRS messages; if the addressee is a registered callsign, `sendText` to the mapped node. Verify end-to-end before adding TX.
2. **Add mesh→RF with strict gating**: implement the registration table + `!register`/`!unregister` DM flow (copy aprstastic), the `CALLSIGN:` addressing convention, AX.25 UI/APRS encoding, and RF transmit. **Gate on the registration table and clear-text only** — get this right before anything else.
3. **Add ACK handling and dedupe** in both directions, then **rate limiting / duty-cycle awareness**.
4. **Reuse aprstastic's MIT-licensed code and patterns directly** (mapping store, `!register` flow, MESHID-01 roaming beacon, APZMAG-style software ID) rather than reinventing; consider adopting its roaming-registry format for interoperability.
5. **Configuration to expose**: TNC mode/host/port, gateway callsign-SSID, digipeater path (default WIDE1-1,WIDE2-1), allowed mesh channel(s), max message rate, EU/US duty-cycle profile, and APRS-IS on/off + passcode.

**Benchmarks/thresholds that change the plan:**
- If you need **connected-mode AX.25** (BBS/Winlink), switch the TNC layer to `pyham_pe`/AGW (port 8000) instead of raw KISS.
- If the target radio is **internet-connected and you don't need RF APRS**, the **MQTT JSON downlink** or **APRS-IS** path can replace the TNC entirely.
- If you operate in **EU 868**, treat the 10% duty cycle as a hard cap in your rate limiter and do not enable Override Duty Cycle Limit.
- If you don't need a custom UI, the `POST /api/hook` webhook + an external script is a quick hack — but you lose real-time RX and should still use the plugin for production.

## Caveats
- **Legal compliance is the operator's responsibility.** Only licensed amateurs may gate to RF APRS; never forward encrypted/default-channel Meshtastic content to RF; ensure callsign ID at least every 10 minutes. Meshtastic on 915 MHz ISM (US) is license-free, but the APRS RF side (144.390 MHz US / 144.800 MHz EU) is amateur-only.
- The **AGWPE spec is dated December 2000** (SV2AGW/LU7DID, describing AGWPE ≥ 2000.20); Direwolf implements the same wire protocol but ignores the `P` login frame and has quirks (e.g. historically `KISSPORT` did not disable the default 8001). The tutorial also mislabels the `V` frame's ASCII code — the DataKind letter is unambiguously `V` (0x56).
- MeshDash plugins run **in-process with no sandbox** — an unhandled exception or a blocking call at module scope can destabilize the whole dashboard; follow the threading / `asyncio.to_thread` / `run_coroutine_threadsafe` rules precisely, and heartbeat the watchdog.
- Meshtastic's **~200-byte payload** and low LoRa throughput mean long APRS messages (up to 67 chars of text plus addressing overhead is fine, but chained/verbose content is not) will be truncated or must be split — design for brevity.
- The Meshtastic **MQTT JSON downlink is finicky** in community reports (needs a channel literally named `mqtt`, downlink enabled, JSON enabled, exact topic); treat it as secondary to the direct-library path MeshDash already uses.
- **aprstastic is explicitly alpha / "proof of concept"** under active development — excellent as a reference and importable library, but validate its behavior before relying on it in production.