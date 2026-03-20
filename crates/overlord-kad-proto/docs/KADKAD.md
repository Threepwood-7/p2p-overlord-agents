# KADKAD — Specification

Imported into `overlord-kad-proto/docs` from `c:\prj\p2p\kadkad\KADKAD.md`.
This copy is preserved as donor architecture and protocol background for the Overlord Kad transplant.
It is reference material, not the canonical Overlord workspace spec.

**Language**: Rust
**Protocol**: eMule Kademlia v2 (Kad2), IPv4 only
**Status**: Implementation in progress — this document is the authoritative high-level spec

---

## Table of Contents

1. [Goals](#1-goals)
2. [Non-Goals / Future Work](#2-non-goals--future-work)
3. [Reference Implementations](#3-reference-implementations)
4. [Workspace Structure](#4-workspace-structure)
5. [Crate Responsibilities](#5-crate-responsibilities)
6. [Protocol Overview](#6-protocol-overview)
7. [Routing Table](#7-routing-table)
8. [DHT Operations](#8-dht-operations)
9. [Obfuscation](#9-obfuscation)
10. [UPnP](#10-upnp)
11. [Local Index — Database Schema](#11-local-index--database-schema)
12. [REST API](#12-rest-api)
13. [Configuration](#13-configuration)
14. [Logging](#14-logging)
15. [Bootstrap](#15-bootstrap)
16. [File Sharing & Publishing](#16-file-sharing--publishing)
17. [Testing Strategy](#17-testing-strategy)
18. [Phased Implementation Plan](#18-phased-implementation-plan)
19. [Key Dependencies](#19-key-dependencies)
20. [Future Work](#20-future-work)
21. [Code Conventions](#21-code-conventions)

---

## 1. Goals

Build a fully wire-compatible eMule Kad2 client library and daemon in Rust that can:

- Join and participate in the live eMule Kad2 DHT network
- Search for files by keyword
- Find sources (peers) for a known file hash
- Fetch file notes (ratings/comments)
- Publish file availability and keywords into the DHT
- Accumulate a persistent, queryable local index of everything seen
- Expose all functionality via a REST API
- Run as a foreground process (lifecycle managed by external tools such as pm2)
- Be structured as a reusable library crate, not just an application

---

## 2. Non-Goals / Future Work

See [§20 Future Work](#20-future-work) for detailed notes on each item.

| Feature | Status |
|---|---|
| Kad1 (legacy protocol) | Intentionally omitted |
| ed2k server protocol (TCP to central servers) | Phase 2+ |
| ed2k peer TCP transfer (actual file download) | Phase 2 |
| AICH hash tree computation | Phase 2 |
| Firewall buddy system / NAT callback | Phase 3 |
| IPv6 | Future |
| GUI / system tray | Out of scope for this repo |
| CLI binary (interactive shell) | Separate project, possibly different language |

---

## 3. Reference Implementations

Three reference codebases were analysed prior to writing this spec:

| Repo | Path | Notes |
|---|---|---|
| eMule | `c:\prj\p2p\eMule-my\deps-repos\eMule\srchybrid\kademlia\` | Authoritative Kad2 wire format and protocol behaviour. Windows/MFC-only. |
| aMule | `c:\prj\p2p\amule\src\kademlia\` | Cross-platform port of eMule. Better code organisation. wxWidgets. |
| libed2k | `c:\prj\p2p\libed2k\src\kademlia\` | libtorrent-derived C++ library. Best architectural separation of the three. Boost/pre-C++11. |

The eMule source is ground truth for packet formats. aMule is ground truth for portable logic.
libed2k's `traversal_algorithm` / `rpc_manager` / `observer` pattern informs our async design.

---

## 4. Workspace Structure

```
kadkad/
├── Cargo.toml                  ← workspace root
├── KADKAD.md                   ← this file
├── KAD_PROTOCOL.md             ← wire-level Kad2 reference (packets, tags, semantics)
├── crates/
│   ├── kadkad-proto/           ← Kad2 wire codec: packet types, tag system, node ID
│   ├── kadkad-routing/         ← routing table: zone tree, k-buckets, contacts
│   ├── kadkad-net/             ← Tokio UDP transport, RPC manager, obfuscation
│   ├── kadkad-dht/             ← DHT operations: bootstrap, lookup, search, publish
│   ├── kadkad-index/           ← SQLite index: schema, queries, typed access
│   └── kadkad-node/            ← top-level library: assembles all crates, REST API
└── bin/
    └── kadkad/                 ← daemon binary (thin shell over kadkad-node)
        └── src/
            └── main.rs
```

### Dependency Graph

```
kadkad-proto
    ↑
kadkad-routing   (depends on kadkad-proto for NodeId, Contact types)
    ↑
kadkad-net       (depends on kadkad-proto + kadkad-routing)
    ↑
kadkad-dht       (depends on kadkad-net + kadkad-routing)
    ↑
kadkad-index     (standalone, depends only on kadkad-proto for file hash types)
    ↑
kadkad-node      (depends on all: kadkad-dht + kadkad-index, exposes REST API)
    ↑
bin/kadkad       (depends on kadkad-node only)
```

`kadkad-proto` and `kadkad-routing` have zero async, zero IO — they are pure data structures
and transformations. This makes them trivially unit-testable.

---

## 5. Crate Responsibilities

### `kadkad-proto`

- All Kad2 packet types as Rust enums/structs
- Binary encode/decode (`binrw` crate) — `&[u8]` ↔ `KadPacket`
- eMule tag system (typed name/value pairs: `TagName` × `TagValue`)
- `NodeId` — 128-bit (`[u8; 16]`) wrapper with XOR distance metric
- `KadUdpKey` — per-sender anti-spoofing key type
- Ed2k file hash type (`Ed2kHash` — MD4-based, 16 bytes)
- No `async`, no `tokio`, no networking of any kind
- Test vectors for all packet types (see §17)

### `kadkad-routing`

- `RoutingTable` — binary zone tree (eMule style, not flat k-buckets)
- `RoutingZone` — recursive zone node, splits when bin fills
- `RoutingBin` — k-bucket, max K=10 contacts
- `Contact` — node ID, IP, UDP port, TCP port, Kad version, UDP key, liveness type, last seen
- Zone splitting rules (eMule quirks faithfully reproduced — see §7)
- IP/subnet duplicate enforcement (max 1 per IP, max 10 per /24 subnet)
- No `async`, no `tokio`, no networking

### `kadkad-net`

- Tokio UDP socket wrapper
- Outbound rate limiter (configurable packets/sec)
- `RpcManager` — pending request map keyed by transaction ID, timeout handling
- Packet obfuscation layer (RC4, see §9)
- Receives raw UDP datagrams, attempts decrypt, dispatches to `RpcManager`
- `PacketTracker` — request/response correlation, per-IP flood protection

### `kadkad-dht`

- `Bootstrap` — load nodes.dat, send initial HELLO/PING, populate routing table
- `NodeLookup` — iterative find_node traversal (ALPHA=3 parallel queries)
- `KeywordSearch` — iterative search returning `Stream<Item = SearchResult>`
- `SourceSearch` — iterative source lookup returning `Stream<Item = SourceResult>`
- `NotesSearch` — iterative notes lookup
- `Publish` — keyword publish, source publish, notes publish
- Scheduled republish timer
- Firewall UDP tester (periodic, determines if node is reachable)

### `kadkad-index`

- SQLite via `sqlx` (async, compile-time checked queries)
- All table definitions and migrations (see §11)
- Typed query functions: `insert_file`, `find_files_by_name`, `get_sources`, etc.
- No knowledge of Kad2 protocol — takes plain typed structs

### `kadkad-node`

- `Node` struct — owns all subsystems, single entry point
- Loads and validates `config.toml`
- Starts/stops `kadkad-dht` and `kadkad-index`
- REST API server (`axum`, see §12)
- UPnP port mapping via `igd` crate (see §10)
- Hot-reload of log level via REST
- Shared file list management (scan, hash, publish, re-verify)

### `bin/kadkad`

- Parse CLI args (`clap`)
- Resolve config file path
- Construct and start `Node`
- Handle `SIGTERM`/`SIGINT` for clean shutdown
- No business logic — thin shell only

---

## 6. Protocol Overview

### Kad2 Packet Types

All packets use the eMule `OP_KADEMLIAHEADER` (0xE4) or obfuscated header.
For byte-level layouts, verified tag IDs, and packet-family notes, see `KAD_PROTOCOL.md`.

| Packet | Direction | Purpose |
|---|---|---|
| `KADEMLIA2_BOOTSTRAP_REQ` | out | Request bootstrap contact list |
| `KADEMLIA2_BOOTSTRAP_RES` | in | Receive bootstrap contacts |
| `KADEMLIA2_HELLO_REQ` | out | Announce presence, initiate verification |
| `KADEMLIA2_HELLO_RES` | in | Accept hello, return our info |
| `KADEMLIA2_HELLO_RES_ACK` | out | Acknowledge hello response |
| `KADEMLIA2_REQ` | out | Generic find_node / lookup |
| `KADEMLIA2_RES` | in | Response with closest contacts |
| `KADEMLIA2_SEARCH_KEY_REQ` | out | Search by keyword hash |
| `KADEMLIA2_SEARCH_SOURCE_REQ` | out | Search for file sources |
| `KADEMLIA2_SEARCH_NOTES_REQ` | out | Search for file notes |
| `KADEMLIA2_SEARCH_RES` | in | Search response (files/sources/notes) |
| `KADEMLIA2_PUBLISH_KEY_REQ` | out | Publish keyword index entry |
| `KADEMLIA2_PUBLISH_SOURCE_REQ` | out | Publish source availability |
| `KADEMLIA2_PUBLISH_NOTES_REQ` | out | Publish a note/rating |
| `KADEMLIA2_PUBLISH_RES` | in | Publish acknowledgement |
| `KADEMLIA2_PING` | out | Liveness check |
| `KADEMLIA2_PONG` | in | Liveness response |
| `KADEMLIA2_FIREWALLUDP` | both | UDP reachability test |

### Kad1 Policy

> **KAD1_IGNORED**: Kad1 (legacy protocol) nodes are silently ignored. Packets with Kad1
> opcodes are dropped without processing. No Kad1 packet types are implemented.
> See KADKAD.md §20 Future Work if this changes.

This simplifies the implementation significantly. The live network (2024+) is overwhelmingly Kad2.

### Protocol Constants

```rust
pub const K: usize = 10;               // k-bucket size
pub const ALPHA: usize = 3;            // parallel lookup queries
pub const KBASE: usize = 4;            // zone splitting base
pub const KK: usize = 5;               // peer selection parameter
pub const SEARCH_TIMEOUT_SECS: u64 = 45;
pub const STORE_TIMEOUT_SECS: u64 = 140;
pub const REPUBLISH_INTERVAL_SECS: u64 = 18_000; // ~5 hours, configurable
pub const KAD_VERSION: u8 = 9;         // our announced Kad version
```

---

## 7. Routing Table

### Structure

Binary zone tree (eMule style), not a flat array of k-buckets.

- Root zone covers the entire 128-bit address space
- Each zone is either a **leaf** (holds a `RoutingBin`) or an **internal node** (has two child zones)
- A leaf splits into two children when its bin fills AND the split conditions are met
- Zones are indexed by `ZoneIndex` (a `NodeId`) — the path from root is encoded as bits

### Split Conditions (faithfully from aMule `RoutingZone::CanSplit`)

A zone may split if ALL of the following hold:

1. The bin has reached capacity (K=10 contacts)
2. Zone depth < 127
3. The zone contains our own node ID (i.e. we are in this zone's address range), **OR** the zone has depth < KBASE (=4)
4. Total contact count across all zones < `max_routing_table_size` (configurable, default 12000)

### Contact Liveness Types

```rust
pub enum ContactType {
    Active,     // responded recently
    Inactive,   // not responded, still in table, eligible for ping
    Dead,       // failed multiple pings, candidate for replacement
}
```

### IP/Subnet Limits

- Maximum 1 contact per IP address (globally across all bins)
- Maximum 10 contacts per /24 subnet (globally)
- LAN addresses (RFC1918) exempt from subnet limits

### Contact Fields

```rust
pub struct Contact {
    pub id: NodeId,
    pub ip: Ipv4Addr,
    pub udp_port: u16,
    pub tcp_port: u16,
    pub kad_version: u8,
    pub udp_key: KadUdpKey,
    pub verified: bool,
    pub contact_type: ContactType,
    pub last_seen: SystemTime,
    pub created_at: SystemTime,
}
```

---

## 8. DHT Operations

### Bootstrap

1. Read contacts from nodes.dat (see §15)
2. Send `KADEMLIA2_BOOTSTRAP_REQ` to first N reachable contacts
3. Process responses, add contacts to routing table
4. Trigger initial zone refresh once routing table has ≥ 1 contact

### Node Lookup (Iterative Find)

1. Get K closest contacts to target from local routing table (XOR distance)
2. Send `KADEMLIA2_REQ` to ALPHA=3 closest unqueried contacts in parallel
3. Collect responses, update closest set
4. Repeat until no closer nodes found or all K closest have been queried
5. Return K closest found

### Keyword Search

1. Hash keyword with MD4 → target `NodeId`
2. Run node lookup toward target
3. At each step, also send `KADEMLIA2_SEARCH_KEY_REQ`
4. Collect `KADEMLIA2_SEARCH_RES` responses → parse into `SearchResult` structs
5. Emit each result on the search `Stream`
6. Write all results to index automatically
7. Search ends after `SEARCH_TIMEOUT_SECS` (45s) or no new nodes found

### Source Search

Same as keyword search but uses `KADEMLIA2_SEARCH_SOURCE_REQ`. Target is the file's `Ed2kHash`.
Kad2 source search requests require the file size on the wire, so the daemon resolves size from
the local `files` table before starting the network search. If the file hash is not indexed locally
or the indexed size is zero, the REST request fails with `400` and no DHT search is started.
Results include peer IP/port and are stored in `sources`. The `kad_version` column remains nullable
because Kad source-search result tags do not carry a Kad version byte.

### Notes Search

Same pattern, `KADEMLIA2_SEARCH_NOTES_REQ`. Kad2 notes search also requires the file size on the
wire, so the daemon uses the indexed size from the local `files` table and fails the API request if
that size is unavailable. Notes results are stored in `notes`; the result entry hash is treated as
the note author's Kad/source ID and is persisted as `author_hash`.

### Publish

On file add (automatic) and on schedule (configurable interval, default 18000s):

1. `KADEMLIA2_PUBLISH_SOURCE_REQ` — announce we have the file
2. `KADEMLIA2_PUBLISH_KEY_REQ` — publish keyword→hash mapping for each keyword
3. (Optional) `KADEMLIA2_PUBLISH_NOTES_REQ` — if we have a note for the file

### Concurrent Search Limit

Maximum simultaneous active searches: configurable, default 5.
New search requests are queued if limit is reached.

### Outbound Rate Limit

Maximum outbound Kad2 UDP packets per second: configurable, default 50.
This is a global limit across all operations.

---

## 9. Obfuscation

eMule uses an RC4-based obfuscation layer on all UDP packets (Kad2 v6+).
Most modern nodes on the live network use obfuscation. Without it, many nodes will ignore us.

### Behaviour

- Default: **enabled**
- Config: `[obfuscation] enabled = true`
- When enabled: all outbound packets are obfuscated; all inbound packets are tried as obfuscated first, then plain if decrypt fails
- When disabled: plain packets only (useful for debugging, Wireshark capture)

### Key Negotiation

Each node pair negotiates a session key via the `KADEMLIA2_HELLO_REQ/RES` exchange.
Keys are stored per-contact in the `KadUdpKey` field of `Contact`.

### Note

The obfuscation protocol is poorly documented. The authoritative implementation is in
`eMule: KademliaUDPListener.cpp` and `aMule: KademliaUDPListener.cpp`.
libed2k also implements it in `dht_tracker.cpp`.

---

## 10. UPnP

Uses the `igd` crate (pure Rust, UPnP IGD protocol).

### Behaviour

1. On startup, if `[upnp] enabled = true`: discover gateway
2. Request external UDP port mapping for our Kad2 port
3. If successful: log the external IP/port, use it as our announced address
4. On shutdown: release the port mapping

### Configuration

```toml
[upnp]
enabled = true
bind_ip = "0.0.0.0"    # local interface for UPnP discovery multicast
gateway = "auto"        # "auto" = discover via SSDP, or explicit e.g. "192.168.1.1"
```

The `bind_ip` allows selecting which network interface to use for UPnP.
The `gateway` override is for environments where SSDP discovery fails.

---

## 11. Local Index — Database Schema

SQLite via `sqlx`. Database file: configurable, default `%APPDATA%\kadkad\index.db`.
Schema migrations managed by `sqlx migrate`.

**Goal**: accumulate a permanent, ever-growing index of everything seen on the network.
Nothing expires. The user manages the database size.

### Tables

```sql
-- Canonical file record. Ed2k hash is the unique identity.
CREATE TABLE files (
    hash            BLOB(16) PRIMARY KEY NOT NULL,
    size            INTEGER  NOT NULL,
    first_seen      INTEGER  NOT NULL,   -- unix timestamp (seconds)
    last_seen       INTEGER  NOT NULL,
    availability    INTEGER  NOT NULL DEFAULT 0  -- source count, updated per sighting
) STRICT;

-- A file can appear under many names on the network.
CREATE TABLE file_names (
    id              INTEGER  PRIMARY KEY AUTOINCREMENT,
    file_hash       BLOB(16) NOT NULL REFERENCES files(hash),
    name            TEXT     NOT NULL,
    first_seen      INTEGER  NOT NULL,
    UNIQUE(file_hash, name)
) STRICT;

-- eMule metadata tags: codec, bitrate, duration, format, artist, album, etc.
-- One row per tag. Queryable.
CREATE TABLE file_tags (
    file_hash       BLOB(16) NOT NULL REFERENCES files(hash),
    tag_name        TEXT     NOT NULL,
    tag_value       TEXT     NOT NULL,
    PRIMARY KEY (file_hash, tag_name)
) STRICT;

-- Search history.
CREATE TABLE searches (
    id              INTEGER  PRIMARY KEY AUTOINCREMENT,
    query           TEXT     NOT NULL,
    search_type     TEXT     NOT NULL CHECK(search_type IN ('keyword','source','notes')),
    started_at      INTEGER  NOT NULL,
    completed_at    INTEGER,                   -- NULL while in progress
    result_count    INTEGER  NOT NULL DEFAULT 0
) STRICT;

-- Which files were found by which search.
CREATE TABLE search_results (
    search_id       INTEGER  NOT NULL REFERENCES searches(id),
    file_hash       BLOB(16) NOT NULL REFERENCES files(hash),
    seen_at         INTEGER  NOT NULL,
    PRIMARY KEY (search_id, file_hash)
) STRICT;

-- Full history of peers known to have a file.
-- No UNIQUE constraint — we keep all sightings.
CREATE TABLE sources (
    id              INTEGER  PRIMARY KEY AUTOINCREMENT,
    file_hash       BLOB(16) NOT NULL REFERENCES files(hash),
    ip              TEXT     NOT NULL,
    udp_port        INTEGER  NOT NULL,
    tcp_port        INTEGER  NOT NULL,
    kad_version     INTEGER,
    seen_at         INTEGER  NOT NULL
) STRICT;
CREATE INDEX idx_sources_hash ON sources(file_hash);

-- File notes and ratings from the network.
CREATE TABLE notes (
    id              INTEGER  PRIMARY KEY AUTOINCREMENT,
    file_hash       BLOB(16) NOT NULL REFERENCES files(hash),
    rating          INTEGER  CHECK(rating BETWEEN 0 AND 5),
    comment         TEXT,
    author_hash     BLOB(16),                  -- Kad node ID of commenter
    seen_at         INTEGER  NOT NULL,
    UNIQUE(file_hash, author_hash)
) STRICT;

-- Files we are sharing / publishing.
CREATE TABLE shared_files (
    hash            BLOB(16) NOT NULL REFERENCES files(hash),
    path            TEXT     NOT NULL,
    added_at        INTEGER  NOT NULL,
    last_verified   INTEGER,                   -- last time we confirmed the file exists
    PRIMARY KEY (hash)
) STRICT;
```

### Index Design Notes

- `file_tags` is one row per tag (queryable by tag name, e.g. `WHERE tag_name='codec'`)
- `sources` is append-only (full history, no deduplication by IP)
- `notes` deduplicates by `(file_hash, author_hash)` — one note per author per file
- `shared_files.last_verified` is set to NULL when the file is found missing; the daemon removes the row and logs a warning
- FTS (full-text search) index on `file_names.name` is recommended for large databases — add in a later migration when needed

---

## 12. REST API

Framework: `axum`. Bind: configurable (default `127.0.0.1:7373`). No authentication.
All endpoints prefixed with `/api/v1`.

### Node / Status

```
GET  /api/v1/status
     → { node_id, ip, port, kad_version, routing_table_size,
         active_searches, uptime_secs, obfuscation_enabled }

GET  /api/v1/routing-table
     → { zone_count, contact_count, contacts: [...] }

GET  /api/v1/routing-table/stats
     → { contacts_by_version: {...}, zones: N, bins: N }
```

### Search

```
POST /api/v1/search/keyword
     Body: { "query": "ubuntu 22.04" }
     → 202 { "search_id": "uuid" }

POST /api/v1/search/source
     Body: { "hash": "aabbcc..." }    ← hex Ed2k hash
     Requires the file to already exist in the local index with non-zero `files.size`.
     Otherwise returns 400 because Kad2 source search needs the file size on the wire.
     → 202 { "search_id": "uuid" }

POST /api/v1/search/notes
     Body: { "hash": "aabbcc..." }
     Requires the file to already exist in the local index with non-zero `files.size`.
     Otherwise returns 400 because Kad2 notes search needs the file size on the wire.
     → 202 { "search_id": "uuid" }

GET  /api/v1/search/{id}/events
     → SSE stream; each event is a JSON SearchResult
     Stream closes when search completes or times out.
     Event types: "result", "complete", "error"

GET  /api/v1/search/{id}/results
     → { search_id, query, search_type, started_at, completed_at,
         result_count, results: [...] }
     Available immediately; returns partial results if search in progress.

GET  /api/v1/searches
     → [ { id, query, search_type, started_at, completed_at, result_count }, ... ]
     Query params: ?limit=50&offset=0
```

### Index Queries

```
GET  /api/v1/index/files
     Query params: ?name=ubuntu&limit=50&offset=0
     → [ { hash, size, names: [...], first_seen, last_seen, availability }, ... ]

GET  /api/v1/index/files/{hash}
     → { hash, size, names, tags, first_seen, last_seen, availability }

GET  /api/v1/index/files/{hash}/sources
     → [ { ip, udp_port, tcp_port, kad_version, seen_at }, ... ]
     `kad_version` may be null for Kad source-search results because the search-result tag set
     does not include a Kad version field.

GET  /api/v1/index/files/{hash}/notes
     → [ { rating, comment, author_hash, seen_at }, ... ]
     `author_hash` is the Kad/source ID carried as the result entry ID in `KADEMLIA2_SEARCH_RES`.

GET  /api/v1/index/searches
     → search history, same as /api/v1/searches
```

### Sharing & Publishing

```
GET  /api/v1/share
     → [ { hash, path, size, added_at, last_verified }, ... ]

POST /api/v1/share
     Body: { "path": "C:\\files\\ubuntu.iso" }
     Daemon computes hash, registers file, triggers publish.
     → 202 { "hash": "aabbcc...", "size": N }

DELETE /api/v1/share/{hash}
     Removes from share list. Stops publishing. Does NOT delete from index.
     → 204

POST /api/v1/publish/{hash}
     Manually trigger re-publish for a shared file.
     → 202
```

### Configuration

```
GET  /api/v1/config
     → current config as JSON (read-only view)

POST /api/v1/config/log-level
     Body: { "level": "debug" }    ← hot-reloadable
     → 200
```

### SSE Search Result Format

```json
event: result
data: {
  "hash": "aabbccdd...",
  "names": ["ubuntu-22.04.iso"],
  "size": 1234567890,
  "availability": 42,
  "tags": { "type": "iso", "bitrate": null }
}

event: complete
data: { "result_count": 87, "duration_ms": 38412 }

event: error
data: { "message": "search timed out" }
```

---

## 13. Configuration

File: `%APPDATA%\kadkad\config.toml` (Windows), `~/.config/kadkad/config.toml` (Linux).
Path can be overridden with `--config` CLI flag.

```toml
[node]
# Kad2 node ID. "auto" = generate once, persist to disk.
# Or explicit 32-char hex string.
id = "auto"

# UDP port for Kad2. 0 = random (chosen at startup, persisted).
port = 4672

# Local IP to bind the UDP socket.
bind_ip = "0.0.0.0"


[obfuscation]
# RC4 obfuscation. Default on. Disable for debugging/Wireshark.
enabled = true


[upnp]
enabled = true
# Local interface for UPnP SSDP discovery.
bind_ip = "0.0.0.0"
# "auto" = discover gateway via SSDP. Or explicit IP e.g. "192.168.1.1".
gateway = "auto"


[api]
# REST API bind address. Keep on localhost unless you know what you're doing.
bind_ip = "127.0.0.1"
port = 7373


[dht]
# Path to nodes.dat for bootstrap. "" = auto-detect.
# Auto-detect checks (in order):
#   1. ./nodes.dat
#   2. %APPDATA%\eMule\config\nodes.dat
#   3. %APPDATA%\aMule\nodes.dat
#   4. Hardcoded bootstrap nodes compiled into the binary
nodes_dat = ""

# Additional bootstrap nodes (ip:port pairs), tried alongside nodes.dat.
bootstrap_nodes = []

# Maximum contacts in the routing table.
max_routing_table_size = 12000

# Maximum simultaneous active searches.
max_concurrent_searches = 5

# Maximum outbound Kad2 UDP packets per second (global, all operations).
max_outbound_pps = 50

# Harvest-first search fanout and result caps.
search_phase2_fanout = 50
keyword_result_cap = 5000
source_result_cap = 1000
notes_result_cap = 1000

# How often to re-publish our shared files (seconds). Default ~5 hours.
republish_interval_secs = 18000


[index]
path = "%APPDATA%\\kadkad\\index.db"


[log]
# Log level: error | warn | info | debug | trace
# Hot-reloadable via POST /api/v1/config/log-level
level = "info"

# Log file path.
file = "%APPDATA%\\kadkad\\kadkad.log"

# Maximum size of a single log file in megabytes before rotation.
max_size_mb = 10

# Number of rotated backup files to keep.
max_backups = 3
```

### Config Notes

- `node.id = "auto"` generates a stable random ID on first run, persists to `%APPDATA%\kadkad\node_id`
- `node.port = 0` selects a random available port, persists it to `%APPDATA%\kadkad\port`
- All `%APPDATA%` references are resolved at runtime via the OS environment
- Only `log.level` is hot-reloadable; all other changes require daemon restart
- Search defaults are intentionally indexer-oriented: fan out broadly in phase 2, collect a lot,
  and filter later from the local index rather than narrowing aggressively during network search

---

## 14. Logging

Library: `tracing` + `tracing-subscriber` + `tracing-appender`.

- Log to file only (no stdout). Use rolling file appender with size-based rotation.
- `tracing-appender` handles the file sink; configure max file size and backup count.
- Structured spans: each active search has its own span with `search_id` and `query` fields.
- Key events to instrument:
  - Node start/stop
  - Bootstrap attempt / success / failure
  - Contact added / removed / rejected
  - Search started / result received / completed / timed out
  - Publish sent / acknowledged
  - Obfuscation failure (packet dropped)
  - UPnP mapping success / failure
  - File shared / verified / removed
  - REST API requests (at `debug` level)

---

## 15. Bootstrap

### Node ID Verification (from libed2k / eMule)

Each node's ID must be consistent with its IP address (Kad v9+ enforces this).
The first 3 bytes of the node ID are derived from `CRC32C(masked_ip)`.
Nodes that fail this check are still accepted but marked as unverified.

### Bootstrap Source Priority

1. `[dht] nodes_dat` config value (if set and file exists)
2. `.\nodes.dat` in current working directory
3. `%APPDATA%\eMule\config\nodes.dat` (if exists)
4. `%APPDATA%\aMule\nodes.dat` (if exists)
5. Hardcoded list compiled into the binary (maintained as a const array)

### Bootstrap Failure Handling

If all bootstrap sources fail or return no responding contacts:
- Enter retry loop with exponential backoff (1s, 2s, 4s, ... max 60s)
- Surface error via `GET /api/v1/status` (`"state": "bootstrapping_failed"`)
- Keep retrying indefinitely until at least one contact responds

### nodes.dat Formats

Support both:
- **eMule binary format** (version 2 and version 3 bootstrap edition)
- **Plain text format**: one `ip:port` per line (our own simple format for easy editing)

---

## 16. File Sharing & Publishing

### Adding a File

Via `POST /api/v1/share { "path": "..." }`:

1. Daemon reads the file, computes Ed2k hash (MD4-based, chunked)
2. Inserts into `files` table (or updates `last_seen` if already known)
3. Inserts filename into `file_names`
4. Inserts into `shared_files`
5. Immediately triggers publish (keyword + source)
6. Returns `{ hash, size }`

### Removing a File

Via `DELETE /api/v1/share/{hash}`:

1. Removes row from `shared_files`
2. Stops re-publishing (removed from publish schedule)
3. File record remains in `files`, `file_names`, `file_tags` — the index is permanent
4. Returns 204

### File Verification

On each scheduled republish cycle, the daemon:

1. Reads `shared_files` where `last_verified` is oldest
2. Checks each file still exists at its path
3. If exists: update `last_verified`, republish
4. If missing: log a warning, remove from `shared_files`

### Publish Schedule

- On file add: immediate publish
- Scheduled: every `republish_interval_secs` (default 18000s ≈ 5 hours)
- Manual: `POST /api/v1/publish/{hash}`

---

## 17. Testing Strategy

### Unit Tests (no network)

Every crate has `#[cfg(test)]` modules.

- `kadkad-proto`: encode/decode round-trips for every packet type using test vectors
- `kadkad-routing`: zone split/merge, contact add/remove, IP limit enforcement, XOR distance ordering
- `kadkad-index`: schema migrations, all query functions against in-memory SQLite

### Test Vectors

Packet test vectors are stored as binary files in `kadkad-proto/tests/vectors/`.
Vectors are generated from reference implementations (eMule/aMule/libed2k) or captured from
the live network via Wireshark.

Each vector file is named `{packet_type}_{variant}.bin` and has a corresponding
`{packet_type}_{variant}.json` with the expected decoded struct.

### Integration Tests (live network, opt-in)

Marked with `#[ignore]` — not run in CI by default. Run manually with:

```
cargo test -- --ignored
```

- Bootstrap test: start a node, bootstrap from nodes.dat, verify routing table has ≥ 10 contacts within 60s
- Search test: search for a known keyword, verify ≥ 1 result within 90s
- Ping test: ping a known stable node, verify PONG received

### Mock Transport

`kadkad-net` exposes a `Transport` trait. A `MockTransport` implementation is provided for
testing `kadkad-dht` operations without real UDP sockets. Supports:
- Injecting fake incoming packets
- Capturing outgoing packets for assertion
- Simulated packet loss and delay

---

## 18. Phased Implementation Plan

### Phase 1 — Codec & Routing Table

Crates: `kadkad-proto`, `kadkad-routing`

- [ ] Workspace setup, `Cargo.toml`, CI skeleton
- [ ] `NodeId` type with XOR metric, distance functions
- [ ] `Ed2kHash` type
- [ ] `KadUdpKey` type
- [ ] eMule tag system (all tag types)
- [ ] All Kad2 packet structs with `binrw` encode/decode
- [ ] Test vectors for all packet types
- [ ] `Contact` struct
- [ ] `RoutingBin` (k-bucket)
- [ ] `RoutingZone` (binary tree, split logic)
- [ ] `RoutingTable` (owns root zone, provides `add_contact`, `get_closest`)
- [ ] IP/subnet limit enforcement
- [ ] Full unit test coverage for routing table

**Milestone**: Can decode any Kad2 packet from a binary buffer. Can build and query a routing
table without any network. All unit tests pass.

### Phase 2 — Transport & RPC

Crate: `kadkad-net`

- [ ] `Transport` trait + `UdpTransport` (Tokio)
- [ ] `MockTransport` for testing
- [ ] Outbound packet rate limiter
- [ ] `RpcManager` — transaction IDs, pending request map, timeout handling
- [ ] Obfuscation layer (RC4 encrypt/decrypt)
- [ ] Incoming packet demux (try obfuscated → try plain → drop)
- [ ] `PacketTracker` — per-IP flood detection
- [ ] Integration test: send PING to a real node, receive PONG

**Milestone**: Can exchange packets with a real eMule node on the network.

### Phase 3 — DHT Operations

Crate: `kadkad-dht`

- [ ] Bootstrap algorithm
- [ ] Iterative node lookup (find_node)
- [ ] Keyword search → `Stream<Item = SearchResult>`
- [ ] Source search → `Stream<Item = SourceResult>`
- [ ] Notes search
- [ ] Publish (keyword, source, notes)
- [ ] Publish scheduler (interval timer)
- [ ] Firewall UDP tester
- [ ] Integration tests (all marked `#[ignore]`)

**Milestone**: Can join the live Kad2 network, search for files, find sources, publish.

### Phase 4 — Index

Crate: `kadkad-index`

- [ ] SQLite schema (all tables, migrations)
- [ ] `IndexStore` struct with all query methods
- [ ] In-memory SQLite for unit tests
- [ ] Integration with DHT: auto-save all search results

**Milestone**: All search results persisted to SQLite. Index queryable.

### Phase 5 — Node & REST API

Crate: `kadkad-node`

- [ ] `Config` loading from TOML
- [ ] `Node` struct (owns all subsystems)
- [ ] UPnP setup via `igd`
- [ ] `axum` REST API (all endpoints)
- [ ] SSE search event streaming
- [ ] Hot-reload log level
- [ ] Shared file management (add, remove, verify, hash computation)
- [ ] Graceful shutdown

**Milestone**: Full daemon. Start with config, query via REST, results in index.

### Phase 6 — Binary

Crate: `bin/kadkad`

- [ ] `clap` CLI: `--config`, `--log-level`
- [ ] Config path resolution and default fallback
- [ ] Signal handling (SIGTERM/SIGINT → clean shutdown)
- [ ] Exit codes (0 = clean, 1 = config error, 2 = network error)

**Milestone**: Shippable daemon binary. Works with pm2 or equivalent.

### Phase 7+ — Download (separate planning)

Crate: TBD (likely `kadkad-transfer` in this workspace)

- ed2k peer TCP protocol
- Slot negotiation
- Chunk request/response
- MD4 + AICH verification
- Download queue
- Integration with index (mark files as downloaded)

---

## 19. Key Dependencies

```toml
# Async runtime
tokio = { version = "1", features = ["full"] }

# Binary packet parsing (declarative, derive macros)
binrw = "0.14"

# Byte buffer management
bytes = "1"

# Web framework (REST API)
axum = "0.7"
axum-extra = "0.9"          # SSE support
tower = "0.4"
tower-http = "0.5"

# SQLite (async, compile-time checked queries)
sqlx = { version = "0.7", features = ["sqlite", "runtime-tokio", "migrate"] }

# Error handling
thiserror = "1"
anyhow = "1"

# Logging / tracing
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tracing-appender = "0.2"

# Config
serde = { version = "1", features = ["derive"] }
toml = "0.8"

# UPnP port mapping
igd = "0.12"

# Crypto (obfuscation)
rc4 = "0.1"                 # RC4 stream cipher
crc32c = "0.6"              # CRC32C for node ID verification

# MD4 (ed2k file hashing)
md4 = "0.10"

# Random number generation
rand = "0.8"

# CLI argument parsing
clap = { version = "4", features = ["derive"] }

# UUID for search IDs
uuid = { version = "1", features = ["v4"] }

# Async streaming
async-stream = "0.3"
tokio-stream = "0.1"
```

---

## 20. Future Work

### Kad1 Support

> **FUTURE(kad1)**: Kad1 (legacy protocol, opcodes 0x01-0x1F range) is intentionally not
> implemented. At time of writing (2026), the live network is overwhelmingly Kad2. Implementing
> Kad1 would add significant complexity with minimal benefit. If ever needed, it would require
> a separate packet parser, separate routing table update rules, and a version negotiation layer.

### ed2k Server Protocol

> **FUTURE(server)**: The eMule TCP server protocol (connecting to central `server.met` nodes)
> is out of scope for this library. The server protocol can be implemented as a separate crate
> in this workspace (`kadkad-server`) without affecting the Kad2 core.

### Buddy System / Firewall NAT Traversal

> **FUTURE(buddy)**: The FINDBUDDY / CALLBACK mechanism allows firewalled nodes to receive
> incoming connections via a "buddy" relay node. Deferred to Phase 3. When implemented:
> - `KADEMLIA2_FINDBUDDY_REQ/RES` packet types (already reserved in proto)
> - Buddy selection logic in routing table
> - Callback handling in the RPC layer
> - Config: `[dht] buddy_enabled = true`

### IPv6

> **FUTURE(ipv6)**: All address types currently use `Ipv4Addr`. IPv6 would require
> dual-stack socket handling and separate routing table instances.

### AICH Hash Tree

> **FUTURE(aich)**: The Advanced Intelligent Corruption Handler hash tree is needed for
> chunk-level file verification during download. Deferred to Phase 7 (download crate).

### UPnP v2 / NAT-PMP

> **FUTURE(nat-pmp)**: NAT-PMP and PCP (Port Control Protocol) are alternatives to UPnP IGD.
> The `igd` crate supports IGD v1/v2. NAT-PMP would require a separate crate.

---

## 21. Code Conventions

### Kad1 / Future Work Markers

Use these comment markers so they are grep-able:

```rust
// KAD1_IGNORED: <reason>
// FUTURE(tag): <description>
```

### Error Handling

- `kadkad-proto`, `kadkad-routing`, `kadkad-index`: use `thiserror` for typed errors
- `kadkad-net`, `kadkad-dht`: typed errors + `anyhow` for context in call sites
- `kadkad-node`, `bin/kadkad`: `anyhow` throughout

Never `.unwrap()` in non-test code. Use `expect("reason")` only where a panic is the
correct response to an invariant violation.

### Packet Parsing

All packet parsing returns `Result<T, ProtoError>`. No panics on malformed input.
Malformed or unrecognised packets from the network are logged at `debug` level and dropped.

### Async

- All async code targets Tokio exclusively
- No `async_std` or other runtimes
- Prefer `tokio::sync::mpsc` channels for cross-task communication
- Use `CancellationToken` from `tokio-util` for search/operation cancellation

### Windows Paths

Resolve `%APPDATA%` at runtime via `std::env::var("APPDATA")`. Do not hardcode paths.
Use `std::path::PathBuf` everywhere, not string concatenation.

### Versioning

Follow semver. Until v1.0.0, breaking API changes are permitted between minor versions.
The wire protocol is always Kad2-compatible regardless of library version.
