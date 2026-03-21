# overlord-tools

`overlord-tools` is the workspace home for executable developer and end-to-end tools.

The first tool is `minirupnpc`, a small NAT/UPnP diagnostic executable built on top of
`overlord-agent-nat` public APIs. It is intended for real-network troubleshooting and
mapping verification without launching the full agent runtime.

## Build

```bat
cargo build -p overlord-tools --bin minirupnpc
```

This produces `minirupnpc.exe` in the workspace target directory on Windows.

## Usage

Show help:

```bat
cargo run -p overlord-tools --bin minirupnpc -- --help
```

Create the standard test mappings on the VPN-bound interface:

```bat
cargo run -p overlord-tools --bin minirupnpc -- map --bind-ip 10.54.220.34
```

Keep the mappings alive for inspection:

```bat
cargo run -p overlord-tools --bin minirupnpc -- map --bind-ip 10.54.220.34 --hold-secs 15
```

Force the lower-level SSDP bind override used during discovery debugging:

```bat
cargo run -p overlord-tools --bin minirupnpc -- map --bind-ip 10.54.220.34 --ssdp-bind-ip 0.0.0.0
```

Delete the standard test mappings:

```bat
cargo run -p overlord-tools --bin minirupnpc -- cleanup --bind-ip 10.54.220.34
```

## Behavior

- `map` creates the standard test mappings for:
  - UDP `41000` named `kad`
  - TCP `41001` named `ed2k`
- `cleanup` removes those standard test mappings for the selected backend/config.
- JSON NAT status snapshots are printed to stdout.
- Tracing and library diagnostics are printed to stderr.

When validating real UPnP behavior, always confirm the live mappings with:

```bat
miniupnpc.exe -l
```

The workspace currently patches `rupnp` and `ssdp-client` to local instrumented checkouts,
so `minirupnpc` automatically exercises those local debugging changes.

## Future Tools

`overlord-tools` is intentionally a multi-tool crate. Additional executables can be added
under `src/bin/` over time while reusing shared helper modules from `src/`.
