# streaming/

Simulated Bengaluru order + GPS Kafka pipeline. Full write-up, recorded 1M-event numbers and
interview Q&A live in [`../docs/streaming.md`](../docs/streaming.md); the interface is
[`../docs/CONTRACTS.md`](../docs/CONTRACTS.md) section 7.

Quickstart (native, no Docker, isolated ports):

```bash
make setup        # once: venv + fetch JDK/Kafka into .tools/
make stream-up    # Kafka 127.0.0.1:9094 + Postgres 127.0.0.1:5436
make stream-test  # unit tests (no Kafka needed)
make stream-million    # produce + consume ~1,000,000 simulated events
make stream-down
```

Entry point: `python -m streaming.run --events N`. Topics `orders.events` + `gps.pings`; consumer
lands to `raw.stream_events`, rejects to `raw.stream_rejects`, summary to `raw.stream_runs`, and
stream checks to `quality.check_results` (layer `stream`).
