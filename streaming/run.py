"""Entry point for the streaming piece: ``python -m streaming.run --events N``.

Pipeline for one recorded run:

1. load simulation parameters (warehouse-derived if available, else JSON);
2. recreate the two Kafka topics for a clean, unambiguous run;
3. produce N simulated messages (lifecycle + GPS pings + injected faults);
4. consume them, validating / deduping / watermarking, landing to Postgres and
   counting rejects;
5. write ``raw.stream_runs`` and the stream rows of ``quality.check_results``.

Runs on the contract env vars: ``KAFKA_BOOTSTRAP`` (default 127.0.0.1:9092) and
``PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE``.  For the isolated build-machine
run these are exported as 127.0.0.1:9094 and PGPORT=5436 (see docs/streaming.md).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime

from streaming import db
from streaming.consumer import consume
from streaming.params import StreamParams
from streaming.producer import DEFAULT_TOPICS, ensure_topics, produce
from streaming.simulator import EventStream
from streaming.validation import StreamProcessor


def _load_id() -> str:
    return os.environ.get("LOAD_ID") or datetime.utcnow().strftime("%Y%m%dT%H%M%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="streaming.run", description=__doc__)
    parser.add_argument(
        "--events", type=int, default=5000, help="messages to simulate (default 5000)"
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument("--load-id", default=None, help="lineage id (default: LOAD_ID or now)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducible simulation")
    parser.add_argument(
        "--mode", choices=("both", "produce", "consume"), default="both", help="run stage(s)"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="topic partitions; 1 keeps global event-time order so watermark "
        "lateness reflects only injected late events (default 1)",
    )
    parser.add_argument("--idle-timeout", type=float, default=20.0)
    parser.add_argument("--params", default=None, help="override params JSON path")
    parser.add_argument("--json-out", default=None, help="write the run summary JSON here")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_id = args.load_id or _load_id()

    params = StreamParams.load(pg_dsn=db.dsn_from_env(), path=args.params)
    print(f"[stream] run={load_id} events={args.events} bootstrap={args.bootstrap}")
    print(
        f"[stream] params: demand={params.sources.get('demand')} "
        f"delivery={params.sources.get('delivery_time')}"
    )

    produce_stats = {"produced": 0, "duration_s": 0.0, "events_per_s": 0.0, "injected": {}}
    if args.mode in ("both", "produce"):
        ensure_topics(args.bootstrap, DEFAULT_TOPICS, partitions=args.partitions, recreate=True)
        stream = EventStream(params=params, seed=args.seed)
        produce_stats = produce(stream, args.events, args.bootstrap)
        print(
            f"[stream] produced {produce_stats['produced']} in "
            f"{produce_stats['duration_s']:.1f}s "
            f"({produce_stats['events_per_s']:.0f}/s) injected={produce_stats['injected']}"
        )
        if produce_stats["errors"]:
            print(f"[stream] WARNING producer errors: {produce_stats['errors'][:3]}")
        if args.mode == "produce":
            _emit_summary(args, load_id, params, produce_stats, None)
            return 0

    conn = db.connect()
    db.ensure_schema(conn)
    sink = db.StreamSink(conn, load_id)
    if args.mode == "consume":
        ensure_topics(args.bootstrap, DEFAULT_TOPICS, partitions=args.partitions, recreate=False)

    processor = StreamProcessor(allowed_lateness_s=params.allowed_lateness_s)
    expected = produce_stats["produced"] if args.mode == "both" else None
    started = time.time()
    consume_stats = consume(
        args.bootstrap,
        processor,
        sink,
        load_id,
        expected=expected,
        idle_timeout_s=args.idle_timeout,
    )
    end_to_end_s = time.time() - started

    stats = {
        **consume_stats,
        "produced": produce_stats["produced"],
        "duration_s": end_to_end_s,
        "events_per_s": consume_stats["consumed"] / end_to_end_s if end_to_end_s else 0.0,
    }
    sink.write_run(stats)
    sink.write_quality(stats)

    _log_stats(load_id, stats)
    _emit_summary(args, load_id, params, produce_stats, stats)
    return 0


def _log_stats(load_id: str, stats: dict) -> None:
    print(
        f"[stream] consumed={stats['consumed']} landed={stats['landed']} "
        f"dups={stats['duplicates']} late={stats['late']} oob={stats['out_of_bounds']} "
        f"malformed={stats['malformed']}"
    )
    print(
        f"[stream] {stats['events_per_s']:.0f} events/s "
        f"({stats['duration_s']:.1f}s end-to-end) run={load_id}"
    )


def _emit_summary(args, load_id, params, produce_stats, stats) -> None:
    summary = {
        "load_id": load_id,
        "events_requested": args.events,
        "seed": args.seed,
        "bootstrap": args.bootstrap,
        "bootstrap_port": args.bootstrap.rsplit(":", 1)[-1],
        "params_demand_source": params.sources.get("demand"),
        "params_delivery_source": params.sources.get("delivery_time"),
        "produce": produce_stats,
        "consume": stats,
    }
    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(summary, fh, indent=2)
        print(f"[stream] summary -> {args.json_out}")


if __name__ == "__main__":
    raise SystemExit(main())
