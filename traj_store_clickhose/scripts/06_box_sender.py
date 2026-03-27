#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
import socket
import time
from pathlib import Path

from ingest_common import BoxStreamConfig, encode_batch_message, generate_box_batch, sender_summary_dict, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously send box batches to the ClickHouse ingest receiver.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--max-batches", type=int, default=0)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--vehicles-per-frame", type=int, default=200)
    parser.add_argument("--batch-frames", type=int, default=5)
    parser.add_argument("--start-ts-ms", type=int, default=1741910400000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--pace", choices=("realtime", "max"), default="realtime")
    parser.add_argument("--run-dir", default="", help="Optional benchmark run directory for sender_summary.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = BoxStreamConfig(
        fps=args.fps,
        vehicles_per_frame=args.vehicles_per_frame,
        batch_frames=args.batch_frames,
        start_ts_ms=args.start_ts_ms,
        seed=args.seed,
        pace=args.pace,
    )

    rng = random.Random(config.seed)
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    summary_path = run_dir / "sender_summary.json" if run_dir else None

    total_rows = 0
    total_bytes = 0
    batches_sent = 0
    frame_id = 0
    start_monotonic = time.monotonic()
    deadline = start_monotonic + args.duration_seconds if args.duration_seconds > 0 else None

    with socket.create_connection((args.host, args.port)) as sock:
        if hasattr(socket, "TCP_NODELAY"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        while True:
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            if args.max_batches > 0 and batches_sent >= args.max_batches:
                break

            rows, frame_id = generate_box_batch(frame_id, config, rng)
            payload = encode_batch_message(batches_sent, rows)
            sock.sendall(payload)

            batches_sent += 1
            total_rows += len(rows)
            total_bytes += len(payload)

            if config.pace == "realtime":
                target_elapsed = frame_id / config.fps
                sleep_seconds = target_elapsed - (time.monotonic() - start_monotonic)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

    elapsed_seconds = max(time.monotonic() - start_monotonic, 1e-9)
    summary = sender_summary_dict(
        batches_sent=batches_sent,
        rows_sent=total_rows,
        bytes_sent=total_bytes,
        elapsed_seconds=elapsed_seconds,
    )

    if summary_path is not None:
        write_json(summary_path, summary)

    print("\n".join(f"{key}={value}" for key, value in summary.items()))


if __name__ == "__main__":
    main()
