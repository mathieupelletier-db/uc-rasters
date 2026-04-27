"""Micro-benchmark v1 vs v2 tile endpoints.

Hits a configurable list of (z, x, y) coordinates against both
endpoints with a fixed concurrency and reports per-tile latency
distributions (p50/p90/p95/p99), throughput, and error rates.

Usage::

    python scripts/bench.py \
        --base https://uc-rasters-dev-7405612117836809.9.azure.databricksapps.com \
        --profile adb-7405612117836809 \
        --requests 200 --concurrency 8 --max-zoom 3 --warmup 20
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import random
import statistics
import sys
import time
from dataclasses import dataclass

import requests
from databricks.sdk import WorkspaceClient


@dataclass
class Sample:
    elapsed_ms: float
    status: int
    bytes: int
    redirected: bool


def make_coords(max_zoom: int, n: int, seed: int = 0) -> list[tuple[int, int, int]]:
    rng = random.Random(seed)
    coords: list[tuple[int, int, int]] = []
    for z in range(max_zoom + 1):
        side = 1 << z
        for x in range(side):
            for y in range(side):
                coords.append((z, x, y))
    rng.shuffle(coords)
    if n <= len(coords):
        return coords[:n]
    out = []
    while len(out) < n:
        out.extend(coords)
    return out[:n]


def fetch(session: requests.Session, url: str, token: str) -> Sample:
    t0 = time.perf_counter()
    r = session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
        allow_redirects=True,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    redirected = bool(r.history)
    return Sample(
        elapsed_ms=elapsed_ms,
        status=r.status_code,
        bytes=len(r.content),
        redirected=redirected,
    )


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def run(label: str, base: str, path_template: str, coords, token, concurrency: int, warmup: int) -> dict:
    session = requests.Session()
    urls = [f"{base}{path_template.format(z=z, x=x, y=y)}" for (z, x, y) in coords]

    if warmup > 0:
        with cf.ThreadPoolExecutor(max_workers=concurrency) as ex:
            list(ex.map(lambda u: fetch(session, u, token), urls[:warmup]))

    samples: list[Sample] = []
    t_start = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=concurrency) as ex:
        for s in ex.map(lambda u: fetch(session, u, token), urls):
            samples.append(s)
    wall_s = time.perf_counter() - t_start

    latencies = [s.elapsed_ms for s in samples if s.status == 200]
    errors = [s for s in samples if s.status != 200]
    bytes_total = sum(s.bytes for s in samples if s.status == 200)
    redirected = sum(1 for s in samples if s.redirected)

    return {
        "label": label,
        "n": len(samples),
        "ok": len(latencies),
        "errors": len(errors),
        "error_codes": sorted({s.status for s in errors}),
        "wall_s": wall_s,
        "rps": len(samples) / wall_s if wall_s > 0 else 0.0,
        "throughput_kbps": (bytes_total / 1024) / wall_s if wall_s > 0 else 0.0,
        "p50_ms": percentile(latencies, 50),
        "p90_ms": percentile(latencies, 90),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "mean_ms": statistics.fmean(latencies) if latencies else float("nan"),
        "min_ms": min(latencies) if latencies else float("nan"),
        "max_ms": max(latencies) if latencies else float("nan"),
        "redirected": redirected,
        "bytes_total": bytes_total,
    }


def fmt(stats: dict) -> str:
    return (
        f"{stats['label']:<10}  "
        f"n={stats['n']}  ok={stats['ok']}  err={stats['errors']}  "
        f"redir={stats['redirected']}  "
        f"wall={stats['wall_s']:.2f}s  "
        f"rps={stats['rps']:.1f}  "
        f"p50={stats['p50_ms']:.0f}ms  "
        f"p90={stats['p90_ms']:.0f}ms  "
        f"p95={stats['p95_ms']:.0f}ms  "
        f"p99={stats['p99_ms']:.0f}ms  "
        f"mean={stats['mean_ms']:.0f}ms  "
        f"min={stats['min_ms']:.0f}ms  "
        f"max={stats['max_ms']:.0f}ms  "
        f"bytes={stats['bytes_total']/1024:.1f}KiB"
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True, help="App base URL")
    p.add_argument("--profile", default="adb-7405612117836809")
    p.add_argument("--requests", type=int, default=200)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--max-zoom", type=int, default=3)
    p.add_argument("--warmup", type=int, default=20)
    p.add_argument("--repeats", type=int, default=1, help="Re-run the bench N times")
    args = p.parse_args()

    w = WorkspaceClient(profile=args.profile)
    token = w.config.oauth_token().access_token

    coords = make_coords(args.max_zoom, args.requests)

    print(f"target={args.base}  n={args.requests}  c={args.concurrency}  max_zoom={args.max_zoom}  warmup={args.warmup}")
    print()

    for run_idx in range(args.repeats):
        v1 = run("v1", args.base, "/tiles/{z}/{x}/{y}.png", coords, token, args.concurrency, args.warmup)
        v2 = run("v2", args.base, "/tiles/v2/{z}/{x}/{y}.png", coords, token, args.concurrency, args.warmup)
        if args.repeats > 1:
            print(f"--- run {run_idx + 1}/{args.repeats} ---")
        print(fmt(v1))
        print(fmt(v2))
        if v1["error_codes"] or v2["error_codes"]:
            print(f"  v1 errors: {v1['error_codes']}   v2 errors: {v2['error_codes']}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
