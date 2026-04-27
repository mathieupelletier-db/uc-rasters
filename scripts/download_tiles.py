"""Download a small set of OpenStreetMap raster tiles and upload them
into the UC volume backing the uc-rasters Databricks App.

Usage:
    python scripts/download_tiles.py \
        --profile adb-7405612117836809 \
        --volume /Volumes/classic_stable_2cn624/default/raster_tiles \
        --max-zoom 3
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import ssl
from urllib.request import Request, urlopen

import certifi
from databricks.sdk import WorkspaceClient

USER_AGENT = "uc-rasters-demo/0.1 (https://github.com/databricks)"
TILE_HOST = "https://tile.openstreetmap.org"
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("seed-tiles")


def fetch_tile(z: int, x: int, y: int) -> bytes:
    url = f"{TILE_HOST}/{z}/{x}/{y}.png"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30, context=SSL_CTX) as resp:
        return resp.read()


def upload_tile(w: WorkspaceClient, volume: str, z: int, x: int, y: int, data: bytes) -> str:
    remote_path = f"{volume}/{z}/{x}/{y}.png"
    w.files.upload(remote_path, io.BytesIO(data), overwrite=True)
    return remote_path


def seed_one(w: WorkspaceClient, volume: str, z: int, x: int, y: int) -> tuple[int, int, int, str]:
    data = fetch_tile(z, x, y)
    remote = upload_tile(w, volume, z, x, y, data)
    return z, x, y, remote


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="adb-7405612117836809")
    p.add_argument("--volume", required=True, help="UC volume path, e.g. /Volumes/cat/sch/raster_tiles")
    p.add_argument("--max-zoom", type=int, default=3)
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args()

    w = WorkspaceClient(profile=args.profile)

    coords: list[tuple[int, int, int]] = []
    for z in range(args.max_zoom + 1):
        n = 1 << z
        for x in range(n):
            for y in range(n):
                coords.append((z, x, y))

    log.info("Seeding %d tiles into %s (zooms 0..%d)", len(coords), args.volume, args.max_zoom)

    successes = 0
    failures: list[tuple[int, int, int, str]] = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(seed_one, w, args.volume, z, x, y): (z, x, y) for z, x, y in coords}
        for fut in as_completed(futures):
            zxy = futures[fut]
            try:
                z, x, y, remote = fut.result()
                successes += 1
                if successes % 10 == 0 or successes == len(coords):
                    log.info("uploaded %d/%d (latest: %s)", successes, len(coords), remote)
            except Exception as exc:
                failures.append((*zxy, str(exc)))
                log.warning("failed %s: %s", zxy, exc)

    elapsed = time.time() - start
    log.info("Done in %.1fs. ok=%d, failed=%d", elapsed, successes, len(failures))
    if failures:
        for f in failures[:5]:
            log.error("  %s", f)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
