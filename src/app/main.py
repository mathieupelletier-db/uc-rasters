"""UC Rasters - serve XYZ raster map tiles from a Unity Catalog volume.

Two tile endpoints are exposed:

* ``/tiles/{z}/{x}/{y}.png`` (v1) - the app streams the file out of the
  volume using ``WorkspaceClient.files.download`` and pipes it to the
  client. Bytes flow through the app process.
* ``/tiles/v2/{z}/{x}/{y}.png`` (v2) - the app calls the
  ``POST /api/2.0/fs/create-download-url`` endpoint (see
  ``_PresignedUrlBuilder.build_download_url`` in databricks-sdk-py) to
  mint a short-lived presigned URL pointing directly at cloud storage,
  then redirects the browser there. Bytes flow straight from the
  storage provider to the browser; the app never sees them.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterator
from urllib.parse import urlparse

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.mixins.files import _PresignedUrlRequestBuilder
from databricks.sdk.mixins.files_utils import CreateDownloadUrlResponse
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("uc-rasters")

VOLUME_PATH = os.environ.get(
    "UC_RASTER_VOLUME_PATH", "/Volumes/main/default/raster_tiles"
).rstrip("/")

CHUNK_SIZE = 64 * 1024
PRESIGN_TTL = timedelta(minutes=10)

# Hosts where the presigned URL is reachable directly from a browser
# without Databricks credentials. Anything else (e.g. the Databricks
# storage proxy) has to be fetched server-side and re-streamed.
_DIRECT_CLOUD_HOST_SUFFIXES: tuple[str, ...] = (
    ".blob.core.windows.net",
    ".dfs.core.windows.net",
    ".s3.amazonaws.com",
    ".amazonaws.com",
    "storage.googleapis.com",
)


def _is_direct_cloud_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == s.lstrip(".") or host.endswith(s) for s in _DIRECT_CLOUD_HOST_SUFFIXES)

app = FastAPI(title="UC Rasters", docs_url="/docs", redoc_url=None)

_workspace: WorkspaceClient | None = None


def workspace() -> WorkspaceClient:
    """Return a process-wide WorkspaceClient.

    Inside Databricks Apps the SDK picks up the app's service principal
    automatically via the standard env vars.
    """
    global _workspace
    if _workspace is None:
        _workspace = WorkspaceClient()
    return _workspace


_presigned_builder: _PresignedUrlRequestBuilder | None = None


def presigned_builder() -> _PresignedUrlRequestBuilder:
    """Return a process-wide ``_PresignedUrlRequestBuilder``.

    We deliberately bypass ``FilesExt._create_request_builder`` because
    that helper may pick the storage-proxy builder when running inside
    the data plane, which produces URLs a browser cannot authenticate
    against. ``_PresignedUrlRequestBuilder.build_download_url`` always
    hits ``POST /api/2.0/fs/create-download-url`` and yields a real
    cloud-storage SAS URL, which is exactly what we want to redirect a
    browser to.
    """
    global _presigned_builder
    if _presigned_builder is None:
        w = workspace()
        _presigned_builder = _PresignedUrlRequestBuilder(w.files._api, w.config.host)
    return _presigned_builder


def _stream_volume_file(remote_path: str) -> Iterator[bytes]:
    """Yield bytes from a UC volume file using the SDK's streaming download."""
    response = workspace().files.download(remote_path)
    body = response.contents
    try:
        while True:
            chunk = body.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk
    finally:
        try:
            body.close()
        except Exception:
            pass


def _create_download_url(remote_path: str) -> CreateDownloadUrlResponse:
    """Mint a short-lived presigned download URL for a UC volume path.

    Delegates to ``_PresignedUrlRequestBuilder.build_download_url`` from
    databricks-sdk-py, which posts to ``/api/2.0/fs/create-download-url``
    and returns a ``CreateDownloadUrlResponse(url, headers)``.
    """
    expire_time = (datetime.now(timezone.utc) + PRESIGN_TTL).strftime("%Y-%m-%dT%H:%M:%SZ")
    return presigned_builder().build_download_url(remote_path, expire_time)


def _validate_zxy(z: int, x: int, y: int) -> None:
    if not (0 <= z <= 22):
        raise HTTPException(status_code=400, detail="invalid zoom")
    n = 1 << z
    if not (0 <= x < n and 0 <= y < n):
        raise HTTPException(status_code=400, detail="tile out of range")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "volume": VOLUME_PATH}


@app.get("/tiles/{z}/{x}/{y}.png")
def get_tile(z: int, x: int, y: int) -> Response:
    """v1: stream the tile bytes through the app process."""
    _validate_zxy(z, x, y)
    remote_path = f"{VOLUME_PATH}/{z}/{x}/{y}.png"
    log.info("v1 streaming tile %s", remote_path)
    try:
        stream = _stream_volume_file(remote_path)
    except NotFound:
        raise HTTPException(status_code=404, detail="tile not found")
    except PermissionDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    headers = {"Cache-Control": "public, max-age=86400"}
    return StreamingResponse(stream, media_type="image/png", headers=headers)


@app.get("/tiles/v2/{z}/{x}/{y}.png")
def get_tile_v2(z: int, x: int, y: int) -> Response:
    """v2: hand the browser a presigned URL straight to cloud storage.

    Always uses ``POST /api/2.0/fs/create-download-url`` to mint a
    short-lived URL pointing at the file in the volume. When that URL is
    a direct cloud-storage SAS (Azure Blob, S3, GCS) we 307-redirect so
    the browser fetches the bytes directly from the storage account; the
    app never touches them. When the workspace returns a Databricks
    storage-proxy URL instead (which a browser can't authenticate
    against), we fall back to fetching it server-side using the headers
    the API gave us and stream the bytes through. Both branches go
    through the same ``create-download-url`` flow.
    """
    _validate_zxy(z, x, y)
    remote_path = f"{VOLUME_PATH}/{z}/{x}/{y}.png"
    try:
        presigned = _create_download_url(remote_path)
    except NotFound:
        raise HTTPException(status_code=404, detail="tile not found")
    except PermissionDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    if _is_direct_cloud_url(presigned.url):
        log.info("v2 redirecting browser to cloud presigned URL for %s", remote_path)
        # Cloud SAS URLs ignore the Authorization header from the
        # original request; they validate via the URL signature alone.
        # Any extra headers from the API (e.g. ``x-ms-version``) are
        # safe to omit for a simple GET; if the cloud rejects, the
        # browser will surface the error.
        return RedirectResponse(
            url=presigned.url,
            status_code=307,
            headers={"Cache-Control": "public, max-age=300"},
        )

    log.info(
        "v2 proxying presigned URL (host=%s) for %s",
        urlparse(presigned.url).hostname,
        remote_path,
    )
    try:
        upstream = requests.get(
            presigned.url,
            headers={**presigned.headers, "Accept": "image/png"},
            stream=True,
            timeout=30,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"upstream fetch failed: {exc}")

    if upstream.status_code == 404:
        raise HTTPException(status_code=404, detail="tile not found")
    if upstream.status_code >= 400:
        body_preview = upstream.text[:200]
        raise HTTPException(
            status_code=502,
            detail=f"upstream {upstream.status_code}: {body_preview}",
        )

    def _iter() -> Iterator[bytes]:
        try:
            for chunk in upstream.iter_content(CHUNK_SIZE):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    return StreamingResponse(
        _iter(),
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )


INDEX_HTML = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <title>UC Rasters</title>
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />
  <style>
    html, body, #map { height: 100%; margin: 0; }
    .banner {
      position: absolute; z-index: 1000; top: 12px; left: 50%;
      transform: translateX(-50%);
      background: rgba(17,24,39,0.9); color: #fff;
      padding: 10px 14px; border-radius: 8px;
      font-family: -apple-system, system-ui, sans-serif; font-size: 13px;
      display: flex; gap: 10px; align-items: center;
    }
    .banner code { background: rgba(255,255,255,0.1); padding: 1px 5px; border-radius: 3px; }
    .banner button {
      background: #2563eb; border: 0; color: white; padding: 4px 10px;
      border-radius: 4px; cursor: pointer; font-size: 12px;
    }
    .banner button.active { background: #16a34a; }
  </style>
</head>
<body>
  <div class=\"banner\">
    <span>UC Rasters &mdash; <code>__VOLUME__</code></span>
    <span>mode:</span>
    <button id=\"mode-v1\">v1 streamed</button>
    <button id=\"mode-v2\" class=\"active\">v2 presigned redirect</button>
  </div>
  <div id=\"map\"></div>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    const map = L.map('map', { worldCopyJump: true }).setView([20, 0], 2);
    const layers = {
      v1: L.tileLayer('tiles/{z}/{x}/{y}.png', {
        maxZoom: 3,
        attribution: 'v1 &middot; bytes proxied through the app',
      }),
      v2: L.tileLayer('tiles/v2/{z}/{x}/{y}.png', {
        maxZoom: 3,
        attribution: 'v2 &middot; presigned URL, browser fetches direct from cloud storage',
      }),
    };
    let current = 'v2';
    layers[current].addTo(map);

    function setMode(mode) {
      if (mode === current) return;
      map.removeLayer(layers[current]);
      layers[mode].addTo(map);
      current = mode;
      document.getElementById('mode-v1').classList.toggle('active', mode === 'v1');
      document.getElementById('mode-v2').classList.toggle('active', mode === 'v2');
    }
    document.getElementById('mode-v1').onclick = () => setMode('v1');
    document.getElementById('mode-v2').onclick = () => setMode('v2');
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML.replace("__VOLUME__", VOLUME_PATH))
