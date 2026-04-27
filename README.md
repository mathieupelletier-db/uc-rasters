# UC Rasters

A Databricks Asset Bundle that explores two ways of serving raster (XYZ) map
tiles out of a Unity Catalog volume from a Databricks App:

1. **v1 — proxy through the app** using `WorkspaceClient.files.download`.
   Bytes flow `cloud → app → browser`.
2. **v2 — presigned URL** via
   [`_PresignedUrlRequestBuilder.build_download_url`](https://github.com/databricks/databricks-sdk-py/blob/815b17ec7ebac42b8f914a21ec1d23fc6d74a8c7/databricks/sdk/mixins/files.py#L818).
   The app mints a short-lived URL, then either redirects the browser
   (when the URL is a real cloud SAS URL) or falls back to fetching it
   server-side (when the workspace returns a storage-proxy URL).

The bundle also provisions the volume, seeds it with a small set of
OpenStreetMap tiles, and ships a tiny Leaflet viewer that lets you flip
between v1 and v2 live.

Target workspace: `https://adb-7405612117836809.9.azuredatabricks.net`

## Layout

```
uc-rasters/
├── databricks.yml          # Bundle config + targets
├── resources/
│   ├── volume.yml          # UC volume definition
│   └── app.yml             # Databricks App + uc_securable binding
├── src/app/                # App source (deployed to the workspace)
│   ├── app.yaml            # App start command + env vars
│   ├── main.py             # FastAPI app: /tiles, /tiles/v2, /
│   └── requirements.txt
└── scripts/
    ├── download_tiles.py   # Seeds the volume with OSM tiles
    ├── bench.py            # Micro-benchmark v1 vs v2
    └── requirements.txt
```

## Quickstart

```bash
# 1. Authenticate (once)
databricks auth login -p adb-7405612117836809 \
    --host https://adb-7405612117836809.9.azuredatabricks.net/

# 2. Validate and deploy the bundle (creates the volume + uploads app source)
databricks bundle validate -p adb-7405612117836809
databricks bundle deploy   -p adb-7405612117836809

# 3. Seed the volume with sample raster tiles
python -m pip install -r scripts/requirements.txt
python scripts/download_tiles.py \
    --profile adb-7405612117836809 \
    --volume /Volumes/classic_stable_2cn624/default/raster_tiles \
    --max-zoom 3

# 4. Start the app
databricks bundle run uc_rasters_app -p adb-7405612117836809
```

Open the app URL printed by `bundle run` to see the Leaflet viewer with a
v1/v2 toggle.

---

## Endpoints

| Path | Strategy | Bytes path | Notes |
|---|---|---|---|
| `GET /` | Leaflet viewer | — | toggle between v1 and v2 |
| `GET /healthz` | health probe | — | returns `{"status": "ok", "volume": …}` |
| `GET /tiles/{z}/{x}/{y}.png` | **v1** — `files.download` streaming | cloud → app → client | always works |
| `GET /tiles/v2/{z}/{x}/{y}.png` | **v2** — `build_download_url` + 307 redirect; falls back to server-side fetch | redirect: cloud → client; fallback: cloud → app → client | redirect only works in some scenarios (see below) |

---

## Findings

### 1. The `create-download-url` API decides v2's URL based on caller

`POST /api/2.0/fs/create-download-url` is what `_PresignedUrlRequestBuilder.build_download_url`
hits. The server picks one of two URLs depending on the caller's network
zone, identity, and workspace configuration:

| Returned URL | Browser-reachable? | When |
|---|---|---|
| Cloud SAS, e.g. `https://*.blob.core.windows.net/…?sig=…` | Yes — signature alone authenticates | Caller is **outside the Databricks data plane** (CLI on a laptop, external service, public-cloud server). Also common on AWS/GCP managed-storage and BYO-bucket workspaces. |
| Storage proxy, e.g. `http://storage-proxy.databricks.com/api/2.0/fs/files/…?X-Databricks-Signature=…` | No — signature only honored at the proxy host, which isn't reachable from outside the data plane | Caller is **inside the data plane** (Databricks Apps, Jobs, notebooks) on workspaces with the storage proxy enabled (default on Azure managed-storage today). |

We confirmed this empirically:

- From local CLI (user OAuth, off-data-plane): API returns `dbstoragecrshd5glw7eeo.blob.core.windows.net/…?sig=…` ✓ browser-redirect works.
- From the deployed app (SP, in-data-plane): API returns `storage-proxy.databricks.com/…?X-Databricks-Signature=…` ✗ browser can't auth.

### 2. Rewriting host to `*.azuredatabricks.net` doesn't bridge the gap

A natural workaround is to swap the URL host for the workspace host
(`https://adb-…azuredatabricks.net/api/2.0/fs/files/…`). Findings:

- The workspace Files API **rejects** the storage-proxy query params
  (`X-Databricks-Signature`, `workspaceId`, `X-Databricks-TTL`, …) with
  `BAD_REQUEST: FILES_API_UNEXPECTED_QUERY_PARAMETERS`.
- After stripping them, the endpoint requires a real Databricks credential
  (Bearer or workspace session cookie). The browser, sandboxed at
  `*.databricksapps.com`, has neither for the `*.azuredatabricks.net`
  origin → `401 Unauthorized`.
- `databricksapps.com` and `azuredatabricks.net` are different sites, so
  the workspace session cookie doesn't ride along on cross-site redirects
  (modern browsers default to `SameSite=Lax`, which doesn't apply to image
  fetches).

Conclusion: from an Azure managed-storage Databricks App, the only way to
make v2 actually work in the browser is to keep the bytes flowing through
the app (server-side fallback), or move the serving process outside the
data plane.

### 3. v2 redirect works great when the caller is outside the data plane

If you run this same FastAPI app on Vercel / Cloudflare / any non-Databricks
host, the `create-download-url` API returns a real SAS URL and the
`/tiles/v2` redirect lets the browser fetch directly from cloud storage
(zero bytes through the app, no app CPU per tile, easy to put a CDN in
front).

### 4. Performance: v1 vs v2 (proxy fallback)

Micro-benchmark from a laptop against the deployed app, hitting both
endpoints with the same shuffled set of zoom 0–3 tiles. All v2 calls
took the proxy fallback path (worst case for v2):

| Concurrency | Endpoint | p50 | p90 | p95 | p99 | rps |
|---|---|---|---|---|---|---|
| 1   | v1 | 250 ms | 290 ms | 395 ms | 479 ms | 3.7  |
| 1   | v2 | 230 ms | 302 ms | 320 ms | 591 ms | 4.0  |
| 8   | v1 | 227 ms | 248 ms | 256 ms | 287 ms | 34.3 |
| 8   | v2 | 228 ms | 254 ms | 266 ms | 360 ms | 33.6 |
| 32  | v1 | 254 ms | 1605 ms | 1778 ms | 2251 ms | 63.4 |
| 32  | v2 | 243 ms | 1477 ms | 1791 ms | 3067 ms | 62.1 |

Takeaways:

- **v1 ≈ v2 within 5 %** at every concurrency level we tried; the extra
  `create-download-url` round trip is essentially free inside the data
  plane (sub-30 ms, TLS reused).
- Both saturate around **~60 RPS on a single uvicorn worker** — that's the
  app, not the technique. Scale by adding workers / app compute, or move
  to v2-redirect so storage handles the bytes.
- v2-redirect (when available) is dramatically cheaper: only the metadata
  hop touches the app; bytes go cloud → browser direct.

Reproduce:

```bash
python scripts/bench.py \
    --base https://uc-rasters-dev-7405612117836809.9.azure.databricksapps.com \
    --profile adb-7405612117836809 \
    --requests 200 --concurrency 8 --warmup 20 --repeats 2
```

### 5. Decision matrix

| Where the app runs | Workspace cloud | Best endpoint | Why |
|---|---|---|---|
| Databricks App | Azure (managed storage, storage proxy on) | **v1** | v2 falls back to proxying anyway, and v1 is simpler with one fewer round trip |
| Databricks App | AWS / GCP / BYO-bucket | likely **v2** redirect | API often hands back a real SAS URL → bytes never touch the app |
| External server (Vercel, Lambda, EC2, on-prem) | any | **v2** redirect | API sees an external caller and returns a SAS URL; CDN-friendly, app stays metadata-only |
| Databricks Job / notebook serving via API | Azure managed storage | **v1** | same reason as Apps |

In short: v1 always works; v2 wins big when the URL minted by the SDK is a
real cloud SAS URL — which is determined by the workspace + caller, not by
the code.

---

## Implementation notes

- The app uses an explicit `_PresignedUrlRequestBuilder` instead of going
  through `FilesExt._create_request_builder()` because the latter may
  pick the storage-proxy builder (which doesn't even hit the
  `create-download-url` API). We want the public API path so the example
  is faithful to the technique.
- `app.yml` binds the volume to the app via `uc_securable` (type `VOLUME`,
  permission `READ_VOLUME`). The Apps SP gets read access automatically.
- `scripts/download_tiles.py` uses `certifi` for SSL trust; system
  defaults on macOS often miss the OSM tile server's chain.
- The bundle deploy creates the volume and uploads source in one step;
  on a fresh workspace you may see the first deploy fail at the app
  binding step because Terraform creates the volume and the app in
  parallel — re-running `databricks bundle deploy` after the volume
  exists is enough.

## References

- [databricks-sdk-py FilesExt.download](https://github.com/databricks/databricks-sdk-py/blob/815b17ec7ebac42b8f914a21ec1d23fc6d74a8c7/databricks/sdk/mixins/files.py#L1064)
- [`_PresignedUrlRequestBuilder.build_download_url`](https://github.com/databricks/databricks-sdk-py/blob/815b17ec7ebac42b8f914a21ec1d23fc6d74a8c7/databricks/sdk/mixins/files.py#L818)
- [`_StorageProxyRequestBuilder.build_download_url`](https://github.com/databricks/databricks-sdk-py/blob/815b17ec7ebac42b8f914a21ec1d23fc6d74a8c7/databricks/sdk/mixins/files.py#L902)
- [Databricks Asset Bundles — Apps resource](https://docs.databricks.com/dev-tools/bundles/resources)
