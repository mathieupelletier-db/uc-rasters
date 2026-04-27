# UC Rasters

A Databricks Asset Bundle that:

1. Provisions a Unity Catalog managed **volume** to hold raster (XYZ) map tiles.
2. Seeds the volume with a small sample of OpenStreetMap tiles (zooms 0–3).
3. Deploys a **Databricks App** (FastAPI) that streams those tiles back over
   HTTP at `/tiles/{z}/{x}/{y}.png`, using
   [`WorkspaceClient.files.download`](https://github.com/databricks/databricks-sdk-py/blob/815b17ec7ebac42b8f914a21ec1d23fc6d74a8c7/databricks/sdk/mixins/files.py#L821)
   for chunked, memory‑efficient streaming straight from the UC volume.
4. The app also serves a tiny Leaflet viewer at `/`.

Target workspace: `https://adb-7405612117836809.9.azuredatabricks.net`

## Layout

```
uc-rasters/
├── databricks.yml          # Bundle config + targets
├── resources/
│   ├── volume.yml          # UC volume definition
│   └── app.yml             # Databricks App definition
├── src/app/                # App source (deployed to the workspace)
│   ├── app.yaml            # App start command + env vars
│   ├── main.py             # FastAPI app
│   └── requirements.txt
└── scripts/
    └── download_tiles.py   # Seeds the volume with OSM tiles
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

Open the app URL printed by `bundle run` to see the Leaflet viewer.
