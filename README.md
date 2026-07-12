<div align="center">

# Local Data Studio

**GUI Application for Local Dataset Viewing and Analysis**

English | [日本語](docs/README_ja.md)
</div>

Local Data Studio is a Web Viewer for browsing and analyzing JSONL/JSON/CSV/TSV/Parquet files locally, inspired by Hugging Face Datasets' [Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio).  
It provides fast preview, DuckDB SQL execution (with optional LLM-assisted SQL generation), basic statistics, EDA report generation, and Embedding Atlas visualization.

<div align="center">
<img src="images/local_data_studio_01.png" alt="local data studio 01" width=90%>
</div>

## Key Features

- Fast bounded preview and cursor-style paging for large-scale datasets
- DuckDB SQL console (read-only) with timeout, memory, and large-scan guards
- EDA report generation for the whole dataset or SQL query results (cached under `./cache/eda`)
- Embedding Atlas visualization for selected text/image columns, including SQL query results
- Row Inspector (copy, delete, highlight)
- Image rendering from URLs, local paths, and `{bytes, path}` image dictionaries
- Image zoom and row-level multi-image navigation
- Drag & drop upload support
- Hide/delete within the current session

## Installation

### From PyPI

After the package is published, install it with pip:

Local Data Studio supports Python 3.11, 3.12, and 3.13.

```bash
python -m pip install local-data-studio
```

Run the app with either entrypoint:

```bash
local-data-studio --data-dir /local/data/path
python -m local_data_studio --data-dir /local/data/path
```

Use `--data-file` instead of `--data-dir` to open a single dataset file. By default, `.env`, `data`, `cache`, and `models/embedder` are resolved under the current working directory. For repeatable launches, use `--workspace-dir` or `--config`; individual paths can still be overridden with `--data-dir`, `--data-file`, `--cache-dir`, `--models-dir`, `--env-file`, and `--file-serve-roots`.

Example config file:

```toml
[paths]
workspace_dir = "/Users/me/local-data-studio"
env_file = ".env"
data_dir = "/Users/me/datasets"
cache_dir = "/Users/me/.cache/local-data-studio"
models_dir = "/Users/me/models/embedder"
file_serve_roots = ["/Users/me/datasets", "/Users/me/images"]
vis_exclude_files = ["/Users/me/datasets/archive.csv"]

[server]
host = "127.0.0.1"
port = 8000
reload = false
```

Run it with:

```bash
local-data-studio --config /path/to/local_data_studio.toml
```

Path precedence is: CLI option, OS environment variable, config file, `.env`, workspace default, then current-working-directory default.

### From source

1. **Clone or download the repository**

    ```bash
    git clone git@github.com:Onely7/local_data_studio.git
    cd local_data_studio
    ```

2. **Install dependencies**

    ```bash
    uv sync
    ```

3. **Configure environment variables**
   Create or edit `.env` and set the environment variables.

    ```bash
    cp .env.example .env
    ```

    ```bash
    # Data set specification (if both exist, DATA_FILE takes precedence)
    # DATA_FILE=
    DATA_DIR=/local/data/path  # FIXME: data directory path set here (required)
    FILE_SERVE_ROOTS=""
    VIS_EXCLUDE_DIRS=""
    VIS_EXCLUDE_FILES=""

    # LLM SQL Generation Settings
    OPENAI_API_KEY=""  # FIXME: OpenAI API Key set here
    OPENAI_BASE_URL=https://api.openai.com/v1
    OPENAI_MODEL=gpt-5.2

    # EDA Settings
    EDA_ROW_LIMIT=50000
    EDA_CELL_MAX_CHARS=5000
    EDA_NESTED_POLICY=stringify
    EDA_CACHE_MAX_BYTES=1073741824

    # Embedding Atlas Settings
    EMBEDDER_MODELS_DIR=models/embedder
    ATLAS_HOST=127.0.0.1
    ATLAS_PORT=5055
    # ATLAS_SAMPLE=5000
    # ATLAS_BATCH_SIZE=16
    ATLAS_CACHE_MAX_BYTES=10737418240
    ATLAS_TEXT_MAX_CHARS=4096
    ATLAS_EMBEDDING_DTYPE=float32
    ATLAS_UMAP_PROJECTION_MODE=full
    ATLAS_UMAP_ANCHOR_SAMPLE=10000
    ATLAS_TRUST_REMOTE_CODE=false

    # Delete Permission
    ALLOW_DELETE_DATA=false
    ```

   Environment variable details:

   - `DATA_FILE`: Directly specify a single file. If set, it takes precedence over `DATA_DIR`.
   - `DATA_DIR`: Directory to search for datasets (required if `DATA_FILE` is not used).
   - `FILE_SERVE_ROOTS`: Comma-separated directories from which local image previews may be served.
   - `VIS_EXCLUDE_DIRS`: Comma-separated directories to exclude from dataset discovery under `DATA_DIR`.
   - `VIS_EXCLUDE_FILES`: Comma-separated dataset files to exclude from discovery under `DATA_DIR`. Relative paths are resolved from `DATA_DIR`; absolute paths are also accepted.
   - `OPENAI_API_KEY`: API key to enable LLM-based SQL generation.
   - `OPENAI_BASE_URL`: Endpoint for an OpenAI-compatible API.
   - `OPENAI_MODEL`: OpenAI model name to use.
   - `EDA_ROW_LIMIT`: Server-side row limit for dataset and query-result EDA reports. Configure it through the environment or `.env`; the UI does not override it. Any integer greater than or equal to `1` is accepted. Set `-1` to load all rows without a row limit.
   - `EDA_CELL_MAX_CHARS`: Maximum number of characters to display for long strings in EDA. Excess text is truncated as `... (truncated)`.
   - `EDA_NESTED_POLICY`: How to handle nested types (list/struct/object/binary, etc.). `stringify` keeps them as strings, and `drop` removes the corresponding columns.
   - `EDA_CACHE_MAX_BYTES`: Maximum combined size of EDA reports stored under `./cache/eda`. The default is 1 GiB; when the limit is exceeded, the oldest reports are removed first.
   - `EMBEDDER_MODELS_DIR`: Directory containing local HuggingFace encoder model directories. Defaults to `models/embedder` under the workspace/current directory.
   - `ATLAS_HOST` / `ATLAS_PORT`: Host and starting port for local Embedding Atlas pages. `embedding-atlas` may choose another port if the port is already in use.
   - `ATLAS_SAMPLE`: Strict maximum number of rows embedded, projected, and written to the cached Atlas parquet. Sampling happens deterministically with seed 42 after applying a SQL query. Leave unset or `0` to use all selected rows; negative values are rejected at startup.
   - `ATLAS_BATCH_SIZE`: Optional embedding batch size. Leave unset or `0` to use Embedding Atlas defaults.
   - `ATLAS_CACHE_MAX_BYTES`: Maximum size for Local Data Studio's Embedding Atlas cache under `./cache/atlas`. Old cache files are pruned first.
   - `ATLAS_TEXT_MAX_CHARS`: Maximum characters kept per text cell for Atlas embedding inputs and cached Atlas parquet output. Set `0` to disable truncation.
   - `ATLAS_EMBEDDING_DTYPE`: Embedding array precision before projection: `float32` or `float16`.
   - `ATLAS_UMAP_PROJECTION_MODE`: UMAP-only strategy. `full` computes UMAP on all sampled embeddings, while `anchor_transform` fits UMAP on representative anchors and transforms the remaining sampled rows into the same space. t-SNE and PCA always use full projection.
   - `ATLAS_UMAP_ANCHOR_SAMPLE`: Number of rows used to fit UMAP when `ATLAS_UMAP_PROJECTION_MODE=anchor_transform`.
   - `ATLAS_TRUST_REMOTE_CODE`: Passes `--trust-remote-code` to Embedding Atlas when `true`.
   - `ALLOW_DELETE_DATA`: If `false`, physical file deletion is disabled (session-level hiding is still allowed).

## Run

```bash
uv run local-data-studio --reload
```

For direct ASGI development, use:

```bash
uv run uvicorn local_data_studio.app:app --reload
```

After running, you will see messages like the following in your terminal:

```
INFO:     Will watch for changes in these directories: ['local/data_viewer']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [00000] using StatReload
INFO:     Started server process [00000]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

The Local Data Studio server is now running.  
Open [http://127.0.0.1:8000](http://127.0.0.1:8000) to view the Local Data Studio GUI.

## Usage

1. **Select a file from DATASETS**
   Choose a dataset from the DATASETS list on the left. You can also filter by using the search box. Long file names are ellipsized in the list, and file sizes are shown with up to three significant digits using `Bytes`, `kB`, `MB`, `GB`, or `TB`.  
   <img src="images/local_data_studio_02.png" alt="local data studio 02" width=45%>

2. **Preview / Search / Paging**
   Use Search at the top to search the data, Rows to change the number of displayed rows, and Prev/Next to move between pages.  
   <img src="images/local_data_studio_03.png" alt="local data studio 03" width=45%>

3. **SQL Console**
   Run DuckDB SQL queries against the `data` table.  
   It also supports converting natural-language instructions into SQL using an LLM. SQL execution is limited to a single `SELECT`/CTE statement and runs with timeout, memory, and large-dataset scan-risk guards.  
   <img src="images/local_data_studio_04.png" alt="local data studio 04" width=45%>

4. **EDA Report**
   Run EDA to generate and cache a report for the dataset sample. Use **Run EDA on Query Results** to profile the current SQL Console query results instead.  
   Dataset reports are cached under `./cache/eda` based on {file fingerprint, row limit, profile mode}. Query-result reports are cached separately based on {file fingerprint, SQL, row limit, profile mode}. The combined EDA cache defaults to 1 GiB and removes the oldest reports first when it exceeds `EDA_CACHE_MAX_BYTES`.
   Configure the row limit with `EDA_ROW_LIMIT` in the environment or `.env`. The UI does not override this server-side setting. Any positive value is accepted, and `-1` removes the row limit. Select **Profile mode** in the EDA panel for each run; the default is `minimal`.
   <img src="images/local_data_studio_05.png" alt="local data studio 05" width=45%> <img src="images/local_data_studio_06.png" alt="local data studio 06" width=45%>

5. **Visualize Embedding**
   Put local HuggingFace encoder model directories under `models/embedder` or the directory selected with `--models-dir` / `EMBEDDER_MODELS_DIR` (for example, `models/embedder/google/siglip2-base-patch16-224`, `models/embedder/Qwen/Qwen3-Embedding-0.6B`, or `models/embedder/Qwen/Qwen3-VL-Embedding-2B`). Directories containing model marker files such as `config.json`, `modules.json`, `tokenizer_config.json`, or `preprocessor_config.json` appear in the Model dropdown.
   Select a text or image column, model, available backend, and projection method in **Visualize Embedding**, then run **Run Atlas** to launch a local Embedding Atlas page. Projection choices are **UMAP** (default), **t-SNE**, and **PCA**. Use **Run Atlas on Query Results** to visualize the current SQL Console query results instead. Model configuration is inspected without loading weights; unavailable backends remain visible but disabled. When both backends are available, Sentence Transformers is selected by default.
   With Sentence Transformers selected, an optional Prompt field is shown. Leave it blank to use the model's saved default prompt. Text without a placeholder is prepended to every selected value. Exact placeholders such as `{title}` or `{body}` insert values from the same dataset row or SQL result row; `{{` and `}}` produce literal braces. Unknown columns, malformed braces, conversions, and format specifiers are rejected before the model is loaded.
   The job runs in the background with progress updates, and an **Open Atlas** link appears when the local Atlas page is ready.  
   <img src="images/local_data_studio_07.png" alt="local data studio 07" width=45%> <img src="images/local_data_studio_08.png" alt="local data studio 08" width=45%>

6. **Row Inspector / Image Zoom**
   Click a row to expand it in the details panel. Long values are compacted by default and can be toggled with Raw. For image columns, click to open a zoomed view. Image candidates are detected from image URLs, relative/absolute image paths, and dictionaries such as `{ "bytes": ..., "path": ... }`; bytes are tried first and path is used as a fallback.  
   <img src="images/local_data_studio_09.png" alt="local data studio 09" width=45%> <img src="images/local_data_studio_10.png" alt="local data studio 10" width=45%>

## Notes

- Supported dataset formats: `.jsonl`, `.json`, `.csv`, `.tsv`, `.parquet`.
- On large datasets, searching and running EDA may take time.
- For very large datasets, preview uses cursor-style page tokens instead of large `OFFSET` scans where supported. Row counts, global search, sampled statistics, and EDA run through background jobs with progress and cancellation APIs.
- Embedding Atlas jobs use the selected local encoder model and compute embeddings/projections locally. Projected parquet inputs are cached under `./cache/atlas/datasets`; repeated runs reuse the projected parquet only when the dataset fingerprint, SQL, column, model, backend, prompt template, capability fingerprint, projection method, and projection settings match. Image display columns are kept in their original URL/path/`{bytes, path}` shape while a hidden embedding input column is used only for encoder input conversion. `ATLAS_SAMPLE=N` deterministically limits embedding, projection, and cached parquet output to at most `N` rows after SQL filtering; it does not avoid the current initial DataFrame load. UMAP supports `full` and `ATLAS_UMAP_PROJECTION_MODE=anchor_transform`, while t-SNE and PCA always project all sampled embeddings. t-SNE can become expensive quickly and has no separate fixed row cap, so set a practical `ATLAS_SAMPLE`. Use `ATLAS_TEXT_MAX_CHARS` to bound long text and expanded prompts, `ATLAS_EMBEDDING_DTYPE=float16` to reduce embedding memory, and `ATLAS_CACHE_MAX_BYTES` to cap the combined Atlas cache size.
- Backend support is derived from bounded parsing of local `modules.json`, `config.json`, tokenizer/processor, pooling, and normalization metadata rather than model-name rules. Sentence Transformers reports `native`, `generic_fallback`, `metadata_only`, `unsupported`, or `unknown`; Transformers reports `direct`, `remote_code`, `backbone_only`, `unsupported`, or `unknown`. Sentence Transformers generic fallback is selectable only for tokenizer-backed, text-only Transformers models; image or multimodal models require a native `modules.json`. For example, the image-only DINOv3 checkpoints expose only Transformers and use their declared `pooler_output`, while Qwen3-VL-Embedding exposes both backends through its native Sentence Transformers pipeline. Only capabilities with a verified runnable adapter are selectable, and `remote_code` remains disabled unless `ATLAS_TRUST_REMOTE_CODE=true` explicitly permits repository code at execution time. Built-in Transformer/Pooling/Normalize pipelines can be reproduced by the Transformers adapter without importing Python files from the model repository.
- Local encoder model files under `models/embedder` or a configured models directory are intentionally not bundled. Only the directory placeholder files are tracked; download or place model files locally on each machine.
- Cache files are separated under `./cache/metadata`, `./cache/index`, `./cache/stats`, `./cache/count`, `./cache/search`, and `./cache/eda`. EDA reports have a configurable combined capacity (`EDA_CACHE_MAX_BYTES`) and are pruned oldest-first; fingerprint-based caches are invalidated by file path, size, and modification time where applicable.
- `Run EDA on Query Results` excludes helper columns such as `rn` and `__rowid` from the generated report.
- Feedback shown after Count Rows, EDA, and Atlas actions uses one compact status style so results remain distinguishable without overpowering the surrounding controls.
- `EDA_ROW_LIMIT=-1` materializes all selected rows in memory for profiling. Use it only when the complete dataset or query result comfortably fits in memory; large datasets can exhaust system memory.
- TB-scale `.json` arrays are not recommended. Prefer JSONL or Parquet for responsive preview.
- `Delete from file` modifies the actual file, so make backups as needed.
- If `ALLOW_DELETE_DATA=false`, only session-level hiding is allowed (the actual file will not be modified).

## Implementation Notes

- The application package lives under `src/local_data_studio`. Static UI assets are packaged in `src/local_data_studio/static`, while runtime `.env`, `data`, `cache`, and `models/embedder` directories default to the selected workspace or current working directory. CLI options override OS environment variables, config-file values, `.env`, and workspace defaults in that order.
- `src/local_data_studio/app.py` is a small application assembly entrypoint. Request models and API routes are grouped under `src/local_data_studio/server/api` by dataset access, analysis, background jobs, mutations, shared services, and static mounts. Blocking filesystem, DuckDB, EDA, and job routes run in FastAPI's threadpool; only streaming upload handling remains asynchronous.
- `src/local_data_studio/server/readers.py` remains the compatibility facade for implementations under `src/local_data_studio/server/dataset_readers`. JSONL metadata inference stops at fixed row and byte budgets, and JSONL/CSV/TSV preview uses byte/page tokens with a fingerprinted sparse line index. Completed indexes are reused and checkpoints are saved in batch transactions. CSV/TSV use the same large-field parser for schema, preview, search, and raw rows. Parquet schema reads footer metadata, previews and raw rows use bounded record batches, and compatible offsets are resolved from row-group metadata without row-by-row scans.
- `src/local_data_studio/server/stats.py` remains the compatibility facade for `src/local_data_studio/server/column_stats`, which separates value inference, per-column accumulation, and DuckDB orchestration. Sample rows are fetched in fixed-size batches and transferred directly into column accumulators instead of retaining both a complete row matrix and column copies.
- SQL execution is centralized in `src/local_data_studio/server/sql.py`, which validates read-only SQL, applies DuckDB resource limits, and supports cooperative cancellation for background jobs.
- EDA report orchestration lives in `src/local_data_studio/server/eda_reports.py`; low-level profiling setup and DataFrame sanitization live in `src/local_data_studio/server/eda.py`. Reports are isolated under `./cache/eda` and use the shared oldest-first capacity pruner.
- `src/local_data_studio/server/atlas.py` remains the compatibility facade for `src/local_data_studio/server/atlas_components`, which separates contracts, capability-driven embedding adapters, safe prompt templates, image conversion, projection, dataset caching, subprocess control, and orchestration. `server/embedder_capabilities.py` performs bounded metadata-only model inspection and fingerprints the relevant configuration. An encoder is created once per Atlas job and reused across anchor/transform batches. Anchor-transform reads only the anchor and current transform batch instead of converting the complete input column to a Python list. Final display sanitization and projection-column attachment share one owned DataFrame copy, while concurrent misses for the same fingerprint/query/column/model/backend/prompt/settings key share one cache generation.
- Atlas UMAP projection uses a fixed random seed for reproducible cache artifacts and explicitly sets `n_jobs=1`, matching UMAP's seeded execution mode without emitting thread override warnings.
- On macOS, Atlas subprocess launch is kept compatible with Python's `posix_spawn` path to avoid child-side fork `SIGSEGV (-11)` failures. Keep Atlas commands on absolute paths, do not pass `cwd` to `Popen`, and keep `close_fds=False`.
- Background jobs are managed by `src/local_data_studio/server/jobs.py` and expose progress, cancellation, result, and error state through `/api/jobs/*`.

## Contribution

- Please use Issues for bug reports and feature requests.
- This repository uses pre-commit for code quality. Running `uv run pre-commit run --all-files` (or `uvx pre-commit run --all-files`) is equivalent to executing the following:

  - `uv run ruff format` (or `uvx ruff format`)
  - `uv run ruff check` (or `uvx ruff check`)
  - `uv run ty check` (or `uvx ty check`)
- Ruff enforces PEP 257 docstrings with the Google convention across application and test code. Public APIs document constraints, exceptions, side effects, and ownership when types and names alone are insufficient; private implementation details should not receive redundant narration.
- Please make sure to fix all reported errors before committing.

## Acknowledgements

- [Dataset viewer (Huggingface)](https://github.com/huggingface/dataset-viewer): Used as a reference for UI/feature design.
- [YData Profiling](https://github.com/ydataai/ydata-profiling): Used for EDA report generation.
- [Embedding Atlas](https://github.com/apple/embedding-atlas): Used for interactive embedding visualization.

## License

This repository is released under the MIT License.
