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
- EDA report generation for the whole dataset or SQL query results (cached under `./cache`)
- Embedding Atlas visualization for selected text/image columns, including SQL query results
- Row Inspector (copy, delete, highlight)
- Image rendering from URLs, local paths, and `{bytes, path}` image dictionaries
- Image zoom and row-level multi-image navigation
- Drag & drop upload support
- Hide/delete within the current session

## Setup

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

    # LLM SQL Generation Settings
    OPENAI_API_KEY=""  # FIXME: OpenAI API Key set here
    OPENAI_BASE_URL=https://api.openai.com/v1
    OPENAI_MODEL=gpt-5.2

    # EDA Settings
    EDA_ROW_LIMIT=10000000
    # EDA_FONT_FAMILY=IPAexGothic
    # EDA_FONT_PATH=fonts/ipaexg.ttf
    EDA_PROFILE_MODE=minimal
    EDA_CELL_MAX_CHARS=5000
    EDA_NESTED_POLICY=stringify

    # Embedding Atlas Settings
    ATLAS_HOST=127.0.0.1
    ATLAS_PORT=5055
    # ATLAS_SAMPLE=5000
    # ATLAS_BATCH_SIZE=16
    ATLAS_CACHE_MAX_BYTES=10737418240
    ATLAS_TEXT_MAX_CHARS=4096
    ATLAS_EMBEDDING_DTYPE=float32
    ATLAS_PROJECTION_MODE=full
    ATLAS_ANCHOR_SAMPLE=10000
    # ATLAS_TEXT_EMBEDDER=sentence-transformers
    # ATLAS_IMAGE_EMBEDDER=transformers
    ATLAS_TRUST_REMOTE_CODE=false

    # Delete Permission
    ALLOW_DELETE_DATA=false
    ```

   Environment variable details:

   - `DATA_FILE`: Directly specify a single file. If set, it takes precedence over `DATA_DIR`.
   - `DATA_DIR`: Directory to search for datasets (required if `DATA_FILE` is not used).
   - `FILE_SERVE_ROOTS`: Comma-separated directories from which local image previews may be served.
   - `VIS_EXCLUDE_DIRS`: Comma-separated directories to exclude from dataset discovery under `DATA_DIR`.
   - `OPENAI_API_KEY`: API key to enable LLM-based SQL generation.
   - `OPENAI_BASE_URL`: Endpoint for an OpenAI-compatible API.
   - `OPENAI_MODEL`: OpenAI model name to use.
   - `EDA_ROW_LIMIT`: Maximum number of rows to load when generating an EDA report.
   - `EDA_FONT_FAMILY`: Font family name used in EDA reports. (Optional)
   - `EDA_FONT_PATH`: Path to a font file (takes precedence if set). (Optional)
   - `EDA_PROFILE_MODE`: Either `minimal` or `maximal`. `minimal` generates a lightweight report, while `maximal` includes more detailed statistics but takes longer.
   - `EDA_CELL_MAX_CHARS`: Maximum number of characters to display for long strings in EDA. Excess text is truncated as `... (truncated)`.
   - `EDA_NESTED_POLICY`: How to handle nested types (list/struct/object/binary, etc.). `stringify` keeps them as strings, and `drop` removes the corresponding columns.
   - `ATLAS_HOST` / `ATLAS_PORT`: Host and starting port for local Embedding Atlas pages. `embedding-atlas` may choose another port if the port is already in use.
   - `ATLAS_SAMPLE`: Optional random sample size passed to Embedding Atlas. Leave unset or `0` to use all rows.
   - `ATLAS_BATCH_SIZE`: Optional embedding batch size. Leave unset or `0` to use Embedding Atlas defaults.
   - `ATLAS_CACHE_MAX_BYTES`: Maximum size for Local Data Studio's Embedding Atlas cache under `./cache/atlas`. Old cache files are pruned first.
   - `ATLAS_TEXT_MAX_CHARS`: Maximum characters kept per text cell for Atlas embedding inputs and cached Atlas parquet output. Set `0` to disable truncation.
   - `ATLAS_EMBEDDING_DTYPE`: Embedding array precision before projection: `float32` or `float16`.
   - `ATLAS_PROJECTION_MODE`: Projection strategy: `full` computes UMAP on all embeddings, while `anchor_transform` fits UMAP on a representative sample and transforms the remaining rows into the same space.
   - `ATLAS_ANCHOR_SAMPLE`: Number of rows used to fit UMAP when `ATLAS_PROJECTION_MODE=anchor_transform`.
   - `ATLAS_TEXT_EMBEDDER` / `ATLAS_IMAGE_EMBEDDER`: Optional Embedding Atlas embedder backend names.
   - `ATLAS_TRUST_REMOTE_CODE`: Passes `--trust-remote-code` to Embedding Atlas when `true`.
   - `ALLOW_DELETE_DATA`: If `false`, physical file deletion is disabled (session-level hiding is still allowed).

## Run

```bash
uv run uvicorn app:app --reload
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

2. **Preview / Search / Paging**
   Use Search at the top to search the data, Rows to change the number of displayed rows, and Prev/Next to move between pages.

3. **SQL Console**
   Run DuckDB SQL queries against the `data` table.  
   It also supports converting natural-language instructions into SQL using an LLM. SQL execution is limited to a single `SELECT`/CTE statement and runs with timeout, memory, and large-dataset scan-risk guards.

4. **EDA Report**
   Run EDA to generate and cache a report for the dataset sample. Use **Run EDA on Query Results** to profile the current SQL Console query results instead.  
   Dataset reports are cached under `./cache` based on {file fingerprint, number of samples, `EDA_PROFILE_MODE`}. Query-result reports are cached separately based on {file fingerprint, SQL, number of samples, `EDA_PROFILE_MODE`}.  
   You can adjust the sample count with `EDA_ROW_LIMIT` and UI-side settings.

5. **Visualize Embedding**
   Put local HuggingFace encoder model directories under `models/embedder` (for example, `models/embedder/google/siglip2-base-patch16-224` or `models/embedder/Qwen/Qwen3-Embedding-0.6B`). Directories containing model marker files such as `config.json`, `modules.json`, `tokenizer_config.json`, or `preprocessor_config.json` appear in the Model dropdown.
   Select a text or image column and a model in **Visualize Embedding**, then run **Run Atlas** to launch a local Embedding Atlas page. Use **Run Atlas on Query Results** to visualize the current SQL Console query results instead.  
   The job runs in the background with progress updates, and an **Open Atlas** link appears when the local Atlas page is ready.

6. **Row Inspector / Image Zoom**
   Click a row to expand it in the details panel. Long values are compacted by default and can be toggled with Raw. For image columns, click to open a zoomed view. Image candidates are detected from image URLs, relative/absolute image paths, and dictionaries such as `{ "bytes": ..., "path": ... }`; bytes are tried first and path is used as a fallback.  
   <img src="images/local_data_studio_02.png" alt="local data studio 02" width=45%> <img src="images/local_data_studio_03.png" alt="local data studio 03" width=45%>

## Notes

- Supported dataset formats: `.jsonl`, `.json`, `.csv`, `.tsv`, `.parquet`.
- On large datasets, searching and running EDA may take time.
- For very large datasets, preview uses cursor-style page tokens instead of large `OFFSET` scans where supported. Row counts, global search, sampled statistics, and EDA run through background jobs with progress and cancellation APIs.
- Embedding Atlas jobs use the selected local encoder model and compute embeddings/projections locally. Projected parquet inputs are cached under `./cache/atlas/datasets`; repeated runs with the same selected data/query/model/settings reuse the projected parquet and skip embedding/UMAP recomputation. Use `ATLAS_SAMPLE` for faster exploratory runs on large datasets, `ATLAS_TEXT_MAX_CHARS` to bound very long text cells, `ATLAS_EMBEDDING_DTYPE=float16` to reduce embedding memory, `ATLAS_PROJECTION_MODE=anchor_transform` to fit UMAP on anchors and transform the remainder, and `ATLAS_CACHE_MAX_BYTES` to cap the combined Atlas cache size.
- Local encoder model files under `models/embedder` are intentionally ignored by Git. Only the directory placeholder files are tracked; download or place model files locally on each machine.
- Cache files are separated under `./cache/metadata`, `./cache/index`, `./cache/stats`, `./cache/count`, `./cache/search`, and EDA report files, and are invalidated by file path, size, and modification time where applicable.
- `Run EDA on Query Results` excludes helper columns such as `rn` and `__rowid` from the generated report.
- TB-scale `.json` arrays are not recommended. Prefer JSONL or Parquet for responsive preview.
- `Delete from file` modifies the actual file, so make backups as needed.
- If `ALLOW_DELETE_DATA=false`, only session-level hiding is allowed (the actual file will not be modified).

## Implementation Notes

- Format-specific readers live in `server/readers.py`. JSONL/CSV/TSV previews use byte/page-token based reads with sparse line indexing, and Parquet previews avoid loading whole row groups.
- SQL execution is centralized in `server/sql.py`, which validates read-only SQL, applies DuckDB resource limits, and supports cooperative cancellation for background jobs.
- EDA report orchestration lives in `server/eda_reports.py`; low-level profiling setup and DataFrame sanitization live in `server/eda.py`.
- Embedding Atlas launch orchestration lives in `server/atlas.py`; it discovers local models under `models/embedder`, infers text/image modality from selected column samples, materializes/reuses projected parquet cache entries, starts the `embedding-atlas` CLI, tracks progress, returns the local URL, and routes Atlas cache pruning through `server/atlas_cache.py`. Cache pruning removes old files first while preserving the parquet artifact needed by the current Atlas launch.
- Background jobs are managed by `server/jobs.py` and expose progress, cancellation, result, and error state through `/api/jobs/*`.

## Contribution

- Please use Issues for bug reports and feature requests.
- This repository uses pre-commit for code quality. Running `uv run pre-commit run --all-files` (or `uvx pre-commit run --all-files`) is equivalent to executing the following:

  - `uv run ruff format` (or `uvx ruff format`)
  - `uv run ruff check` (or `uvx ruff check`)
  - `uv run pyrefly check` (or `uvx pyrefly check`)
- Please make sure to fix all reported errors before committing.

## Acknowledgements

- [Dataset viewer (Huggingface)](https://github.com/huggingface/dataset-viewer): Used as a reference for UI/feature design.
- [Zarque-profiling](https://github.com/crescendo-medix/zarque-profiling): Used for EDA report generation.
- [Embedding Atlas](https://github.com/apple/embedding-atlas): Used for interactive embedding visualization.

## License

This repository is released under the MIT License.
