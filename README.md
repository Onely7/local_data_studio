<div align="center">

# Local Data Studio

**A GUI application for viewing and analyzing local datasets**

English | [日本語](docs/README_ja.md)
</div>

Local Data Studio is a web-based viewer for local datasets, inspired by [Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio) from Hugging Face Datasets.
It lets you browse, search, and analyze data in JSONL, JSON, CSV, TSV, and Parquet formats directly in your browser.

Its main features include fast previews, SQL execution with DuckDB, exploratory data analysis (EDA) report generation, and embedding visualization with Embedding Atlas.
The SQL console can also generate SQL from natural-language instructions using an LLM.

<div align="center">
<img src="images/local_data_studio_01.png" alt="Main screen of Local Data Studio" width="90%">
</div>

## Key Features

- Preview large datasets while limiting how much data is loaded at once
- Cursor-based pagination for efficient navigation between pages
- Read-only search and aggregation using DuckDB SQL
- SQL timeouts, memory limits, and warnings for potentially large scans
- EDA report generation for either an entire dataset or SQL query results
- Embedding Atlas visualization for text columns, image columns, or SQL query results
- A **Row Inspector** for viewing complete row contents
- Image rendering from URLs, local paths, and dictionaries in `{bytes, path}` format
- Enlarged image previews and navigation between multiple images in the same row
- Drag-and-drop file uploads
- Hiding rows within the current session, with optional deletion from the source file

## Supported Environments and Data Formats

- Python 3.11, 3.12, or 3.13
- Supported formats: `.jsonl`, `.json`, `.csv`, `.tsv`, and `.parquet`

## Installation

Choose the installation method that matches your intended use:

- For regular use: **Install from PyPI**
- For development or source-code changes: **Set up from source**

### Install from PyPI

Once the package has been published, install it with the following command:

```bash
python -m pip install local-data-studio
```

After installation, you can start the application with either of the following commands:

```bash
# List data files in the specified directory
local-data-studio --data-dir /local/data/path

# Run the same command through the Python module entry point
python -m local_data_studio --data-dir /local/data/path
```

Replace `/local/data/path` with the actual directory that contains the data you want to inspect.

To open a single file instead of a directory, use `--data-file` instead of `--data-dir`:

```bash
local-data-studio --data-file /local/data/example.parquet
```

After the server starts, open <http://127.0.0.1:8000> in your browser.

### Set Up from Source

Running from source requires Python 3.11–3.13, Git, and uv.

1. **Download the repository**

   ```bash
   # Download the repository from GitHub
   git clone https://github.com/Onely7/local_data_studio.git

   # Move into the downloaded directory
   cd local_data_studio
   ```

2. **Install the required dependencies**

   ```bash
   # Prepare the development environment from the project configuration
   uv sync
   ```

3. **Create the main configuration file**

   ```bash
   # Copy the template, then keep the project settings in this file
   cp local_data_studio.example.toml local_data_studio.toml
   ```

4. **Edit `local_data_studio.toml`**

   This is the normal place for Local Data Studio settings. Set `[paths].data_dir`
   to the directory that contains the data you want to inspect. To open one file,
   use `[paths].data_file` instead. The `[server]`, `[settings]`, and `[llm]`
   sections keep the remaining application, EDA, Atlas, deletion, and SQL model
   settings together.

   ```toml
   [paths]
   data_dir = "/local/data/path"

   [settings]
   eda_row_limit = 50000
   ```

5. **Create `.env` only when needed**

   `.env` is for provider credentials and exceptional local overrides, not for
   the regular application configuration. Copy the example only when you need
   one of those values:

   ```bash
   cp .env.example .env
   ```

   For example, add the credential named by an LLM profile's `api_key_env`:

   ```dotenv
   OPENAI_API_KEY=your_openai_api_key
   ```

   `.env` is ignored by Git. Operating-system environment variables and command
   line options can also override TOML values for a single launch.

6. **Start Local Data Studio**

   ```bash
   # Start with the project configuration and automatic reload for development
   uv run local-data-studio --config ./local_data_studio.toml --reload
   ```

7. **Open the application in your browser**

   Startup is complete when output similar to the following appears in the terminal:

   ```text
   INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
   INFO:     Application startup complete.
   ```

   Open <http://127.0.0.1:8000> in your browser to view Local Data Studio.

   To stop the server, press `Ctrl+C` in the terminal where it is running.

   When Local Data Studio runs on a remote server, forward only its port:

   ```bash
   ssh -N -L 8000:127.0.0.1:8000 user@example-server
   ```

   Open the same local URL afterward. Atlas pages are served through Local Data Studio, so separate SSH forwarding for the internal Atlas ports is unnecessary.

## Paths and Configuration Files

### Recommended Configuration Location

Keep one `local_data_studio.toml` in the workspace that contains the data,
cache, and local models for a project. This is the normal configuration file
for paths, server options, EDA, Atlas, deletion controls, and SQL model
profiles. Start the application with `--config ./local_data_studio.toml` so the
selected file is explicit and its relative paths resolve from that workspace.

Use `.env` only for API keys and occasional machine-specific overrides. It is
not a second required configuration file: when no credentials or overrides are
needed, you do not need to create it.

### Default Paths

Unless otherwise specified, the following files and directories are located or created relative to the directory in which you run the command:

- `.env`
- `data`
- `cache`
- `models/embedder`

The directory in which a command is run is called the **current working directory**.
To use the same base directory every time, specify `--workspace-dir` or `--config`.

You can override individual paths with the following command-line options:

- `--data-dir`
- `--data-file`
- `--cache-dir`
- `--models-dir`
- `--env-file`
- `--file-serve-roots`

### TOML Configuration File

Use `local_data_studio.toml` as the usual place to manage settings. TOML is a
text-based file format for defining configuration keys and values.

The repository includes [local_data_studio.example.toml](local_data_studio.example.toml), which contains paths, server settings, and optional LLM model profiles without credentials.
Copy it to `local_data_studio.toml`, then edit the paths and the profiles you intend to use:

```bash
cp local_data_studio.example.toml local_data_studio.toml
```

Store API keys in `.env` or your shell environment. The `api_key_env` setting in each model profile specifies the name of the credential variable to use. Keep routine paths and feature settings in TOML; use `.env` only when a credential or local override is needed.
The `model` setting accepts either one LiteLLM model string or a list of model strings from the same provider. A list shares that profile's credentials, endpoint, timeout, and `provider_options`; each model appears separately in the SQL Console selector. When `default_model` names the profile, its first listed model is selected by default.

The `[settings]` section configures EDA, Embedding Atlas, and source-file deletion. It uses lowercase snake_case names corresponding to the environment variables described below: for example, `EDA_ROW_LIMIT` becomes `eda_row_limit`, and `ALLOW_DELETE_DATA` becomes `allow_delete_data`. The template lists every supported setting. Omit a key to use a `.env` value or the application default.

Start the application with a configuration file as follows:

```bash
local-data-studio --config /path/to/local_data_studio.toml
```

When the same setting is defined in more than one place, the following precedence order applies.
Items higher in the list take priority over those below them.

1. Command-line options
2. Operating system environment variables
3. TOML configuration file
4. `.env`
5. Workspace-based defaults
6. Current-working-directory-based defaults

### Environment Variables and TOML Settings

All variables in the following sections can also be set in the TOML `[settings]` section using the lowercase snake_case form. Command-line options and OS environment variables take precedence over TOML, and TOML takes precedence over `.env`.

#### Data and Paths

- `DATA_FILE`: Specifies a single data file directly. When set, it takes precedence over `DATA_DIR`.
- `DATA_DIR`: Specifies the directory in which datasets are discovered. This is required unless `DATA_FILE` is used.
- `FILE_SERVE_ROOTS`: A comma-separated list of directories from which local images may be served.
- `VIS_EXCLUDE_DIRS`: A comma-separated list of directories under `DATA_DIR` to exclude from dataset discovery.
- `VIS_EXCLUDE_FILES`: A comma-separated list of files under `DATA_DIR` to exclude from dataset discovery. Relative paths are resolved from `DATA_DIR`, and absolute paths are also supported.

#### LLM Credentials

- `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and `GEMINI_API_KEY`: Examples of credential environment variables referenced by `api_key_env` in an LLM model profile. These values are never sent to the browser.

#### EDA

- `EDA_ROW_LIMIT`: The maximum number of rows loaded into an EDA report from either an entire dataset or SQL query results. This value cannot be changed from the UI. Any integer greater than or equal to `1` is accepted; use `-1` to disable the row limit.
- `EDA_CELL_MAX_CHARS`: The maximum number of characters retained from a string cell during EDA. Content beyond this limit is replaced with `... (truncated)`.
- `EDA_NESTED_POLICY`: Controls how nested values such as lists, structs, objects, and binary data are handled. `stringify` converts and retains them as strings, while `drop` removes the affected columns.
- `EDA_CACHE_MAX_BYTES`: The maximum total size of EDA reports stored in `./cache/eda`. The default is 1 GiB. When the limit is exceeded, the oldest reports are removed first.

#### Embedding Atlas

- `EMBEDDER_MODELS_DIR`: The parent directory containing local Hugging Face encoder models. By default, Local Data Studio uses `models/embedder` under the workspace or current working directory.
- `ATLAS_HOST`, `ATLAS_PORT`: The IPv4 loopback host and first internal candidate port used for an Embedding Atlas child process. `localhost` is normalized to `127.0.0.1`; IPv6 and externally reachable hosts are rejected. Browsers do not connect to this port directly.
- `ATLAS_MAX_INSTANCES`: The maximum number of pending and running Atlas child processes. The default is `4`, and the value must be at least `1`.
- `ATLAS_SAMPLE`: The maximum number of rows used for embedding computation, dimensionality reduction, and the cached Atlas Parquet file. Sampling is applied after the SQL query, with a fixed random seed of 42 so that the same rows are selected for the same input. When unset or set to `0`, all selected rows are used. Negative values are rejected.
- `ATLAS_BATCH_SIZE`: The number of rows processed at once during embedding computation. When unset or set to `0`, the Embedding Atlas default is used.
- `ATLAS_CACHE_MAX_BYTES`: The maximum total size of Embedding Atlas-related files stored in `./cache/atlas`. When the limit is exceeded, the oldest cache files are removed first.
- `ATLAS_TEXT_MAX_CHARS`: The maximum number of characters retained from a text cell for both embedding input and the cached Atlas Parquet file. Set this to `0` to disable truncation.
- `ATLAS_EMBEDDING_DTYPE`: The numeric precision used for embedding arrays before dimensionality reduction. Supported values are `float32` and `float16`.
- `ATLAS_UMAP_PROJECTION_MODE`: Controls how UMAP dimensionality reduction is performed. `full` processes all sampled embeddings together. `anchor_transform` fits UMAP on a representative anchor sample and then places the remaining rows into the same two-dimensional space. t-SNE and PCA always process all sampled rows together.
- `ATLAS_UMAP_ANCHOR_SAMPLE`: The number of rows used to fit UMAP when `ATLAS_UMAP_PROJECTION_MODE=anchor_transform`.
- `ATLAS_TRUST_REMOTE_CODE`: When set to `true`, permits the selected local encoder model's repository code to run while Local Data Studio loads the model. Leave this set to `false` unless you trust that model.

#### Data Deletion

- `ALLOW_DELETE_DATA`: When set to `false`, deletion from the source data file is disabled. Rows may still be hidden temporarily within the current session.

## Usage

### 1. Select a Data File

Select the file you want to inspect from the **DATASETS** list on the left.
You can also use the search box to filter the list by file name.

Long file names are shortened in the list.
File sizes are shown with up to three significant digits using the most appropriate unit: `Bytes`, `kB`, `MB`, `GB`, or `TB`.

<img src="images/local_data_studio_02.png" alt="Selecting a file from the DATASETS list" width="45%">

### 2. Browse and Search Data

Use the controls at the top of the page to change what is displayed:

- **Search**: Searches the data.
- **Rows**: Changes the number of rows shown per page.
- **Prev** / **Next**: Moves to the previous or next page.

<img src="images/local_data_studio_03.png" alt="Searching data and navigating between pages" width="45%">

### 3. Use the SQL Console

The SQL console lets you search and aggregate the `data` table using DuckDB SQL.
Only read-only queries are allowed.

When a LiteLLM model profile has been configured on the server, you can generate SQL from natural-language instructions, including instructions written in Japanese.
The SQL console can use configured OpenAI, Anthropic, Gemini, hosted vLLM, and other LiteLLM-compatible models.

Local Data Studio accepts only SQL returned by the LLM as plain text and does not use tool calls.
Generated SQL is restricted to either a single `SELECT` statement or a `SELECT` statement using a common table expression (CTE) introduced by a `WITH` clause.
SQL execution is subject to timeouts, memory limits, and checks for potentially expensive data scans.

<img src="images/local_data_studio_04.png" alt="SQL console" width="45%">

### 4. Generate an EDA Report

Select **Run EDA** to generate an EDA report from rows loaded from the dataset.
Generated reports are stored in the cache and may be reused when the same report is requested again under identical conditions.

Select **Run EDA on Query Results** to generate a report from the current results displayed in the SQL console.

Set the row limit with `eda_row_limit` in `[settings]`, or with `EDA_ROW_LIMIT` in the environment or `.env` file.
This value cannot be changed from the UI.
Any integer greater than or equal to `1` is accepted; use `-1` to remove the row limit.

The **Profile mode** option in the EDA panel controls the level of detail for each run.
The default value is `minimal`.

<img src="images/local_data_studio_05.png" alt="Running EDA" width="45%"> <img src="images/local_data_studio_06.png" alt="Generated EDA report" width="45%">

### 5. Visualize Embeddings

An embedding represents the features of text or an image as a sequence of numbers called a vector.
Because embeddings usually have too many dimensions to display directly, Local Data Studio converts them into two-dimensional coordinates using methods such as UMAP, t-SNE, or PCA. This process is commonly called **dimensionality reduction**.
Some configuration keys and internal code use the term `projection`, but this README uses **dimensionality reduction** as the general term for UMAP, t-SNE, and PCA.

First, place a local encoder model in Hugging Face format under `models/embedder`, or under the directory specified by `--models-dir` or `EMBEDDER_MODELS_DIR`.

Example locations:

```text
models/embedder/google/siglip2-base-patch16-224
models/embedder/Qwen/Qwen3-Embedding-0.6B
models/embedder/Qwen/Qwen3-VL-Embedding-2B
```

A directory appears in the **Model** selector when it contains model-identifying files such as `config.json`, `modules.json`, `tokenizer_config.json`, or `preprocessor_config.json`.

Under **Visualize Embedding**, select the following:

1. A text or image column
2. The model to use
3. The library used to run the model, referred to as the backend
4. The dimensionality reduction method used to produce two-dimensional coordinates

Available methods are **UMAP** (default), **t-SNE**, and **PCA**.
Select **Run Atlas** to start a local Embedding Atlas page.
Select **Run Atlas on Query Results** to visualize the current SQL query results instead of the full dataset.

When listing models, Local Data Studio inspects configuration files without loading model weights.
Unavailable backends are still shown in the list but cannot be selected.
When both Sentence Transformers and Transformers are supported, Sentence Transformers is selected by default.

When Sentence Transformers is selected, a **Prompt** field appears for entering additional instructions for the model.
If the field is left empty, the default prompt stored with the model is used.

- Text without a placeholder is prepended to each value in the selected column.
- Placeholders such as `{title}` and `{body}` are replaced with values from the corresponding columns in the same dataset row or SQL result row.
- `{{` and `}}` are treated as literal braces.
- Missing columns, unsupported conversions, format specifiers, and unmatched braces are rejected before the model is loaded.

The operation runs as a background job so that the rest of the interface remains responsive. Progress includes the current phase and advances after embedding batches; cancellation takes effect at the next cooperative batch or phase boundary.
An **Open Atlas** link appears only after the Atlas page and metadata endpoint are ready. The link uses `/atlas/{instance_id}/` on the same Local Data Studio origin, so internal child ports are never exposed to the browser.

<img src="images/local_data_studio_07.png" alt="Embedding Atlas settings" width="45%"> <img src="images/local_data_studio_08.png" alt="Embedding Atlas visualization" width="45%">

### 6. Use the Row Inspector and Enlarged Image View

Click a row to display the full contents of each column in the **Row Inspector**.
Long values are shortened by default. Switch to **Raw** to view the complete value.

Values recognized as images can be clicked to open an enlarged preview.
The following formats are supported:

- Image URLs
- Relative or absolute file paths
- Dictionaries in `{ "bytes": ..., "path": ... }` format

When both `bytes` and `path` are present, Local Data Studio first attempts to display the `bytes` value.
If that fails, it uses `path` as a fallback.

<img src="images/local_data_studio_09.png" alt="Row Inspector" width="45%"> <img src="images/local_data_studio_10.png" alt="Enlarged image preview" width="45%">

## Usage Notes

- Searching and generating EDA reports may take time for large datasets.
- Row counting, full-dataset search, sample statistics, and EDA run as background jobs that support progress reporting and cancellation.
- Setting `EDA_ROW_LIMIT=-1` loads all selected rows into memory for analysis. Use this only when the complete dataset or query result fits comfortably in memory.
- JSON array files at terabyte scale are not recommended. Use JSONL or Parquet for efficient access to large datasets.
- t-SNE can become dramatically slower and consume much more memory as the amount of data increases. Set a practical `ATLAS_SAMPLE` limit for large datasets.
- **Delete from file** modifies the original data file. Create a backup beforehand when necessary.
- When `ALLOW_DELETE_DATA=false`, rows are not removed from the source file. They can only be hidden within the current session.
- Place encoder model files yourself under `models/embedder` or the configured model directory. Model weights are not included in the distribution; only a placeholder used to preserve the empty directory is included.

## Advanced Settings and Technical Details

This section is intended for users and administrators who need details about how Local Data Studio works.
You can skip it if you only need the standard viewing and analysis features.

<details>
<summary><strong>Large-Dataset Preview and Background Processing</strong></summary>

For supported formats, previews of very large datasets use cursor-based `page_token` values instead of large `OFFSET` values.
Row counting, full-dataset search, sample statistics, and EDA run as background jobs that support progress reporting and cancellation.

Feedback shown after Count Rows, EDA, and Atlas operations uses a consistent compact status style that does not distract from surrounding controls.

</details>

<details>
<summary><strong>LLM Model Profiles for SQL Generation</strong></summary>

Model profiles used for SQL generation are loaded from the `[llm]` section of `local_data_studio.toml`.
Model names must include an explicit LiteLLM provider prefix such as `openai/`, `anthropic/`, `gemini/`, or `hosted_vllm/`.
The deprecated `vllm/` prefix is not accepted.

Set `model` to one model string or a list of model strings from one provider. The SQL Console exposes each listed model as a separate choice. Profiles must not mix providers because all models in a profile share the same credentials, optional endpoint, timeout, and `provider_options`.

Store credentials in the environment variables referenced by `api_key_env`.
`provider_options` contains trusted administrator-managed settings.
It may define values such as `reasoning_effort`, `thinking`, token limits, `top_k`, and `extra_body`.
Settings that attempt to replace messages, credentials, streaming behavior, tools, multimodal input, or structured responses are rejected.

`OPENAI_MODEL` and `OPENAI_BASE_URL` are no longer Local Data Studio configuration options.
When starting the Local Data Studio application directly through Uvicorn, specify the same TOML file with `LOCAL_DATA_STUDIO_CONFIG_FILE`. Its `[settings]` and `[llm]` sections are both applied.

</details>

<details>
<summary><strong>EDA Cache</strong></summary>

EDA reports for an entire dataset are stored in `./cache/eda` based on the file fingerprint, row limit, and profile mode.
A fingerprint is identifying information used to determine whether two inputs refer to the same file state.

Reports generated from SQL query results are stored separately based on the file fingerprint, SQL query, row limit, and profile mode.
The total EDA cache size is limited to 1 GiB by default. When `EDA_CACHE_MAX_BYTES` is exceeded, the oldest reports are removed first.

For **Run EDA on Query Results**, internal helper columns such as `rn` and `__rowid` are excluded from the report.

</details>

<details>
<summary><strong>Embedding Atlas Computation and Cache</strong></summary>

An Embedding Atlas job computes embeddings with the selected local encoder model and then reduces those embeddings to two dimensions, so processing may take some time.

Parquet files containing the selected display rows and their two-dimensional coordinates are stored in `./cache/atlas/datasets`.
An existing cache entry is reused only when all of the following match:

- Dataset fingerprint
- SQL query
- Selected column
- Model
- Backend
- Prompt template
- Model configuration information used to determine backend compatibility
- Dimensionality reduction method and settings

Columns used for image display preserve their original URL, path, or `{bytes, path}` representation.
Values converted into encoder input format are stored only in an internal embedding-input column.

`ATLAS_SAMPLE=N` applies deterministic sampling in DuckDB after SQL filtering and before pandas DataFrame creation. At most `N` rows are therefore materialized for embedding computation, dimensionality reduction, and the cached Parquet file. Setting `ATLAS_SAMPLE=0` keeps the unbounded all-row behavior.

Text and prompt templates are expanded only for the current embedding batch. Image bytes are decoded into a temporary disk-backed spool and loaded into memory only for the current batch; the spool is removed after success, failure, or cancellation. Full UMAP, t-SNE, and PCA still require the complete sampled embedding matrix. `anchor_transform` avoids retaining non-anchor embeddings after each transform batch.

UMAP supports both `full` and `anchor_transform` modes.
t-SNE and PCA perform dimensionality reduction over all sampled embeddings together.

Use `ATLAS_TEXT_MAX_CHARS` to limit long text columns and expanded prompts, `ATLAS_EMBEDDING_DTYPE=float16` to reduce embedding memory usage, and `ATLAS_CACHE_MAX_BYTES` to control the total cache size.

Local Data Studio keeps at most `ATLAS_MAX_INSTANCES` pending or running Atlas processes. A running instance can be stopped without restarting the application:

```bash
curl -X DELETE http://127.0.0.1:8000/api/atlas/instances/INSTANCE_ID
```

The instance ID is the value between `/atlas/` and the final `/` in the **Open Atlas** URL. It is an unguessable routing identifier, not an authentication mechanism. Do not expose an unauthenticated Local Data Studio server directly to an untrusted network; bind it to loopback or access it through an SSH tunnel.

</details>

<details>
<summary><strong>Embedding Model Backend Detection</strong></summary>

Backend compatibility is not inferred from the model name alone.
Local Data Studio inspects a bounded amount of local model metadata, including:

- `modules.json`
- `config.json`
- Tokenizer and processor configuration
- Pooling configuration
- Normalization metadata

Sentence Transformers reports one of `native`, `generic_fallback`, `metadata_only`, `unsupported`, or `unknown`.
Transformers reports one of `direct`, `remote_code`, `backbone_only`, `unsupported`, or `unknown`.

Sentence Transformers `generic_fallback` is available only for text-only Transformers models with a detectable tokenizer.
Image and multimodal models require a native `modules.json` to run through Sentence Transformers.

As a result, image-only DINOv3 checkpoints can use only the Transformers backend and rely on the model-declared `pooler_output`.
Qwen3-VL-Embedding models with a native Sentence Transformers pipeline can use both backends.

Only backends for which an executable adapter can be confirmed are selectable.
`remote_code` is available only when `ATLAS_TRUST_REMOTE_CODE=true` explicitly permits execution of code from the model repository.
Built-in Transformer, Pooling, and Normalize pipelines are reproduced by the Transformers adapter without importing Python code from the model repository.

</details>

<details>
<summary><strong>Cache Locations and Invalidation</strong></summary>

Caches are separated by purpose and stored in the following directories:

- `./cache/metadata`
- `./cache/index`
- `./cache/stats`
- `./cache/count`
- `./cache/search`
- `./cache/eda`
- `./cache/atlas`

Embedding Atlas caches are stored under `./cache/atlas`, and Parquet files containing dimensionality-reduced coordinates are stored in `./cache/atlas/datasets`.
The total EDA report size is controlled by `EDA_CACHE_MAX_BYTES`, while the total Embedding Atlas cache size is controlled by `ATLAS_CACHE_MAX_BYTES`. When either limit is exceeded, the oldest files in the corresponding cache are removed first. Cache replacement is atomic, so interrupted writes do not replace a valid JSON cache with a partial file.

Caches that use fingerprints are invalidated based on the source file path, size, and modification time.
Here, invalidation means that an old cache entry is not reused when the source file is considered to have changed.

</details>

## Developer Information

For details about the internal architecture, the roles of the main modules, development startup commands, and implementation-specific considerations, see [IMPLEMENTATION_NOTES.md](docs/IMPLEMENTATION_NOTES.md).

## Contributing

Please use GitHub Issues to report bugs or propose new features.

After changing the code, run pre-commit before committing:

```bash
# Run automatic formatting, linting, and type checking on all files
uv run pre-commit run --all-files
```

To run pre-commit without installing it into the project environment, you can also use:

```bash
uvx pre-commit run --all-files
```

These commands primarily run the following checks:

- `uv run ruff format` or `uvx ruff format`: Automatically formats the code.
- `uv run ruff check` or `uvx ruff check`: Checks the code for lint errors.
- `uv run ty check` or `uvx ty check`: Checks for type inconsistencies.

Ruff applies Google-style docstrings based on PEP 257 to both application and test code.
For public APIs, document constraints, exceptions, side effects, and ownership semantics that are not clear from types and names alone.
For private implementation details, do not add docstrings that merely restate what the code does.

Resolve all reported errors before committing.

## Acknowledgments

- [Dataset Viewer (Hugging Face)](https://github.com/huggingface/dataset-viewer): Used as a reference for UI and feature design.
- [YData Profiling](https://github.com/ydataai/ydata-profiling): Used to generate EDA reports.
- [Embedding Atlas](https://github.com/apple/embedding-atlas): Used for interactive embedding visualization.

## License

This repository is released under the MIT License.
