<div align="center">

# Local Data Studio

**A GUI application for viewing and analyzing local datasets**

English | [日本語](docs/README_ja.md)

</div>

Local Data Studio is a web-based viewer for local environments, inspired by [Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio) from Hugging Face Datasets.
It lets you browse, search, and analyze data in JSONL, JSON, CSV, TSV, and Parquet formats directly in your browser.

Its main features include fast previews, SQL execution with DuckDB, exploratory data analysis (EDA) report generation, and embedding visualization with Embedding Atlas.
The SQL console can also generate SQL from natural-language instructions using an LLM.

<div align="center">
<img src="images/local_data_studio_01.png" alt="Main screen of Local Data Studio" width="90%">
</div>

## Key Features

* Preview large datasets while limiting how much data is loaded at once
* Navigate efficiently between pages with cursor-based pagination
* Run read-only searches and aggregations with DuckDB SQL
* Translate visible cells or columns manually with configured LiteLLM models
* Apply SQL timeouts, memory limits, and warnings for potentially large scans
* Generate EDA reports for an entire dataset or SQL query results
* Visualize embeddings for text columns, image columns, or SQL query results with Embedding Atlas
* Inspect complete row contents with the **Row Inspector**
* Display images from URLs, local paths, and dictionaries in `{bytes, path}` format
* Open enlarged image previews and switch between multiple images in the same row
* Upload files by dragging and dropping them into the application
* Hide rows during the current session and optionally delete them from the source data file

## Supported Environments and Data Formats

* Python 3.11, 3.12, or 3.13
* Supported formats: `.jsonl`, `.json`, `.csv`, `.tsv`, and `.parquet`

## Installation

Choose the installation method that matches how you plan to use Local Data Studio.

* For regular use: **Install from PyPI**
* For development or source-code changes: **Set up from source**

### Install from PyPI

Install it with the following command:

```bash
pip install local-data-studio
```

After installation, you can start the application with either of the following commands:

```bash
# List the data files in the specified directory
local-data-studio --data-dir /local/data/path

# Run the same command through the Python module entry point
python -m local_data_studio --data-dir /local/data/path
```

Replace `/local/data/path` with the path to the directory that contains the data you want to inspect.

To open a single file instead of a directory, use `--data-file` instead of `--data-dir`:

```bash
local-data-studio --data-file /local/data/example.parquet
```

After the server starts, open <http://127.0.0.1:8000> in your browser.

### Set Up from Source

Running Local Data Studio from source requires the following software:

* Python 3.11–3.13
* Git, which is used to download the source code from GitHub
* uv, which prepares the Python environment and installs the required dependencies

1. **Download the repository**

   ```bash
   # Download the repository from GitHub
   git clone https://github.com/Onely7/local_data_studio.git

   # Move into the downloaded directory
   cd local_data_studio
   ```

2. **Prepare the development environment**

   ```bash
   # Install the required dependencies from the project configuration
   uv sync
   ```

3. **Create the main configuration file**

   ```bash
   # Copy the configuration template
   cp local_data_studio.example.toml local_data_studio.toml
   ```

4. **Edit `local_data_studio.toml`**

   Keep the regular Local Data Studio settings in this file.

   * Directory containing the data to inspect: `[paths].data_dir`
   * Single data file to inspect: `[paths].data_file`
   * Server settings: `[server]`
   * EDA, Atlas, deletion, and related settings: `[settings]`
   * LLM settings used for SQL generation and manual translation: `[llm]`
   * Translation request limits: `[translation]`

   For example, configure the data location and the maximum number of rows loaded for EDA as follows:

   ```toml
   [paths]
   data_dir = "/local/data/path"

   [settings]
   eda_row_limit = 50000
   ```

5. **Create `.env` only when needed**

   The `.env` file stores LLM provider API keys and optional overrides that should apply only to the current computer.
   Regular application settings belong in `local_data_studio.toml`, so you do not need to create `.env` when no API keys or machine-specific overrides are required.

   When needed, copy the template with the following command:

   ```bash
   cp .env.example .env
   ```

   For example, add the API key referenced by an LLM model profile's `api_key_env` setting:

   ```dotenv
   OPENAI_API_KEY=your_openai_api_key
   ```

   `.env` is excluded from Git tracking.

6. **Start Local Data Studio**

   ```bash
   # Start with the selected configuration file and enable automatic reload for development
   uv run local-data-studio --config ./local_data_studio.toml --reload
   ```

7. **Open the application in your browser**

   Startup is complete when the terminal displays messages similar to the following:

   ```text
   INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
   INFO:     Application startup complete.
   ```

   Open <http://127.0.0.1:8000> in your browser to view Local Data Studio.

   To stop the server, press `Ctrl+C` in the terminal where it is running.

#### Using a Remote Server

When Local Data Studio is running on another computer, use an SSH tunnel to forward only the Local Data Studio port to your local computer:

```bash
ssh -N -L 8000:127.0.0.1:8000 user@example-server
```

Replace `user@example-server` with the actual user name and server address.
After the connection is established, open <http://127.0.0.1:8000> in your local browser.

Atlas pages are served through Local Data Studio, so you do not need to forward the internal Atlas ports separately.

## Paths and Configuration Files

### Recommended Configuration Location

In this README, a **workspace** means the working directory that contains your data, cache, local models, and configuration files.

For each project, we recommend placing one `local_data_studio.toml` file in the workspace.
Use this file to manage paths, server settings, EDA, Atlas, deletion controls, and LLM model profiles for SQL generation.

Starting the application with `--config` makes the selected configuration file explicit and causes relative paths to be resolved from that workspace:

```bash
local-data-studio --config ./local_data_studio.toml
```

Use `.env` for API keys and optional computer-specific overrides only.
You do not need to create it when no credentials or overrides are required.

### Default Paths

Unless otherwise specified, the following files and directories are located or created relative to the directory in which you run the command:

* `.env`
* `data`
* `cache`
* `models/embedder`

The directory in which a command is run is called the **current working directory**.
To use the same base directory every time, specify `--workspace-dir` or `--config`.

You can override individual paths with the following command-line options:

* `--data-dir`
* `--data-file`
* `--cache-dir`
* `--models-dir`
* `--env-file`
* `--file-serve-roots`

### TOML Configuration File

Use `local_data_studio.toml` as the standard location for application settings.
TOML is a text-based file format for defining configuration keys and values.

The repository includes [local_data_studio.example.toml](local_data_studio.example.toml), which contains example paths, server settings, and LLM model profiles without credentials.
Copy it to `local_data_studio.toml`, then edit the paths and model profiles you intend to use:

```bash
cp local_data_studio.example.toml local_data_studio.toml
```

Store API keys in `.env` or shell environment variables.
The `api_key_env` setting in each model profile specifies the name of the environment variable that contains its credential.
Keep regular paths and feature settings in TOML, and use `.env` only for credentials or optional local overrides.

The `model` setting accepts either one LiteLLM model string or a list of model strings from the same provider.
Models in the list share the profile's credentials, endpoint, timeout, and `provider_options`, and each model appears separately in the SQL console.
When `default_model` names a profile ID, the first model in that profile is selected initially.

The `[settings]` section configures EDA, Embedding Atlas, source-file deletion, and related behavior.
Use the lowercase snake_case form of each environment variable name.
For example, `EDA_ROW_LIMIT` becomes `eda_row_limit`, and `ALLOW_DELETE_DATA` becomes `allow_delete_data`.
The template lists all supported settings.
When a key is omitted, Local Data Studio uses the value from `.env` or the application default.

When the same setting is defined in more than one place, the following precedence order applies.
Items higher in the list take priority over those below them.

1. Command-line options
2. Operating system environment variables
3. TOML configuration file
4. `.env`
5. Workspace-based defaults
6. Current-working-directory-based defaults

### Environment Variables and TOML Settings

The environment variables described below can also be specified in the TOML `[settings]` section using their lowercase snake_case names.
Command-line options and operating system environment variables take precedence over TOML, and TOML takes precedence over `.env`.

#### Data and Paths

* `DATA_FILE`: Specifies a single data file directly. When set, it takes precedence over `DATA_DIR`.
* `DATA_DIR`: Specifies the directory in which datasets are discovered. This is required unless `DATA_FILE` is used.
* `FILE_SERVE_ROOTS`: A comma-separated list of directories from which local images may be served.
* `VIS_EXCLUDE_DIRS`: A comma-separated list of directories under `DATA_DIR` to exclude from dataset discovery.
* `VIS_EXCLUDE_FILES`: A comma-separated list of files under `DATA_DIR` to exclude from dataset discovery. Relative paths are resolved from `DATA_DIR`, and absolute paths are also supported.

#### LLM Credentials

* `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and `GEMINI_API_KEY`: Examples of credential environment variables referenced by `api_key_env` in an LLM model profile. These values are never sent to the browser.

#### EDA

* `EDA_ROW_LIMIT`: The maximum number of rows loaded into an EDA report from either an entire dataset or SQL query results. This value cannot be changed from the UI. Any integer greater than or equal to `1` is accepted; use `-1` to disable the row limit.
* `EDA_CELL_MAX_CHARS`: The maximum number of characters retained from a string cell during EDA. Content beyond this limit is replaced with `... (truncated)`.
* `EDA_NESTED_POLICY`: Controls how nested values such as lists, structs, objects, and binary data are handled. `stringify` converts and retains them as strings, while `drop` removes the affected columns.
* `EDA_CACHE_MAX_BYTES`: The maximum total size of EDA reports stored in `./cache/eda`. The default is 1 GiB. When the limit is exceeded, the oldest reports are removed first.

#### Embedding Atlas

* `EMBEDDER_MODELS_DIR`: The parent directory containing local Hugging Face encoder models. By default, Local Data Studio uses `models/embedder` under the workspace or current working directory.
* `ATLAS_HOST`, `ATLAS_PORT`: The IPv4 loopback host used by an Embedding Atlas child process and the first port number to check when selecting an internal port. `localhost` is normalized to `127.0.0.1`. IPv6 and externally reachable hosts are not allowed. Browsers do not connect to this port directly.
* `ATLAS_MAX_INSTANCES`: The maximum number of pending and running Atlas child processes combined. The default is `4`, and the value must be at least `1`.
* `ATLAS_SAMPLE`: The maximum number of rows used for embedding computation, dimensionality reduction, and the cached Atlas Parquet file. Sampling is applied after the SQL query, with a fixed random seed of 42 so that the same rows are selected for the same input. When unset or set to `0`, all selected rows are used. Negative values are rejected.
* `ATLAS_BATCH_SIZE`: The number of rows processed at once during embedding computation. When unset or set to `0`, the Embedding Atlas default is used.
* `ATLAS_CACHE_MAX_BYTES`: The maximum total size of Embedding Atlas-related files stored in `./cache/atlas`. When the limit is exceeded, the oldest cache files are removed first.
* `ATLAS_TEXT_MAX_CHARS`: The maximum number of characters retained from a text cell for both embedding input and the cached Atlas Parquet file. Set this to `0` to disable truncation.
* `ATLAS_EMBEDDING_DTYPE`: The numeric precision used for embedding arrays before dimensionality reduction. Supported values are `float32` and `float16`.
* `ATLAS_UMAP_PROJECTION_MODE`: Controls how UMAP dimensionality reduction is performed. `full` processes all sampled embeddings together. `anchor_transform` determines the layout from a representative set of rows, then places the remaining rows into the same two-dimensional space. t-SNE and PCA always process all sampled rows together.
* `ATLAS_UMAP_ANCHOR_SAMPLE`: The number of rows used to fit UMAP when `ATLAS_UMAP_PROJECTION_MODE=anchor_transform`.
* `ATLAS_TRUST_REMOTE_CODE`: When set to `true`, permits Local Data Studio to execute code from the selected local encoder model's repository while loading the model. Leave this set to `false` unless you trust the model.

#### Data Deletion

* `ALLOW_DELETE_DATA`: When set to `false`, deletion from the source data file is disabled. Rows may still be hidden temporarily within the current session.

## Usage

### 1. Select a Data File

Select the file you want to inspect from the **DATASETS** list on the left.
You can also use the search box to filter the list by file name.

Long file names are shortened in the list.
File sizes are shown with up to three significant digits using the most appropriate unit: `Bytes`, `kB`, `MB`, `GB`, or `TB`.

<img src="images/local_data_studio_02.png" alt="Selecting a file from the DATASETS list" width="45%">

### 2. Browse and Search Data

Use the controls at the top of the page to change what is displayed:

* **Search**: Searches the data.
* **Rows**: Changes the number of rows shown per page.
* **Prev** / **Next**: Moves to the previous or next page.

<img src="images/local_data_studio_03.png" alt="Searching data and navigating between pages" width="45%">

### 3. Use the SQL Console

The SQL console lets you search and aggregate the `data` table, which represents the selected dataset, using DuckDB SQL.
Only read-only queries that do not modify the data are allowed.

When a LiteLLM model profile has been configured on the server, you can generate SQL from natural-language instructions, including instructions written in languages such as Japanese.
The SQL console can use configured OpenAI, Anthropic, Gemini, hosted vLLM, and other LiteLLM-compatible models.

Local Data Studio accepts only SQL returned by the LLM as plain text and does not use tool calls.
Generated SQL is restricted to either a single `SELECT` statement or a `SELECT` statement using a common table expression (CTE) introduced by a `WITH` clause.
SQL execution is subject to timeouts, memory limits, and checks for queries that may read a large amount of data.

<img src="images/local_data_studio_04.png" alt="SQL console" width="45%">

### 4. Translate Visible Values

Configure a model profile with `translation = true`, then select its model and a target language in the toolbar.
The translation icon in an expanded field translates that cell, while the icon in a column header translates the values currently visible in that column.
Column translation is offered only when at least one visible value contains natural-language text.
Numeric-only values and containers, booleans, binary values, and recognized image or audio data are excluded.

Translation is always manual. It does not modify the dataset, load another page, scan the source file, or fetch the complete **Raw** value.
Only the bounded values already loaded in the current Preview, search result, or SQL result page are sent to the selected LLM provider.
The original value remains visible and the translated value appears underneath it.
Expanded values and translations have separate copy controls.
For lists and objects, the code-view control shows the original JSON together with its translation and the applicable copy and translation controls.

Selection menus display about six choices at a time and scroll for additional choices.
The lower fade is removed at the final choice so the end of the list remains clear.

Large requests require confirmation. Results are retained only in browser memory for the current page session and are not written to Local Data Studio's server cache or `localStorage`.
Changing the dataset, view, page, model, language, or source value prevents a result from being shown in the wrong context.

### 5. Generate an EDA Report

Select **Run EDA** to generate an EDA report from rows loaded from the dataset.
Generated reports are stored in the cache and may be reused when the same report is requested again under identical conditions.

Select **Run EDA on Query Results** to generate a report from the current results displayed in the SQL console.

Set the row limit with `eda_row_limit` in `[settings]`, or with `EDA_ROW_LIMIT` in the environment or `.env` file.
This value cannot be changed from the UI.
Any integer greater than or equal to `1` is accepted; use `-1` to remove the row limit.

The **Profile mode** option in the EDA panel controls the level of detail for each run.
The default value is `minimal`.

<img src="images/local_data_studio_05.png" alt="Running EDA" width="45%"> <img src="images/local_data_studio_06.png" alt="Generated EDA report" width="45%">

### 6. Visualize Embeddings

An embedding represents the features of text or an image as a sequence of numbers called a vector.
Embeddings usually contain many numbers, so they cannot be displayed clearly on a screen in their original form.
Local Data Studio uses methods such as UMAP, t-SNE, and PCA to convert embeddings into two-dimensional coordinates for visualization.
This process is commonly called **dimensionality reduction**.

Some configuration keys and internal code use the term `projection`, but this README uses **dimensionality reduction** as the general term for UMAP, t-SNE, and PCA.

First, place a local encoder model in Hugging Face format under `models/embedder`, or under the directory specified by `--models-dir` or `EMBEDDER_MODELS_DIR`.
An encoder model is a model that converts text or images into embeddings.

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

The backend is the library that loads the model and computes the embeddings.

Available dimensionality reduction methods are **UMAP** (default), **t-SNE**, and **PCA**.
Select **Run Atlas** to start a local Embedding Atlas page.
Select **Run Atlas on Query Results** to visualize the current SQL query results instead of the full dataset.

When listing models, Local Data Studio inspects configuration files without loading model weights.
Unavailable backends are still shown in the list but cannot be selected.
When both Sentence Transformers and Transformers are supported, Sentence Transformers is selected initially.

When Sentence Transformers is selected, a **Prompt** field appears for entering additional instructions for the model.
If the field is left empty, the default prompt stored with the model is used.

* Text without a placeholder is prepended to each value in the selected column.
* Placeholders such as `{title}` and `{body}` are replaced with values from the corresponding columns in the same dataset row or SQL result row.
* `{{` and `}}` are treated as literal braces.
* Missing columns, unsupported conversions or format specifications, and unmatched braces are rejected before the model is loaded.

The operation runs as a background job so that the rest of the interface remains responsive.
Progress shows the current phase and advances after each embedding batch.
Cancellation takes effect after the current batch or processing phase finishes.

An **Open Atlas** link appears only after both the Atlas page and the metadata endpoint used for readiness checks are available.
The link uses `/atlas/{instance_id}/` on the same Local Data Studio origin, so internal child-process ports are never exposed to the browser.

<img src="images/local_data_studio_07.png" alt="Embedding Atlas settings" width="45%"> <img src="images/local_data_studio_08.png" alt="Embedding Atlas visualization" width="45%">

### 7. Use the Row Inspector and Enlarged Image View

Click a row to display the full contents of each column in the **Row Inspector**.
On desktop-sized viewports, the dataset list, Preview, and inspector share the available viewport height and scroll independently, keeping the application page itself fixed.
On mobile and tablet layouts, the page returns to normal vertical scrolling and places the **DATASETS** block directly below the title bar, before the Preview controls.
Long values are shortened by default. Switch to **Raw** to view the complete value.

Values recognized as images can be clicked to open an enlarged preview.
The following formats are supported:

* Image URLs
* Relative or absolute file paths
* Dictionaries in `{ "bytes": ..., "path": ... }` format

When both `bytes` and `path` are present, Local Data Studio first attempts to display the `bytes` value.
If that fails, it uses `path` as a fallback.

<img src="images/local_data_studio_09.png" alt="Row Inspector" width="45%"> <img src="images/local_data_studio_10.png" alt="Enlarged image preview" width="45%">

## Usage Notes

* Searching and generating EDA reports may take time for large datasets.
* Row counting, full-dataset search, sample statistics, and EDA run as background jobs that support progress reporting and cancellation.
* Setting `EDA_ROW_LIMIT=-1` loads all selected rows into memory for analysis. Use this only when the complete dataset or query result fits comfortably in memory.
* JSON array files at terabyte scale are not recommended. Use JSONL or Parquet for more efficient access to large datasets.
* t-SNE can become dramatically slower and consume much more memory as the amount of data increases. Set a practical `ATLAS_SAMPLE` limit for large datasets.
* **Delete from file** modifies the original data file. Create a backup beforehand when necessary.
* When `ALLOW_DELETE_DATA=false`, rows are not removed from the source data file. They can only be hidden temporarily within the current session.
* Place encoder model files yourself under `models/embedder` or the configured model directory. Model weights are not included in the distribution; only a placeholder used to preserve the empty directory is included.

## Advanced Settings and Technical Details

This section is intended for users and administrators who need more information about how Local Data Studio works.
You can skip it if you only need the standard viewing and analysis features.

<details>
<summary><strong>Large-Dataset Preview and Background Processing</strong></summary>

For supported formats, previews of very large datasets use cursor-based `page_token` values instead of large `OFFSET` values.
This avoids repeatedly counting through a large number of earlier rows and makes moving between pages more efficient.

Row counting, full-dataset search, sample statistics, and EDA run as background jobs that support progress reporting and cancellation.

Messages shown after Count Rows, EDA, and Atlas operations use a consistent compact format that does not distract from surrounding controls.

</details>

<details>
<summary><strong>LLM Model Profiles for SQL Generation and Translation</strong></summary>

Model profiles used for SQL generation and translation are loaded from the `[llm]` section of `local_data_studio.toml`.
Model names must include an explicit LiteLLM provider prefix such as `openai/`, `anthropic/`, `gemini/`, or `hosted_vllm/`.
A provider prefix is an identifier at the beginning of a model name that indicates which provider should be used.
The deprecated `vllm/` prefix is not accepted.

Set `model` to one model string or a list of model strings from the same provider.
Each model in the list appears as a separate choice for every capability enabled on its profile.

Set `sql_generation = true` to make a profile available in the SQL console and `translation = true` to make it available in the translation toolbar.
For backward compatibility, `sql_generation` defaults to `true`, while translation is opt-in and defaults to `false`.
Profiles that disable both capabilities are rejected.

Use `default_sql_generation_model` and `default_translation_model` to select independent defaults.
The legacy `default_model` key remains an alias for the SQL default; configuring it with a different value from `default_sql_generation_model` is rejected.

Models in the same profile share the credentials, optional endpoint, timeout, and `provider_options` defined by that profile.
For this reason, one profile cannot mix models from different providers.

Store credentials in the environment variables referenced by `api_key_env`.
`provider_options` contains trusted administrator-managed settings.
It may define values such as `reasoning_effort`, `thinking`, token limits, `top_k`, and `extra_body`.
Settings that attempt to replace messages, credentials, streaming behavior, tools, multimodal input, or structured responses are rejected.

`OPENAI_MODEL` and `OPENAI_BASE_URL` are not used as Local Data Studio configuration options.
When starting the Local Data Studio application directly through Uvicorn, specify the same TOML file with `LOCAL_DATA_STUDIO_CONFIG_FILE`.
Its `[settings]` and `[llm]` sections are both applied.

The optional `[translation]` section controls row, string, character, chunk, concurrency, and browser-confirmation limits.
The server recalculates the hard limits for every request instead of trusting browser-provided counts.

</details>

<details>
<summary><strong>EDA Cache</strong></summary>

EDA reports for an entire dataset are stored in `./cache/eda` based on the file fingerprint, row limit, and profile mode.
A fingerprint is identifying information used to determine whether the same file is still in the same state.

Reports generated from SQL query results are stored separately based on the file fingerprint, SQL query, row limit, and profile mode.
The total EDA cache size is limited to 1 GiB by default. When `EDA_CACHE_MAX_BYTES` is exceeded, the oldest reports are removed first.

For **Run EDA on Query Results**, internal helper columns such as `rn` and `__rowid` are excluded from the report.

</details>

<details>
<summary><strong>Embedding Atlas Computation and Cache</strong></summary>

An Embedding Atlas job computes embeddings with the selected local encoder model and then reduces those embeddings to two dimensions.
Depending on the amount of data and the selected model, the operation may take some time to complete.

Parquet files containing the selected display rows and their two-dimensional coordinates are stored in `./cache/atlas/datasets`.
An existing cache entry is reused only when all of the following match:

* Dataset fingerprint
* SQL query
* Selected column
* Model
* Backend
* Prompt template
* Model configuration information used to determine backend compatibility
* Dimensionality reduction method and settings

Columns used for image display preserve their original URL, path, or `{bytes, path}` representation.
Values converted into encoder input format are stored only in an internal embedding-input column.

When `ATLAS_SAMPLE=N` is specified, DuckDB selects rows deterministically after SQL filtering and before creating a pandas DataFrame.
As a result, at most `N` rows are materialized for embedding computation, dimensionality reduction, and the cached Atlas Parquet file.
When `ATLAS_SAMPLE=0`, all selected rows are processed.

Text and prompt templates are expanded only for the current embedding batch.
Image `bytes` values are written incrementally to a temporary disk-backed work area, and only the data required for the current batch is loaded into memory.
This temporary area is removed after success, failure, or cancellation.

However, UMAP in `full` mode, t-SNE, and PCA require the complete sampled embedding matrix.
In `anchor_transform` mode, only the anchor embeddings and the current processing batch are retained.

UMAP supports both `full` and `anchor_transform` modes.
t-SNE and PCA perform dimensionality reduction over all sampled embeddings together.

Use `ATLAS_TEXT_MAX_CHARS` to limit long text columns and expanded prompts, `ATLAS_EMBEDDING_DTYPE=float16` to reduce embedding memory usage, and `ATLAS_CACHE_MAX_BYTES` to control the total cache size.

Local Data Studio keeps at most `ATLAS_MAX_INSTANCES` pending and running Atlas processes combined.
A running Atlas instance can be stopped without restarting Local Data Studio:

```bash
curl -X DELETE http://127.0.0.1:8000/api/atlas/instances/INSTANCE_ID
```

The instance ID is the value between `/atlas/` and the final `/` in the **Open Atlas** URL.
This hard-to-guess value identifies the proxy destination, but it is not an authentication mechanism.

Do not expose Local Data Studio directly to an untrusted network when authentication is not configured.
Bind it only to a loopback address such as `127.0.0.1`, or access it through an SSH tunnel.

</details>

<details>
<summary><strong>Embedding Model Backend Detection</strong></summary>

Backend compatibility is not determined from the model name alone.
Local Data Studio inspects a bounded amount of the following local configuration information:

* `modules.json`
* `config.json`
* Tokenizer configuration, which describes how text is divided into model input units, and processor configuration, which describes how inputs such as images are prepared
* Pooling configuration, which describes how multiple features are combined into one embedding
* Normalization metadata, which describes how embedding magnitudes are adjusted

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

* `./cache/metadata`
* `./cache/index`
* `./cache/stats`
* `./cache/count`
* `./cache/search`
* `./cache/eda`
* `./cache/atlas`

Embedding Atlas caches are stored under `./cache/atlas`, and Parquet files containing dimensionality-reduced coordinates are stored in `./cache/atlas/datasets`.

The total EDA report size is controlled by `EDA_CACHE_MAX_BYTES`, while the total Embedding Atlas cache size is controlled by `ATLAS_CACHE_MAX_BYTES`.
When either limit is exceeded, the oldest files in the corresponding cache are removed first.

A JSON cache is written to a temporary file first and replaces the existing valid cache only after the write has completed.
As a result, an interrupted write does not overwrite a valid cache with an incomplete file.

Caches that use fingerprints are invalidated based on the source file path, size, and modification time.
Here, invalidation means that an old cache entry is not reused when the source file is considered to have changed.

</details>

## Developer Information

For details about the internal architecture, the roles of the main modules, development startup commands, and implementation-specific considerations, see [IMPLEMENTATION_NOTES.md](docs/IMPLEMENTATION_NOTES.md).

## Contributing

Please use GitHub Issues to report bugs or propose new features.

After changing the code, run pre-commit before committing.
pre-commit is a mechanism that runs code formatting and checks together before a commit is created.

```bash
# Run automatic formatting, linting, and type checking on all files
uv run pre-commit run --all-files
```

To run pre-commit without installing it in the project environment, you can also use:

```bash
uvx pre-commit run --all-files
```

These commands primarily run the following checks:

* `uv run ruff format` or `uvx ruff format`: Automatically formats the code.
* `uv run ruff check` or `uvx ruff check`: Checks the code for style and lint errors.
* `uv run ty check` or `uvx ty check`: Checks for type inconsistencies.

Ruff applies Google-style docstrings based on PEP 257 to both application and test code.
A docstring is a string that explains the purpose and usage of a Python function, class, or similar object.

For public APIs, document constraints, exceptions, side effects, and ownership semantics that are not clear from types and names alone.
For private implementation details, do not add documentation that merely restates what the code does.

Resolve all reported errors before committing.

## Acknowledgments

* [Dataset Viewer (Hugging Face)](https://github.com/huggingface/dataset-viewer): Used as a reference for UI and feature design.
* [YData Profiling](https://github.com/ydataai/ydata-profiling): Used to generate EDA reports.
* [Embedding Atlas](https://github.com/apple/embedding-atlas): Used for interactive embedding visualization.

## License

This repository is released under the MIT License.
