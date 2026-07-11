<div align="center">

# Local Data Studio

**GUI Application for Local Dataset Viewing and Analysis**

[English](../README.md) | 日本語
</div>

Local Data Studio は、Huggingface Datasets の [Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio) を参考にして作成された JSONL/JSON/CSV/TSV/Parquet をローカルで閲覧・分析するための Web Viewer です。  
高速プレビュー、(LLM による SQL 補助付き) DuckDB SQL 実行、簡易統計、EDA レポート生成、Embedding Atlas による可視化などを提供します。

<div align="center">
<img src="../images/local_data_studio_01.png" alt="local data studio 01" width=90%>
</div>

## 主な特徴

- 大規模データに対応した bounded preview とカーソル形式ページング
- タイムアウト・メモリ制限・大規模スキャン警告を備えた DuckDB SQL コンソール（読み取り専用）
- データセット全体または SQL クエリ結果を対象にした EDA レポート生成（`./cache` にキャッシュ）
- 選択したテキスト/画像カラム、または SQL クエリ結果を対象にした Embedding Atlas 可視化
- Row Inspector（コピー、削除、ハイライト）
- URL、ローカルパス、`{bytes, path}` 形式の画像辞書からの画像レンダリング
- 画像拡大・行内の複数画像ナビゲーション
- ドラッグ&ドロップでアップロード可能
- セッション内の非表示/削除

## インストール

### PyPI からインストール

パッケージ公開後は pip でインストールできます。

Local Data Studio は Python 3.11、3.12、3.13 に対応しています。

```bash
python -m pip install local-data-studio
```

以下のどちらの entrypoint でも起動できます。

```bash
local-data-studio --data-dir /local/data/path
python -m local_data_studio --data-dir /local/data/path
```

単一ファイルを開く場合は `--data-dir` の代わりに `--data-file` を指定します。デフォルトでは `.env`, `data`, `cache`, `models/embedder` は現在の作業ディレクトリ配下から解決されます。再現性のある起動には `--workspace-dir` または `--config` を使い、個別のパスは `--data-dir`, `--data-file`, `--cache-dir`, `--models-dir`, `--env-file`, `--file-serve-roots` で上書きできます。

設定ファイルの例:

```toml
[paths]
workspace_dir = "/Users/me/local-data-studio"
env_file = ".env"
data_dir = "/Users/me/datasets"
cache_dir = "/Users/me/.cache/local-data-studio"
models_dir = "/Users/me/models/embedder"
file_serve_roots = ["/Users/me/datasets", "/Users/me/images"]

[server]
host = "127.0.0.1"
port = 8000
reload = false
```

以下のように起動できます。

```bash
local-data-studio --config /path/to/local_data_studio.toml
```

パス設定の優先順位は、CLI option、OS environment variable、config file、`.env`、workspace default、current-working-directory default の順です。

### ソースからセットアップ

1. **リポジトリをクローンまたはダウンロード**  

   ```bash
   git clone git@github.com:Onely7/local_data_studio.git
   cd local_data_studio
   ```

2. **必要なライブラリをインストール**  

   ```bash
   uv sync
   ```

3. **環境変数の設定**  
   `.env` を作成または編集し、環境変数を指定します。

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
   EDA_PROFILE_MODE=minimal
   EDA_CELL_MAX_CHARS=5000
   EDA_NESTED_POLICY=stringify

   # Embedding Atlas Settings
   EMBEDDER_MODELS_DIR=models/embedder
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

   環境変数の説明:
   - `DATA_FILE`: 単一ファイルを直接指定します。指定した場合は `DATA_DIR` より優先されます。
   - `DATA_DIR`: データセットの探索対象ディレクトリです（DATA_FILE を使わない場合は必須）。
   - `FILE_SERVE_ROOTS`: ローカル画像プレビューとして配信を許可するディレクトリをカンマ区切りで指定します。
   - `VIS_EXCLUDE_DIRS`: `DATA_DIR` 配下でデータセット探索から除外するディレクトリをカンマ区切りで指定します。
   - `OPENAI_API_KEY`: LLM による SQL 生成を有効化するための API Key です。
   - `OPENAI_BASE_URL`: OpenAI 互換 API のエンドポイントです。
   - `OPENAI_MODEL`: 使用する OpenAI モデル名です。
   - `EDA_ROW_LIMIT`: EDA レポート生成時に読み込む最大行数です。
   - `EDA_PROFILE_MODE`: `minimal` または `maximal` を指定できます。`minimal` は軽量なレポート、`maximal` は詳細な統計を含む代わりに時間がかかります。
   - `EDA_CELL_MAX_CHARS`: EDA で文字列が長い場合の最大表示文字数です。超過分は `... (truncated)` として省略されます。
   - `EDA_NESTED_POLICY`: ネスト型（list/struct/object/binary など）の扱い方です。`stringify` は文字列化して残し、`drop` は該当列を除外します。
   - `EMBEDDER_MODELS_DIR`: ローカル HuggingFace encoder model ディレクトリを含むディレクトリです。デフォルトは workspace/current directory 配下の `models/embedder` です。
   - `ATLAS_HOST` / `ATLAS_PORT`: ローカル Embedding Atlas ページの host と開始 port です。port が使用中の場合、`embedding-atlas` が別 port を選ぶことがあります。
   - `ATLAS_SAMPLE`: Embedding Atlas に渡す任意のランダムサンプル数です。未設定または `0` の場合は全行を対象にします。
   - `ATLAS_BATCH_SIZE`: 任意の embedding batch size です。未設定または `0` の場合は Embedding Atlas のデフォルトを使用します。
   - `ATLAS_CACHE_MAX_BYTES`: `./cache/atlas` に保存する Embedding Atlas cache 全体の最大容量です。超過時は古い cache file から削除されます。
   - `ATLAS_TEXT_MAX_CHARS`: Atlas の embedding input と cached Atlas parquet output に残すテキストセルの最大文字数です。`0` で省略を無効化します。
   - `ATLAS_EMBEDDING_DTYPE`: projection 前の embedding 配列精度です。`float32` または `float16` を指定できます。
   - `ATLAS_PROJECTION_MODE`: projection 方式です。`full` は全 embedding に対して UMAP を実行し、`anchor_transform` は代表サンプルで UMAP を fit して残りを同じ空間へ transform します。
   - `ATLAS_ANCHOR_SAMPLE`: `ATLAS_PROJECTION_MODE=anchor_transform` の場合に UMAP fit に使う行数です。
   - `ATLAS_TEXT_EMBEDDER` / `ATLAS_IMAGE_EMBEDDER`: 任意の Embedding Atlas embedder backend 名です。
   - `ATLAS_TRUST_REMOTE_CODE`: `true` の場合、Embedding Atlas に `--trust-remote-code` を渡します。
   - `ALLOW_DELETE_DATA`: `false` の場合は実ファイル削除を無効にします（セッション内非表示は可）。

## 実行方法

```bash
uv run local-data-studio --reload
```

ASGI app を直接起動して開発する場合は以下も使えます。

```bash
uv run uvicorn local_data_studio.app:app --reload
```

実行後、ターミナルに以下のようなメッセージが表示されます。

```
INFO:     Will watch for changes in these directories: ['local/data_viewer']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [00000] using StatReload
INFO:     Started server process [00000]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

これで Local Data Studio サーバーが立ち上がりました。  
<http://127.0.0.1:8000> にアクセスすることで、Local Data Studio の GUI が表示されます。

## 使い方

1. **DATASETS からファイルを選択**  
   左側の DATASETS リストから閲覧対象を選択します。検索ボックスで絞り込みも可能です。長いファイル名はリスト内で省略表示され、ファイルサイズは最大 3 有効桁で `Bytes`, `kB`, `MB`, `GB`, `TB` の適切な単位に変換して表示されます。  
   <img src="../images/local_data_studio_02.png" alt="local data studio 02" width=45%>

2. **プレビュー / 検索 / ページング**  
   上部の Search でデータ検索、Rows で表示件数、Prev/Next でページ移動ができます。  
   <img src="../images/local_data_studio_03.png" alt="local data studio 03" width=45%>

3. **SQL コンソール**  
   DuckDB SQL で `data` テーブルに対してクエリを実行できます。  
   また、LLM を用いた自然言語による指示から SQL の変換をサポートしています。SQL は単一の `SELECT`/CTE に制限され、タイムアウト、メモリ制限、大規模データセット向けのスキャンリスク検知が適用されます。  
   <img src="../images/local_data_studio_04.png" alt="local data studio 04" width=45%>

4. **EDA レポート**  
   Run EDA を実行するとデータセットのサンプルを対象にしたレポートが生成され、キャッシュされます。**Run EDA on Query Results** を使うと、SQL Console の現在のクエリ結果を対象にした EDA レポートを生成できます。  
   データセット全体のレポートは {ファイル fingerprint, サンプル数, `EDA_PROFILE_MODE`} に基づいてキャッシュされます。クエリ結果のレポートは {ファイル fingerprint, SQL, サンプル数, `EDA_PROFILE_MODE`} に基づいて別キャッシュされます。  
   `EDA_ROW_LIMIT` と UI 側の設定でサンプル数を調整できます。  
   <img src="../images/local_data_studio_05.png" alt="local data studio 05" width=45%> <img src="../images/local_data_studio_06.png" alt="local data studio 06" width=45%>

5. **Embedding 可視化**  
   HuggingFace 形式のローカル encoder model ディレクトリを `models/embedder` または `--models-dir` / `EMBEDDER_MODELS_DIR` で指定したディレクトリ配下に配置します（例: `models/embedder/google/siglip2-base-patch16-224`, `models/embedder/Qwen/Qwen3-Embedding-0.6B`, `models/embedder/Qwen/Qwen3-VL-Embedding-2B`）。`config.json`, `modules.json`, `tokenizer_config.json`, `preprocessor_config.json` などの model marker file を含むディレクトリが Model プルダウンに表示されます。
   **Visualize Embedding** でテキストまたは画像カラム、モデル、利用可能な backend を選択し、**Run Atlas** を実行するとローカルの Embedding Atlas ページが起動します。**Run Atlas on Query Results** を使うと、SQL Console の現在のクエリ結果を対象に可視化できます。モデル探索では weight をロードせず設定だけを解析し、利用できない backend も選択不可の状態で表示します。両 backend を利用できる場合は Sentence Transformers が既定で選択されます。
   Sentence Transformers を選択すると任意の Prompt 入力欄が表示されます。空欄の場合はモデルに保存された default prompt を使用します。placeholder を含まないテキストは選択列の各値へ prefix として付加されます。`{title}` や `{body}` のような正確な placeholder は、同じ dataset 行または SQL 結果行の値へ置換されます。`{{` / `}}` は通常の波括弧として扱われます。存在しないカラム、壊れた波括弧、conversion、format specifier はモデルをロードする前に拒否されます。
   処理はバックグラウンドジョブとして進捗表示され、準備が完了すると **Open Atlas** リンクが表示されます。  
   <img src="../images/local_data_studio_07.png" alt="local data studio 07" width=45%> <img src="../images/local_data_studio_08.png" alt="local data studio 08" width=45%>

6. **Row Inspector / 画像拡大**  
   行をクリックすると詳細パネルで展開されます。長い値はデフォルトで省略表示され、Raw で完全表示に切り替えられます。画像列はクリックで拡大表示できます。画像候補は画像 URL、相対/絶対画像パス、`{ "bytes": ..., "path": ... }` のような辞書から検出され、bytes を優先して表示し、失敗した場合は path を fallback として使用します。  
   <img src="../images/local_data_studio_09.png" alt="local data studio 09" width=45%> <img src="../images/local_data_studio_10.png" alt="local data studio 10" width=45%>

## 注意点

- サポートしているデータフォーマット: `.jsonl`, `.json`, `.csv`, `.tsv`, `.parquet`.
- 大規模データでは検索・EDA の実行に時間がかかることがあります。
- 非常に大きなデータセットでは、対応形式のプレビューに大きな `OFFSET` ではなくカーソル形式の `page_token` を使用します。行数カウント、全体検索、サンプル統計、EDA は進捗確認とキャンセルが可能なバックグラウンドジョブとして実行されます。
- Embedding Atlas ジョブは選択したローカル encoder model で embedding/projection 計算を行うため、時間がかかる場合があります。投影済み parquet input は `./cache/atlas/datasets` に保存され、dataset fingerprint、SQL、column、model、backend、prompt template、capability fingerprint、projection 設定が一致する場合だけ再利用されます。画像表示カラムは元の URL/path/`{bytes, path}` 形式を保持し、encoder 入力変換には hidden embedding input column だけを使います。大規模データで素早く試す場合は `ATLAS_SAMPLE` を指定し、長文テキスト列と展開後 prompt は `ATLAS_TEXT_MAX_CHARS` で上限を調整し、embedding メモリは `ATLAS_EMBEDDING_DTYPE=float16` で削減できます。`ATLAS_PROJECTION_MODE=anchor_transform` を使うと代表サンプルで UMAP を fit し、残りを transform します。容量上限は `ATLAS_CACHE_MAX_BYTES` で調整してください。
- backend 対応状況はモデル名ではなく、ローカルの `modules.json`、`config.json`、processor、pooling、normalization metadata を上限付きで解析して判定します。Sentence Transformers は `native`, `generic_fallback`, `metadata_only`, `unsupported`, `unknown`、Transformers は `direct`, `remote_code`, `backbone_only`, `unsupported`, `unknown` の状態を返し、実行可能な adapter を確認できた backend だけを選択できます。`remote_code` は `ATLAS_TRUST_REMOTE_CODE=true` で repository code の実行を明示許可した場合だけ利用できます。Qwen3-VL-Embedding のような multimodal last-token pooling model も、モデルリポジトリ内の Python を import せず、組み込み Transformer/Pooling/Normalize 構成から Transformers adapter を解決します。
- `models/embedder` または設定した models directory 配下のローカル encoder model 実体は配布物に含めません。リポジトリにはディレクトリ用の placeholder のみを含め、モデルファイルは各環境で配置してください。
- キャッシュは `./cache/metadata`, `./cache/index`, `./cache/stats`, `./cache/count`, `./cache/search` および EDA レポートファイルに分離され、該当するものはファイルパス・サイズ・更新時刻に基づいて無効化されます。
- `Run EDA on Query Results` では、`rn` や `__rowid` のような補助カラムはレポートから除外されます。
- TB 級の `.json` 配列は推奨しません。高速な閲覧には JSONL または Parquet を推奨します。
- `Delete from file` は実ファイルを書き換えるため、必要に応じてバックアップを推奨します。
- `ALLOW_DELETE_DATA=false` の場合は、セッション内の非表示のみ可能です。（実ファイルは書き換わらない）

## 実装メモ

- application package は `src/local_data_studio` 配下にあります。静的 UI asset は `src/local_data_studio/static` として package に含め、実行時の `.env`, `data`, `cache`, `models/embedder` は選択された workspace または現在の作業ディレクトリ配下をデフォルトにしています。CLI option、OS environment variable、config file、`.env`、workspace default の順で上書きされます。
- `src/local_data_studio/app.py` は application assembly だけを行う小さな entrypoint です。Request model と API route は `src/local_data_studio/server/api` 配下で dataset access、analysis、background job、mutation、共通 service、static mount に分割されています。Filesystem、DuckDB、EDA、job に関する blocking route は FastAPI の threadpool で実行し、streaming upload だけを async のまま維持しています。
- `src/local_data_studio/server/readers.py` は互換 facade として維持し、形式別実装を `src/local_data_studio/server/dataset_readers` に分割しています。JSONL metadata 推論は行数と byte 数の固定上限で停止し、JSONL/CSV/TSV preview は fingerprint 付き sparse line index と byte/page token を使います。完成済み index は再利用し、checkpoint は batch transaction で保存します。CSV/TSV の schema、preview、search、Raw は長大 field 対応 parser を共有します。Parquet schema は footer metadata のみを読み、preview と Raw は bounded record batch、offset 互換処理は行単位 scan ではなく row-group metadata を使います。
- `src/local_data_studio/server/stats.py` は互換 facade として維持し、`src/local_data_studio/server/column_stats` で値の推論、カラム単位の集計、DuckDB orchestration を分離しています。Sample row は固定サイズ batch で取得し、全 row matrix と column copy を同時に保持せず、column accumulator へ直接渡します。
- SQL 実行は `src/local_data_studio/server/sql.py` に集約され、読み取り専用 SQL の検証、DuckDB リソース制限、バックグラウンドジョブの協調キャンセルを扱います。
- EDA レポートの orchestration は `src/local_data_studio/server/eda_reports.py`、profiling 設定と DataFrame sanitization は `src/local_data_studio/server/eda.py` に分離されています。
- `src/local_data_studio/server/atlas.py` は互換 facade として維持し、`src/local_data_studio/server/atlas_components` で contract、capability-driven embedding adapter、安全な prompt template、画像変換、projection、dataset cache、subprocess 制御、orchestration を分離しています。`server/embedder_capabilities.py` は上限付きの metadata-only model inspection と関連設定の fingerprint 作成を担当します。Encoder は Atlas job ごとに 1 回だけ生成して anchor/transform batch 間で再利用します。Anchor-transform は input column 全体を Python list に変換せず、anchor と現在の transform batch だけを取得します。表示値の sanitization と projection column の追加は 1 つの owned DataFrame copy 上で行い、同じ fingerprint・query・column・model・backend・prompt・設定に対する並行 cache miss は 1 回の cache 生成を共有します。
- Atlas の UMAP projection は cache artifact の再現性のため固定 seed を使い、UMAP の seeded execution mode に合わせて `n_jobs=1` を明示することで thread override warning を出さないようにしています。
- macOS では child-side fork による `SIGSEGV (-11)` を避けるため、Atlas subprocess 起動を Python の `posix_spawn` path に乗る形に固定しています。Atlas command は絶対パスを使い、`Popen` に `cwd` を渡さず、`close_fds=False` を維持してください。
- バックグラウンドジョブは `src/local_data_studio/server/jobs.py` で管理され、`/api/jobs/*` 経由で進捗、キャンセル、結果、エラー状態を確認できます。

## Contribution

- バグ報告・機能提案は Issue からお願いします。
- コードの品質管理には pre-commit を使用しており、`uv run pre-commit run --all-files` (あるいは `uvx pre-commit run --all-files`) を実行することで、以下のコマンド実行に相当するフォーマット / Lint / 型チェックが実施されます:
  - `uv run ruff format` (あるいは `uvx ruff format`)
  - `uv run ruff check` (あるいは `uvx ruff check`)
  - `uv run ty check` (あるいは `uvx ty check`)
- Ruff は application code と test code の両方に、PEP 257 を基本とした Google convention の docstring を適用します。公開 API は型と名前だけでは明確にならない制約、例外、副作用、ownership を記載し、private な実装詳細には処理内容を言い換えるだけの説明を追加しません。
- 上で指摘された全てのエラーを解消した後に、コミットするようにしてください。

## 謝辞

- [Dataset viewer (Huggingface)](https://github.com/huggingface/dataset-viewer): UI/機能設計の参考にしました。
- [YData Profiling](https://github.com/ydataai/ydata-profiling): EDA レポート生成に利用しています。
- [Embedding Atlas](https://github.com/apple/embedding-atlas): Embedding のインタラクティブな可視化に利用しています。

## ライセンス

本リポジトリは MIT License の下で公開されています。
