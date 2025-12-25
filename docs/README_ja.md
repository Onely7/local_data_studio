<div align="center">

# Local Data Studio

**GUI Application for Local Dataset Viewing and Analysis**

[English](../README.md) | 日本語
</div>

Local Data Studio は、Huggingface Datasets の [Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio) を参考にして作成された JSONL/JSON/CSV/TSV/Parquet をローカルで閲覧・分析するための Web Viewer です。  
高速プレビュー、(LLM による SQL 補助付き) DuckDB SQL 実行、簡易統計、EDA レポート生成などを提供します。

<div align="center">
<img src="../images/local_data_studio_01.png" alt="local data studio 01" width=90%>
</div>

## 主な特徴

- 大規模データに対応した高速プレビューとページング
- DuckDB SQL コンソール（読み取り専用）
- EDA レポート生成（`./cache` にキャッシュ）
- Row Inspector（コピー、削除、ハイライト）
- 画像拡大・複数画像のナビゲーション
- ドラッグ&ドロップでアップロード可能
- セッション内の非表示/削除

## 環境構築

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

   # Delete Permission
   ALLOW_DELETE_DATA=false
   ```

   環境変数の説明:
   - `DATA_FILE`: 単一ファイルを直接指定します。指定した場合は `DATA_DIR` より優先されます。
   - `DATA_DIR`: データセットの探索対象ディレクトリです（DATA_FILE を使わない場合は必須）。
   - `OPENAI_API_KEY`: LLM による SQL 生成を有効化するための API Key です。
   - `OPENAI_BASE_URL`: OpenAI 互換 API のエンドポイントです。
   - `OPENAI_MODEL`: 使用する OpenAI モデル名です。
   - `EDA_ROW_LIMIT`: EDA レポート生成時に読み込む最大行数です。
   - `EDA_FONT_FAMILY`: EDA レポートで使用するフォント名です。(任意)
   - `EDA_FONT_PATH`: フォントファイルへのパスです（指定すると優先されます）。(任意)
   - `EDA_PROFILE_MODE`: `minimal` または `maximal` を指定できます。`minimal` は軽量なレポート、`maximal` は詳細な統計を含む代わりに時間がかかります。
   - `EDA_CELL_MAX_CHARS`: EDA で文字列が長い場合の最大表示文字数です。超過分は `... (truncated)` として省略されます。
   - `EDA_NESTED_POLICY`: ネスト型（list/struct/object/binary など）の扱い方です。`stringify` は文字列化して残し、`drop` は該当列を除外します。
   - `ALLOW_DELETE_DATA`: `false` の場合は実ファイル削除を無効にします（セッション内非表示は可）。

## 実行方法

```bash
uv run uvicorn app:app --reload
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
   左側の DATASETS リストから閲覧対象を選択します。検索ボックスで絞り込みも可能です。

2. **プレビュー / 検索 / ページング**  
   上部の Search でデータ検索、Rows で表示件数、Prev/Next でページ移動ができます。

3. **SQL コンソール**  
   DuckDB SQL で `data` テーブルに対してクエリを実行できます。  
   また、LLM を用いた自然言語による指示から SQL の変換をサポートしています。

4. **EDA レポート**  
   Run EDA を実行するとレポートが生成され、キャッシュされます。  
   レポートは {ファイル名, サンプル数, `EDA_PROFILE_MODE`} に基づいて `./cache` にキャッシュされます。  
   `EDA_ROW_LIMIT` と UI 側の設定でサンプル数を調整できます。

5. **Row Inspector / 画像拡大**  
   行をクリックすると詳細パネルで展開されます。画像列はクリックで拡大表示できます。  
   <img src="../images/local_data_studio_02.png" alt="local data studio 01" width=60%>

## 注意点

- サポートしているデータフォーマット: `.jsonl`, `.json`, `.csv`, `.tsv`, `.parquet`.
- 大規模データでは検索・EDA の実行に時間がかかることがあります。
- `Delete from file` は実ファイルを書き換えるため、必要に応じてバックアップを推奨します。
- `ALLOW_DELETE_DATA=false` の場合は、セッション内の非表示のみ可能です。（実ファイルは書き換わらない）

## Contribution

- バグ報告・機能提案は Issue からお願いします。
- コードの品質管理には pre-commit を使用しており、`uv run pre-commit run --all-files` (あるいは `uvx pre-commit run --all-files`) を実行することで、以下のコマンド実行に相当するフォーマット / Lint / 型チェックが実施されます:
  - `uv run ruff format` (あるいは `uvx ruff format`)
  - `uv run ruff check` (あるいは `uvx ruff check`)
  - `uv run pyrefly check` (あるいは `uvx pyrefly check`)
- 上で指摘された全てのエラーを解消した後に、コミットするようにしてください。

## 謝辞

- [Dataset viewer (Huggingface)](https://github.com/huggingface/dataset-viewer): UI/機能設計の参考にしました。
- [Zarque-profiling](https://github.com/crescendo-medix/zarque-profiling): EDA レポート生成に利用しています。

## ライセンス

本リポジトリは MIT License の下で公開されています。
