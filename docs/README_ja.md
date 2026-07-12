<div align="center">

# Local Data Studio

**ローカルデータセットを閲覧・分析するための GUI アプリケーション**

[English](../README.md) | 日本語
</div>

Local Data Studio は、Hugging Face Datasets の [Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio) を参考に開発された、ローカル環境向けの Web ビューアーです。
JSONL、JSON、CSV、TSV、Parquet 形式のデータをブラウザーで閲覧・検索・分析できます。

主な機能として、高速なプレビュー、DuckDB を使った SQL 実行、探索的データ分析（Exploratory Data Analysis、以下 EDA）レポートの生成、Embedding Atlas を使った埋め込みの可視化などを提供します。
SQL コンソールでは、LLM に自然言語で指示して SQL を生成することもできます。

<div align="center">
<img src="../images/local_data_studio_01.png" alt="Local Data Studio のメイン画面" width="90%">
</div>

## 主な特徴

- 大規模なデータでも、読み込む量を制限しながらプレビュー可能
- 前後のページへ効率よく移動できるカーソル形式のページング
- DuckDB SQL を使った読み取り専用の検索・集計
- SQL 実行時のタイムアウト、メモリ制限、大規模スキャン警告
- データセット全体または SQL クエリ結果を対象とした EDA レポート生成
- テキスト列（カラム）・画像列または SQL クエリ結果を対象とした Embedding Atlas 可視化
- 行の内容を詳しく確認できる **Row Inspector**
- URL、ローカルパス、`{bytes, path}` 形式の辞書に保存された画像の表示
- 画像の拡大表示と、同じ行に含まれる複数画像の切り替え
- ドラッグ＆ドロップによるファイルのアップロード
- セッション内での行の非表示と、必要に応じた実ファイルからの削除

## 対応環境とデータ形式

- Python 3.11、3.12、3.13
- 対応形式：`.jsonl`、`.json`、`.csv`、`.tsv`、`.parquet`

## インストール

利用方法に応じて、次のどちらかを選んでください。

- 通常利用する場合: **PyPI からインストール**
- 開発やコードの変更を行う場合: **ソースコードからセットアップ**

### PyPI からインストール

パッケージ公開後は、次のコマンドでインストールできます。

```bash
python -m pip install local-data-studio
```

インストール後は、次のどちらのコマンドでも起動できます。

```bash
# 指定したディレクトリ内のデータファイルを一覧表示する
local-data-studio --data-dir /local/data/path

# 上と同じ処理を Python モジュールとして実行する
python -m local_data_studio --data-dir /local/data/path
```

`/local/data/path` は、実際に閲覧したいデータが保存されているディレクトリへ置き換えてください。

単一のファイルだけを開く場合は、`--data-dir` の代わりに `--data-file` を指定します。

```bash
local-data-studio --data-file /local/data/example.parquet
```

起動後、ブラウザーで <http://127.0.0.1:8000> を開いてください。

### ソースコードからセットアップ

ソースコードから実行する場合は、Python 3.11〜3.13、Git、uv が必要です。

1. **リポジトリを取得する**

   ```bash
   # GitHub からリポジトリをダウンロードする
   git clone https://github.com/Onely7/local_data_studio.git

   # ダウンロードしたディレクトリへ移動する
   cd local_data_studio
   ```

2. **必要なライブラリをインストールする**

   ```bash
   # プロジェクトの設定に基づいて実行環境を準備する
   uv sync
   ```

3. **主設定ファイルを作成する**

   ```bash
   # テンプレートをコピーし、プロジェクトの設定はこのファイルへまとめる
   cp local_data_studio.example.toml local_data_studio.toml
   ```

4. **`local_data_studio.toml` を編集する**

   通常の Local Data Studio の設定は、このファイルに記述します。閲覧するデータのディレクトリは `[paths].data_dir` に指定します。単一ファイルを開く場合は、`[paths].data_file` を使用します。サーバー、EDA、Atlas、削除許可、SQL モデルの設定も、`[server]`、`[settings]`、`[llm]` にまとめます。

   ```toml
   [paths]
   data_dir = "/local/data/path"

   [settings]
   eda_row_limit = 50000
   ```

5. **必要な場合だけ `.env` を作成する**

   `.env` は、LLM プロバイダーの API キーや例外的なローカル上書きのために使います。通常のアプリケーション設定を書く場所ではありません。これらの値が必要なときだけ、次のコマンドで作成してください。

   ```bash
   cp .env.example .env
   ```

   たとえば、LLM モデルプロファイルの `api_key_env` が参照する API キーを記述します。

   ```dotenv
   OPENAI_API_KEY=your_openai_api_key
   ```

   `.env` は Git の管理対象外です。OS の環境変数やコマンドラインオプションを使うと、1 回の起動に限って TOML の値を上書きすることもできます。

6. **Local Data Studio を起動する**

   ```bash
   # プロジェクトの設定を指定し、開発用の自動再読み込みを有効にして起動する
   uv run local-data-studio --config ./local_data_studio.toml --reload
   ```

7. **ブラウザーで画面を開く**

   ターミナルに次のようなメッセージが表示されたら、起動は完了です。

   ```text
   INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
   INFO:     Application startup complete.
   ```

   ブラウザーで <http://127.0.0.1:8000> を開くと、Local Data Studio の画面が表示されます。

   サーバーを停止するには、起動したターミナルで `Ctrl+C` を押してください。

## パスと設定ファイル

### 推奨する設定場所

プロジェクトごとに、データ、キャッシュ、ローカルモデルを置くワークスペース内へ `local_data_studio.toml` を 1 つ置く運用を推奨します。このファイルが、パス、サーバー、EDA、Atlas、削除許可、SQL モデルプロファイルの通常の設定場所です。`--config ./local_data_studio.toml` を付けて起動すると、使用する設定ファイルが明確になり、相対パスもそのワークスペースを基準に解決されます。

`.env` は API キーと、必要な場合だけのマシン固有の上書きに使います。必須の第 2 設定ファイルではないため、認証情報や上書きが不要なら作成する必要はありません。

### デフォルトのパス

特に指定しない場合、次のファイルやディレクトリは、コマンドを実行したディレクトリを基準に検索または作成されます。

- `.env`
- `data`
- `cache`
- `models/embedder`

コマンドを実行したディレクトリは、「現在の作業ディレクトリ」または「カレントディレクトリ」と呼ばれます。
毎回同じ場所を基準に起動したい場合は、`--workspace-dir` または `--config` を指定してください。

個別のパスは、次のコマンドラインオプションで上書きできます。

- `--data-dir`
- `--data-file`
- `--cache-dir`
- `--models-dir`
- `--env-file`
- `--file-serve-roots`

### TOML 設定ファイル

通常の設定管理には `local_data_studio.toml` を使用します。TOML は、設定項目とその値をテキストで記述するためのファイル形式です。

リポジトリには、パス、サーバー設定、認証情報を含まない任意の LLM モデルプロファイルを記載した [local_data_studio.example.toml](../local_data_studio.example.toml) を用意しています。
このファイルを `local_data_studio.toml` へコピーしてから、使用するパスとモデルプロファイルを編集してください。

```bash
cp local_data_studio.example.toml local_data_studio.toml
```

API キーは `.env` またはシェルの環境変数に保存します。各モデルプロファイルの `api_key_env` には、参照する認証情報の環境変数名を指定します。通常のパスや機能設定は TOML にまとめ、`.env` は認証情報または必要時のローカル上書きだけに使います。
`model` には、LiteLLM のモデル名を1件の文字列として指定するほか、同じプロバイダーのモデル名をリストとして指定できます。リスト内のモデルは、同じプロファイルの認証情報、接続先、タイムアウト、`provider_options` を共有し、SQL Console では個別に選択できます。`default_model` にプロファイルIDを指定した場合は、リストの先頭のモデルが初期選択されます。

`[settings]` セクションには、EDA、Embedding Atlas、元ファイルの削除許可を設定します。環境変数名を小文字の snake_case へ置き換えて指定します。たとえば `EDA_ROW_LIMIT` は `eda_row_limit`、`ALLOW_DELETE_DATA` は `allow_delete_data` です。テンプレートには指定できる設定をすべて記載しています。キーを省略した場合は `.env` またはアプリケーションの既定値が使われます。

設定ファイルを指定して起動する例を次に示します。

```bash
local-data-studio --config /path/to/local_data_studio.toml
```

同じ設定項目が複数の場所で指定されている場合は、次の順番で優先されます。
上にあるものほど優先度が高くなります。

1. コマンドラインオプション
2. OS の環境変数
3. TOML 設定ファイル
4. `.env`
5. ワークスペースを基準としたデフォルト値
6. 現在の作業ディレクトリを基準としたデフォルト値

### 環境変数と TOML 設定の説明

以下の各環境変数は、TOML の `[settings]` セクションに小文字の snake_case 形式でも指定できます。コマンドラインオプションと OS の環境変数は TOML より優先され、TOML は `.env` より優先されます。

#### データとパス

- `DATA_FILE`: 単一のデータファイルを直接指定します。指定した場合は `DATA_DIR` より優先されます。
- `DATA_DIR`: データセットを検索するディレクトリです。`DATA_FILE` を使用しない場合は必須です。
- `FILE_SERVE_ROOTS`: ローカル画像の配信を許可するディレクトリを、カンマ区切りで指定します。
- `VIS_EXCLUDE_DIRS`: `DATA_DIR` の下にあるディレクトリのうち、データセットの検索対象から除外するものをカンマ区切りで指定します。
- `VIS_EXCLUDE_FILES`: `DATA_DIR` の下にあるファイルのうち、データセットの検索対象から除外するものをカンマ区切りで指定します。相対パスは `DATA_DIR` を基準に解決され、絶対パスも指定できます。

#### LLM の認証情報

- `OPENAI_API_KEY`、`ANTHROPIC_API_KEY`、`GEMINI_API_KEY`: LLM モデルプロファイルの `api_key_env` から参照する認証情報の例です。これらの値がブラウザーへ送信されることはありません。

#### EDA

- `EDA_ROW_LIMIT`: データセット全体または SQL クエリ結果から EDA レポートへ読み込む最大行数です。UI からは変更できません。`1` 以上の整数を指定でき、`-1` を指定すると行数を制限しません。
- `EDA_CELL_MAX_CHARS`: EDA で扱う文字列セルの最大文字数です。上限を超えた部分は `... (truncated)` として省略されます。
- `EDA_NESTED_POLICY`: リスト、構造体、オブジェクト、バイナリなどのネスト型をどのように扱うかを指定します。`stringify` は文字列へ変換して残し、`drop` は対象の列を除外します。
- `EDA_CACHE_MAX_BYTES`: `./cache/eda` に保存する EDA レポート全体の最大容量です。既定値は 1 GiB で、上限を超えると古いレポートから削除されます。

#### Embedding Atlas

- `EMBEDDER_MODELS_DIR`: ローカルの Hugging Face エンコーダーモデルを保存する親ディレクトリです。既定では、ワークスペースまたは現在の作業ディレクトリの `models/embedder` を使用します。
- `ATLAS_HOST`、`ATLAS_PORT`: ローカルの Embedding Atlas ページを起動するホスト名と、最初に調べるポートです。Local Data Studio が空きポートを選択し、ブラウザーが HTTP 接続を禁止しているポートを除外したうえで、Embedding Atlas 側の独立した自動ポート探索を無効化します。
- `ATLAS_SAMPLE`: 埋め込み計算と次元削減を行い、Atlas 用キャッシュ Parquet に保存する行数の上限です。SQL クエリを適用した後、乱数の初期値（シード）を 42 に固定して、同じ入力からは毎回同じ行が選ばれるように抽出します。未設定または `0` の場合は、選択されたすべての行を使用します。負の値は指定できません。
- `ATLAS_BATCH_SIZE`: 埋め込み計算で一度に処理する行数（バッチサイズ）です。未設定または `0` の場合は、Embedding Atlas の既定値を使用します。
- `ATLAS_CACHE_MAX_BYTES`: `./cache/atlas` に保存する Embedding Atlas 関連キャッシュ全体の最大容量です。上限を超えると古いキャッシュファイルから削除されます。
- `ATLAS_TEXT_MAX_CHARS`: 埋め込みへの入力と、Atlas 用キャッシュ Parquet に残すテキストセルの最大文字数です。`0` を指定すると省略しません。
- `ATLAS_EMBEDDING_DTYPE`: 次元削減前の埋め込み配列に使用する数値精度です。`float32` または `float16` を指定できます。
- `ATLAS_UMAP_PROJECTION_MODE`: UMAP による次元削減の実行方式です。`full` は、抽出したすべての埋め込みをまとめて処理します。`anchor_transform` は、代表となる行で UMAP の配置を決め、残りの行を同じ 2 次元空間へ配置します。t-SNE と PCA は、抽出したすべての行をまとめて処理します。
- `ATLAS_UMAP_ANCHOR_SAMPLE`: `ATLAS_UMAP_PROJECTION_MODE=anchor_transform` の場合に、UMAP の学習へ使用する行数です。
- `ATLAS_TRUST_REMOTE_CODE`: `true` を指定すると、Local Data Studio がモデルを読み込む際に、選択したローカルエンコーダーモデルのリポジトリーコード実行を許可します。信頼できるモデル以外では `false` のままにしてください。

#### データの削除

- `ALLOW_DELETE_DATA`: `false` の場合は、元のデータファイルからの削除を禁止します。画面上で一時的に非表示にする操作は可能です。

## 使い方

### 1. データファイルを選択する

左側の **DATASETS** リストから、閲覧するファイルを選択します。
検索ボックスを使って、ファイル名を絞り込むこともできます。

長いファイル名はリスト内で省略して表示されます。
ファイルサイズは有効数字 3 桁までに整えられ、`Bytes`、`kB`、`MB`、`GB`、`TB` の適切な単位で表示されます。

<img src="../images/local_data_studio_02.png" alt="DATASETS リストからファイルを選択する画面" width="45%">

### 2. データを閲覧・検索する

画面上部の各項目を使って、表示内容を操作できます。

- **Search**: データを検索します。
- **Rows**: 1 ページに表示する行数を変更します。
- **Prev**／**Next**: 前後のページへ移動します。

<img src="../images/local_data_studio_03.png" alt="データの検索とページ移動を行う画面" width="45%">

### 3. SQL コンソールを使う

SQL コンソールでは、DuckDB SQL を使って `data` テーブルを検索・集計できます。
実行できるのは読み取り専用のクエリです。

サーバー側で設定した LiteLLM モデルプロファイルを使うと、日本語などの自然言語による指示から SQL を生成できます。
SQL コンソールでは、設定済みの OpenAI、Anthropic、Gemini、hosted vLLM、その他の LiteLLM 対応モデルを選択できます。

LLM からはプレーンテキストとして生成された SQL だけを受け取り、ツール呼び出しなどは使用しません。生成される SQL は、単一の `SELECT` 文、または `WITH` 句による共通テーブル式（CTE）を使った `SELECT` 文に制限されます。
SQL の実行時には、タイムアウト、メモリ制限、大規模なデータ読み込みのリスク検知が適用されます。

<img src="../images/local_data_studio_04.png" alt="SQL コンソールの画面" width="45%">

### 4. EDA レポートを生成する

**Run EDA** を実行すると、データセットから取得した行を対象に EDA レポートを生成します。
生成したレポートはキャッシュへ保存されるため、同じ条件で再実行した場合に再利用できます。

**Run EDA on Query Results** を使うと、SQL コンソールに表示されている現在のクエリ結果を対象にレポートを生成できます。

行数の上限は、`[settings]` の `eda_row_limit`、または環境変数／`.env` の `EDA_ROW_LIMIT` で指定します。
この値は UI から変更できません。
`1` 以上の整数を指定でき、`-1` を指定すると行数制限を解除します。

EDA パネルの **Profile mode** では、分析の詳しさを実行ごとに選択できます。
初期値は `minimal` です。

<img src="../images/local_data_studio_05.png" alt="EDA の実行画面" width="45%"> <img src="../images/local_data_studio_06.png" alt="生成された EDA レポート" width="45%">

### 5. 埋め込みを可視化する

埋め込みとは、テキストや画像の特徴を、数値の並び（ベクトル）として表したものです。
埋め込みはそのままでは画面上で確認しにくいため、UMAP、t-SNE、PCA などを使って 2 次元の座標へ変換して表示します。この処理は一般に「次元削減」と呼ばれます。
設定名や内部コードでは `projection` と表記している箇所がありますが、この README では UMAP、t-SNE、PCA をまとめて「次元削減」と説明します。

はじめに、Hugging Face 形式で保存されたローカルエンコーダーモデルを、`models/embedder` または `--models-dir`／`EMBEDDER_MODELS_DIR` で指定したディレクトリの下へ配置します。

配置例:

```text
models/embedder/google/siglip2-base-patch16-224
models/embedder/Qwen/Qwen3-Embedding-0.6B
models/embedder/Qwen/Qwen3-VL-Embedding-2B
```

`config.json`、`modules.json`、`tokenizer_config.json`、`preprocessor_config.json` など、モデルを識別するためのファイルを含むディレクトリが **Model** の選択欄に表示されます。

**Visualize Embedding** で、次の項目を選択します。

1. テキスト列または画像列
2. 使用するモデル
3. モデルの実行に使用するライブラリ（バックエンド）
4. 2 次元への次元削減手法

次元削減手法は、**UMAP**（既定）、**t-SNE**、**PCA** から選択できます。
**Run Atlas** を実行すると、ローカルの Embedding Atlas ページが起動します。
**Run Atlas on Query Results** を使うと、現在の SQL クエリ結果を対象に可視化できます。

モデルを一覧表示する段階では重みを読み込まず、設定ファイルだけを解析します。
利用できないバックエンドも一覧には表示されますが、選択はできません。
Sentence Transformers と Transformers の両方を利用できる場合は、Sentence Transformers が既定で選択されます。

Sentence Transformers を選択すると、モデルへ追加で渡す指示を入力するための **Prompt** 欄が表示されます。
空欄の場合は、モデルに保存されたデフォルトプロンプトを使用します。

- プレースホルダーを含まない文字列は、選択した列の各値の先頭へ追加されます。
- `{title}` や `{body}` のようなプレースホルダーは、同じデータ行または SQL 結果行にある対応する列の値へ置き換えられます。
- `{{` と `}}` は、通常の波括弧として扱われます。
- 存在しない列、対応していない変換指定、書式指定、正しく閉じられていない波括弧は、モデルを読み込む前にエラーとなります。

処理は、画面の操作を止めずに裏側で進むバックグラウンドジョブとして実行されます。進捗には現在の処理段階が表示され、埋め込みのバッチ処理が終わるたびに更新されます。キャンセルは、現在処理中のバッチまたは処理段階が終了した時点で反映されます。
Atlas サーバーがブラウザーから接続可能なポートで実際に待受を開始した後にだけ、**Open Atlas** リンクが表示されます。

<img src="../images/local_data_studio_07.png" alt="Embedding Atlas の設定画面" width="45%"> <img src="../images/local_data_studio_08.png" alt="Embedding Atlas の可視化画面" width="45%">

### 6. Row Inspector と画像拡大を使う

行をクリックすると、**Row Inspector** に各列の詳しい内容が表示されます。
長い値は初期状態では省略されますが、**Raw** に切り替えると省略前の値を確認できます。

画像として認識された値は、クリックすると拡大表示できます。
次の形式に対応しています。

- 画像 URL
- 相対パスまたは絶対パス
- `{ "bytes": ..., "path": ... }` 形式の辞書

`bytes` と `path` の両方がある場合は、まず `bytes` の内容を表示します。
表示できなかった場合は、`path` を代わりに使用します。

<img src="../images/local_data_studio_09.png" alt="Row Inspector の画面" width="45%"> <img src="../images/local_data_studio_10.png" alt="画像の拡大表示画面" width="45%">

## 利用上の注意

- 大規模なデータでは、検索や EDA に時間がかかることがあります。
- 行数カウント、全体検索、サンプル統計、EDA は、進捗確認とキャンセルが可能なバックグラウンドジョブとして実行されます。
- `EDA_ROW_LIMIT=-1` を指定すると、選択したすべての行を分析用のメモリへ読み込みます。データ全体が無理なくメモリへ収まる場合だけ使用してください。
- TB 級の JSON 配列ファイルは推奨しません。大規模データを高速に閲覧する場合は、JSONL または Parquet を推奨します。
- t-SNE はデータ量が増えると処理時間とメモリ使用量が急増します。大規模なデータでは、`ATLAS_SAMPLE` に現実的な上限を設定してください。
- **Delete from file** は元のデータファイルを書き換えます。必要に応じて、操作前にバックアップを作成してください。
- `ALLOW_DELETE_DATA=false` の場合、元のファイルからは削除されません。画面上のセッション内で非表示にする操作だけが可能です。
- `models/embedder` または設定したモデルディレクトリには、モデル本体を各自で配置してください。配布物にはモデルファイルを含めず、空のディレクトリを維持するためのプレースホルダーだけを含めています。

## 高度な設定と技術仕様

このセクションは、動作の詳細を確認したい利用者や管理者向けです。
通常の閲覧・分析だけを行う場合は、読み飛ばしても問題ありません。

<details>
<summary><strong>大規模データのプレビューとバックグラウンド処理</strong></summary>

非常に大きなデータセットでは、対応形式のプレビューに大きな `OFFSET` を使用せず、カーソル形式の `page_token` を使用します。
行数カウント、全体検索、サンプル統計、EDA は、進捗確認とキャンセルが可能なバックグラウンドジョブとして実行されます。

Count Rows、EDA、Atlas の実行後に表示されるフィードバックは、ほかの操作を妨げない共通のコンパクトなステータス表示に統一されています。

</details>

<details>
<summary><strong>SQL 生成用 LLM モデルプロファイル</strong></summary>

SQL 生成用のモデルプロファイルは、`local_data_studio.toml` の `[llm]` セクションから読み込みます。
モデル名には、`openai/`、`anthropic/`、`gemini/`、`hosted_vllm/` など、LiteLLM のプロバイダープレフィックスを明示してください。
非推奨の `vllm/` は使用できません。

`model` には、1件のモデル名の文字列、または同じプロバイダーに属するモデル名のリストを指定できます。リスト内の各モデルは SQL Console で個別に選択できます。1つのプロファイルに設定したモデルは、認証情報、任意の接続先、タイムアウト、`provider_options` を共有するため、異なるプロバイダーを混在させることはできません。

認証情報は、`api_key_env` が参照する環境変数へ保存します。
`provider_options` は管理者が指定する信頼済みの設定です。
`reasoning_effort`、`thinking`、トークン上限、`top_k`、`extra_body` などを指定できます。
一方で、メッセージ、認証情報、ストリーミング、ツール、マルチモーダル入力、構造化レスポンスを置き換える設定は拒否されます。

`OPENAI_MODEL` と `OPENAI_BASE_URL` は、Local Data Studio の設定項目としては使用しません。
Uvicorn から Local Data Studio のアプリケーション本体を直接起動する場合は、`LOCAL_DATA_STUDIO_CONFIG_FILE` で同じ TOML ファイルを指定できます。この場合も `[settings]` と `[llm]` の両方が反映されます。

</details>

<details>
<summary><strong>EDA のキャッシュ</strong></summary>

データセット全体の EDA レポートは、ファイルのフィンガープリント、行数上限、プロファイルモードに基づいて `./cache/eda` へ保存されます。
ここでいうフィンガープリントは、同じファイルかどうかを識別するための情報です。

SQL クエリ結果のレポートは、ファイルのフィンガープリント、SQL、行数上限、プロファイルモードに基づいて別のキャッシュとして保存されます。
EDA キャッシュ全体は既定で 1 GiB に制限され、`EDA_CACHE_MAX_BYTES` を超えると古いレポートから削除されます。

**Run EDA on Query Results** では、`rn` や `__rowid` のような内部処理用の補助カラムをレポートから除外します。

</details>

<details>
<summary><strong>Embedding Atlas の計算とキャッシュ</strong></summary>

Embedding Atlas ジョブは、選択したローカルエンコーダーモデルを使って埋め込みを計算し、その埋め込みに 2 次元への次元削減を行うため、完了まで時間がかかる場合があります。

埋め込みと次元削減後の座標を含む Parquet ファイルは、`./cache/atlas/datasets` に保存されます。
次の条件がすべて一致する場合だけ、既存のキャッシュを再利用します。

- データセットのフィンガープリント
- SQL
- 対象カラム
- モデル
- バックエンド
- プロンプトテンプレート
- バックエンド対応状況の判定に使ったモデル設定情報
- 次元削減手法とその設定

画像表示用のカラムは、元の URL、パス、`{bytes, path}` 形式を保持します。
エンコーダーへ渡す形式へ変換した値は、内部の埋め込み入力用カラムだけに保存します。

`ATLAS_SAMPLE=N` は、SQL による絞り込み後に埋め込み計算と次元削減を行い、キャッシュ Parquet に保存する行数を最大 `N` 行へ制限します。
ただし、現在の実装では、最初に DataFrame へ読み込む段階の行数までは制限しません。

UMAP は `full` と `anchor_transform` に対応しています。
t-SNE と PCA は、抽出済みのすべての埋め込みをまとめて次元削減します。

長いテキスト列と展開後のプロンプトは `ATLAS_TEXT_MAX_CHARS`、埋め込みのメモリ使用量は `ATLAS_EMBEDDING_DTYPE=float16`、キャッシュ容量は `ATLAS_CACHE_MAX_BYTES` で調整できます。

</details>

<details>
<summary><strong>埋め込みモデルのバックエンド判定</strong></summary>

バックエンドへの対応状況はモデル名だけでは判断せず、ローカルにある次の情報を、読み込み量に上限を設けて解析します。

- `modules.json`
- `config.json`
- tokenizer／processor の設定
- pooling の設定
- normalization のメタデータ

Sentence Transformers は、`native`、`generic_fallback`、`metadata_only`、`unsupported`、`unknown` のいずれかを返します。
Transformers は、`direct`、`remote_code`、`backbone_only`、`unsupported`、`unknown` のいずれかを返します。

Sentence Transformers の `generic_fallback` を選択できるのは、tokenizer を確認できるテキスト専用の Transformers モデルだけです。
画像モデルやマルチモーダルモデルを Sentence Transformers で実行するには、ネイティブな `modules.json` が必要です。

そのため、画像専用の DINOv3 チェックポイントでは Transformers だけを選択でき、モデルが宣言する `pooler_output` を使用します。
ネイティブな Sentence Transformers パイプラインを持つ Qwen3-VL-Embedding では、両方のバックエンドを利用できます。

実行可能なアダプターを確認できたバックエンドだけを選択できます。
`remote_code` は、`ATLAS_TRUST_REMOTE_CODE=true` を指定し、モデルリポジトリ内のコード実行を明示的に許可した場合だけ利用できます。
組み込みの Transformer／Pooling／Normalize 構成は、モデルリポジトリ内の Python コードを読み込まず、Transformers アダプターで再現します。

</details>

<details>
<summary><strong>キャッシュの保存場所と無効化</strong></summary>

キャッシュは用途ごとに、次のディレクトリへ分けて保存されます。

- `./cache/metadata`
- `./cache/index`
- `./cache/stats`
- `./cache/count`
- `./cache/search`
- `./cache/eda`
- `./cache/atlas`

Embedding Atlas のキャッシュは `./cache/atlas` の下へ保存され、次元削減後の座標を含む Parquet ファイルは `./cache/atlas/datasets` に保存されます。
EDA レポート全体の容量は `EDA_CACHE_MAX_BYTES`、Embedding Atlas 関連キャッシュ全体の容量は `ATLAS_CACHE_MAX_BYTES` で制限されます。上限を超えると、それぞれ古いファイルから削除されます。

フィンガープリントを使用するキャッシュは、対象ファイルのパス、サイズ、更新時刻に基づいて無効化されます。ここでいう無効化とは、元のファイルが変更されたと判断した場合に、古いキャッシュを再利用しないことです。

</details>

## 開発者向け情報

内部構成、主要モジュールの役割、開発時の起動方法、実装上の注意事項については、[IMPLEMENTATION_NOTES_ja.md](IMPLEMENTATION_NOTES_ja.md) を参照してください。

## コントリビューション

バグ報告や機能提案は、GitHub の Issue からお願いします。

コードを変更した場合は、コミット前に pre-commit を実行してください。

```bash
# すべてのファイルを対象に、自動整形・Lint・型チェックを実行する
uv run pre-commit run --all-files
```

pre-commit を環境へインストールせずに実行する場合は、次のコマンドも使用できます。

```bash
uvx pre-commit run --all-files
```

これらのコマンドは、主に次の処理をまとめて実行します。

- `uv run ruff format` または `uvx ruff format`: コードを自動整形します。
- `uv run ruff check` または `uvx ruff check`: コードの問題を検査します。
- `uv run ty check` または `uvx ty check`: 型の不整合を検査します。

Ruff は、アプリケーションコードとテストコードの両方に対して、PEP 257 を基本とする Google スタイルの docstring を適用します。
公開 API には、型や名前だけでは分からない制約、例外、副作用、所有権を記載してください。
一方、非公開の実装には、処理内容をそのまま言い換えるだけの説明を追加しません。

すべてのエラーを解消してからコミットしてください。

## 謝辞

- [Dataset Viewer（Hugging Face）](https://github.com/huggingface/dataset-viewer): UI と機能設計の参考にしています。
- [YData Profiling](https://github.com/ydataai/ydata-profiling): EDA レポートの生成に使用しています。
- [Embedding Atlas](https://github.com/apple/embedding-atlas): 埋め込みのインタラクティブな可視化に使用しています。

## ライセンス

本リポジトリは MIT License の下で公開されています。
