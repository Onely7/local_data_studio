<div align="center">

# Local Data Studio <img src="../images/local_data_studio_icon.png" alt="Local Data Studio Icon" align="right" width="150" style="margin-left:50px;"/>

**ローカルのデータセットをブラウザーで閲覧・分析するアプリケーション**

[English](../README.md) | 日本語

</div>

Local Data Studio は、JSONL、JSON、CSV、TSV、Parquet 形式のデータを、ブラウザーで閲覧・検索・分析できるローカル環境向けのアプリケーションです。
[Hugging Face Datasets の Data Studio](https://huggingface.co/docs/hub/data-studio#data-studio) を参考に開発されています。

データのプレビューに加えて、SQL による検索・集計、EDA（データの特徴をまとめる探索的データ分析）レポートの生成、Embedding Atlas による「埋め込み」の可視化ができます。
埋め込みとは、テキストや画像の特徴を数値で表したものです。
LLM（大規模言語モデル）を設定すると、日本語などの自然言語から SQL を生成したり、画面に表示中の値を翻訳したりできます。

初めて使う場合は、[まず使ってみる](#まず使ってみる)と[基本的な使い方](#基本的な使い方)から読むのがおすすめです。
詳しい設定や内部動作は、後半の折りたたみ項目にまとめています。
画面上部の Local Data Studio ロゴをクリックすると、この GitHub リポジトリが新しいタブで開きます。

<div align="center">
<img src="../images/local_data_studio_01.png" alt="Local Data Studio のメイン画面" width="90%">
</div>

## 主な特徴

* 大規模なデータでも、読み込む量を抑えながらプレビュー
* DuckDB SQL を使った、データを書き換えない検索・集計
* SQL 実行時のタイムアウト、メモリ制限、大量読み込みの警告
* データセット全体または SQL の実行結果を対象とした EDA レポートの生成
* テキスト列、画像列、または SQL の実行結果を対象とした Embedding Atlas 可視化
* 行の内容を詳しく確認できる **Row Inspector**
* URL、ローカルファイルのパス、`{bytes, path}` 形式で保存された画像の表示
* 画像の拡大表示と、同じ行に含まれる複数画像の切り替え
* 設定済みの LLM（LiteLLM 対応モデル）を使った、表示中のセルまたは列の手動翻訳
* ドラッグ＆ドロップによるファイルのアップロード
* アプリを開いている間だけの行の非表示と、必要に応じた元ファイルからの削除

## 対応環境とデータ形式

* Python 3.11、3.12、3.13
* 対応形式：`.jsonl`、`.json`、`.csv`、`.tsv`、`.parquet`

## まず使ってみる

通常の利用では、Python パッケージを配布する PyPI からインストールする方法が簡単です。

### 1. インストールする

```bash
pip install local-data-studio
```

### 2. データを指定して起動する

ディレクトリ内の対応ファイルを一覧表示する場合は、次のように起動します。

```bash
local-data-studio --data-dir /local/data/path
```

`/local/data/path` は、実際に閲覧したいデータが入っているディレクトリのパスへ置き換えてください。
パスとは、ファイルやディレクトリの場所を表す文字列です。

同じ処理は、Python モジュールとしても実行できます。

```bash
python -m local_data_studio --data-dir /local/data/path
```

単一のファイルだけを開く場合は、`--data-dir` の代わりに `--data-file` を指定します。

```bash
local-data-studio --data-file /local/data/example.parquet
```

### 3. ブラウザーで開く

起動後、ブラウザーで <http://127.0.0.1:8000> を開いてください。
通常は、Local Data Studio を起動したコンピューターと同じコンピューターからこのアドレスを開きます。

### 4. 終了する

Local Data Studio を起動したターミナルで `Ctrl+C` を押します。

## 基本的な使い方

### 1. データファイルを選択する

左側の **DATASETS** リストから、閲覧するファイルを選択します。
検索ボックスを使って、ファイル名を絞り込むこともできます。

長いファイル名は、リスト内で省略して表示されます。
ファイルサイズは有効数字 3 桁までに整えられ、`Bytes`、`kB`、`MB`、`GB`、`TB` の適切な単位で表示されます。

<img src="../images/local_data_studio_02.png" alt="DATASETS リストからファイルを選択する画面" width="45%">

### 2. データを閲覧・検索する

画面上部の操作を使って、表示内容を変更できます。

* **Search**：データを検索します。
* **Rows**：1 ページに表示する行数を変更します。
* **Prev**／**Next**：前後のページへ移動します。

<img src="../images/local_data_studio_03.png" alt="データの検索とページ移動を行う画面" width="45%">

### 3. SQL コンソールを使う

SQL は、データの検索や集計に使う言語です。
Local Data Studio の SQL コンソールでは、選択したデータセットを `data` というテーブルとして扱い、DuckDB SQL を実行できます。
実行できるのは、データを書き換えない読み取り専用のクエリ（SQL 文）です。

サーバー側で LiteLLM のモデル接続設定（モデルプロファイル）を用意している場合は、日本語などの自然言語による指示から SQL を生成できます。
設定済みの OpenAI、Anthropic、Gemini、hosted vLLM、その他の LiteLLM 対応モデルを選択できます。

SQL の実行には、タイムアウト、メモリ制限、大量のデータを読み込む可能性の検査が適用されます。
生成される SQL の制限については、[SQL 生成・翻訳用 LLM モデルプロファイル](#sql-生成翻訳用-llm-モデルプロファイル)を参照してください。

<img src="../images/local_data_studio_04.png" alt="SQL コンソールの画面" width="45%">

### 4. EDA レポートを生成する

EDA（探索的データ分析）レポートは、列の型、値の分布、欠損など、データの特徴をまとめて確認するためのレポートです。

**Run EDA** を実行すると、データセットから読み込んだ行を対象に EDA レポートを生成します。
生成したレポートは、同じ結果を再利用するための保存領域（キャッシュ）へ保存されます。
そのため、同じ条件で再実行した場合に再利用できます。

**Run EDA on Query Results** を使うと、SQL コンソールに表示されている現在の実行結果を対象にレポートを生成できます。

読み込む行数の上限は、`local_data_studio.toml` の `[settings]` にある `eda_row_limit`、または環境変数／`.env` の `EDA_ROW_LIMIT` で指定します。
この値は画面から変更できません。
`1` 以上の整数を指定でき、`-1` を指定すると行数制限を解除します。

EDA パネルの **Profile mode** では、分析の詳しさを実行ごとに選択できます。
既定値は `minimal` です。

<img src="../images/local_data_studio_05.png" alt="EDA の実行画面" width="45%"> <img src="../images/local_data_studio_06.png" alt="生成された EDA レポート" width="45%">

### 5. 埋め込みを可視化する

埋め込みとは、テキストや画像の特徴を、数値の並び（ベクトル）として表したものです。
埋め込みは通常、多くの数値から構成されるため、そのままでは画面上で比較しにくくなります。
Local Data Studio は、UMAP、t-SNE、PCA などを使って埋め込みを 2 次元の座標へ変換し、Embedding Atlas で表示します。
この変換処理を「次元削減」と呼びます。
設定名や内部コードでは `projection` と表記している箇所がありますが、この README では UMAP、t-SNE、PCA をまとめて「次元削減」と表記します。

この機能を使うには、Hugging Face 形式のローカルエンコーダーモデルを自分で配置する必要があります。
Hugging Face 形式とは、モデルの設定ファイルなどを所定の構成で保存した形式です。
エンコーダーモデルは、テキストや画像を埋め込みへ変換するモデルです。

モデルは、`models/embedder`、または `--models-dir`／`EMBEDDER_MODELS_DIR` で指定したディレクトリの下へ配置します。

配置例：

```text
models/embedder/google/siglip2-base-patch16-224
models/embedder/Qwen/Qwen3-Embedding-0.6B
models/embedder/Qwen/Qwen3-VL-Embedding-2B
```

`config.json`、`modules.json`、`tokenizer_config.json`、`preprocessor_config.json` など、モデルを識別するファイルを含むディレクトリが **Model** の選択欄に表示されます。

**Visualize Embedding** で、次の項目を選択します。

1. テキスト列または画像列
2. 使用するモデル
3. モデルを読み込み、埋め込みを計算するライブラリ（バックエンド）
4. 2 次元へ変換する方法（次元削減手法）

次元削減手法は、**UMAP**（既定）、**t-SNE**、**PCA** から選択できます。
**Run Atlas** を実行すると、データセットを対象とした Embedding Atlas ページが起動します。
**Run Atlas on Query Results** を使うと、現在の SQL の実行結果を対象に可視化できます。

モデルを一覧表示するときは、モデル本体の大きなデータ（重み）を読み込まず、設定ファイルだけを確認します。
利用できないバックエンドは一覧に表示されますが、選択できません。
Sentence Transformers と Transformers の両方を利用できる場合は、Sentence Transformers が最初に選択されます。

処理は、画面の操作を続けられるバックグラウンド処理として実行されます。
進捗は処理段階ごとに表示され、一定件数ずつ処理するバッチが終わるたびに更新されます。
キャンセルは、現在処理中のバッチまたは処理段階が終了した時点で反映されます。

準備が完了すると、**Open Atlas** リンクが表示されます。

<details>
<summary><strong>Sentence Transformers の Prompt を使う</strong></summary>

Sentence Transformers を選択すると、モデルへ追加の指示を渡す **Prompt** 欄が表示されます。
空欄の場合は、モデルに保存された既定のプロンプトを使用します。

* プレースホルダーを含まない文字列は、選択した列の各値の先頭へ追加されます。
* `{title}` や `{body}` のようなプレースホルダーは、同じデータ行または SQL の実行結果にある対応する列の値へ置き換えられます。
* `{{` と `}}` は、通常の波括弧として扱われます。
* 存在しない列、対応していない変換指定や書式指定、閉じられていない波括弧は、モデルを読み込む前にエラーとなります。

</details>

<img src="../images/local_data_studio_07.png" alt="Embedding Atlas の設定画面" width="45%"> <img src="../images/local_data_studio_08.png" alt="Embedding Atlas の可視化画面" width="45%">

### 6. Row Inspector と画像の拡大表示を使う

行をクリックすると、**Row Inspector** に各列の詳しい内容が表示されます。
長い値は最初は省略されますが、**Raw** に切り替えると省略前の値を確認できます。

デスクトップ幅では、データセット一覧、プレビュー、インスペクターが画面内の同じ高さに並び、それぞれの領域内でスクロールします。
モバイル／タブレット用のレイアウトでは、ページ全体を縦にスクロールし、**DATASETS** ブロックがタイトルバーのすぐ下に表示されます。

画像として認識された値は、クリックすると拡大表示できます。
次の形式に対応しています。

* 画像 URL
* 相対パスまたは絶対パス
* `{ "bytes": ..., "path": ... }` 形式の辞書（オブジェクト）

`bytes` と `path` の両方がある場合は、まず `bytes` の内容を表示します。
表示できなかった場合は、`path` を代わりに使用します。

<img src="../images/local_data_studio_09.png" alt="Row Inspector の画面" width="45%"> <img src="../images/local_data_studio_10.png" alt="画像の拡大表示画面" width="45%">

### 7. 表示中の値を翻訳する

翻訳機能を使うには、LLM モデルプロファイルで `translation = true` を設定します。
その後、画面上部のツールバーでモデルと翻訳先の言語を選択します。
デスクトップでは、翻訳用の選択欄を上段に、データ検索を下段左側に、行数とページ操作を下段右側に配置します。
画面幅が狭い場合は、利用できる幅に収まるように各操作を縦方向へ並べ替えます。

`local_data_studio.toml` の `[translation]` に `default_target_language = "ja"` のように指定すると、対応する言語コードをすべてのブラウザーで最初の翻訳先にできます。
この設定は、ブラウザーに保存された前回の選択や、ブラウザーの言語設定より優先されます。

展開表示したフィールド内の翻訳アイコンでは、1 つのセルを翻訳できます。
列見出しの翻訳アイコンでは、現在のページに表示されている、その列の値を翻訳できます。
列見出しの翻訳アイコンは、表示中の値に自然言語の文字列が 1 件以上含まれる場合だけ表示されます。
リストやオブジェクトは再帰的に調べ、キー、並び順、文字列以外の値を維持したまま翻訳します。
数値だけの値や構造、真偽値、バイナリ値（バイト列）、画像・音声と判定されたデータは翻訳対象から除外されます。

翻訳は、必ず利用者が操作したときだけ実行されます。
翻訳によってデータセットが書き換えられることはありません。
また、別のページを読み込んだり、元ファイル全体を調べたり、**Raw** の完全な値を自動取得したりすることもありません。

選択した LLM プロバイダーへ送信されるのは、現在のプレビュー、検索結果、または SQL の実行結果にすでに読み込まれている、長さを制限した値だけです。
原文はそのまま残り、翻訳結果が下に表示されます。
展開した原文と翻訳結果は、それぞれ専用のコピーアイコンからコピーできます。
リストやオブジェクトのコード表示では、元の JSON と翻訳結果に加えて、利用できるコピー操作と翻訳操作も表示されます。
元の JSON と翻訳後の JSON は、縦にスクロールできる一つのコード表示領域を共有するため、画面幅が狭い場合も両方を確認できます。

翻訳ジョブはバックグラウンドで監視しますが、ツールバーには進捗文言や Cancel ボタンを常設しません。
別の翻訳を開始した場合や、モデルまたは翻訳先言語を変更した場合は、不要になったリクエストへ協調的なキャンセルを要求します。
失敗時の内容は、共通のエラー画面に表示します。

大きなリクエストでは、送信前に確認画面が表示されます。
翻訳結果は、現在のページを開いている間だけブラウザーのメモリに保持されます。
サーバーのキャッシュや、ブラウザーの保存領域である `localStorage` には保存されません。
データセット、表示方法、ページ、モデル、言語、または原文が変わった場合は、別の条件で得た翻訳結果を誤って表示しません。

<img src="../images/local_data_studio_11.png" alt="表示中のセルや列を翻訳する画面" width="45%"> <img src="../images/local_data_studio_12.png" alt="原文と翻訳結果を表示する画面" width="45%">

## 設定ファイルとパス

### 基本方針

この README では、データ、キャッシュ、ローカルモデル、設定ファイルをまとめて置く作業用ディレクトリを「ワークスペース」と呼びます。

プロジェクトごとに、ワークスペース内へ `local_data_studio.toml` を 1 つ置く運用を推奨します。
`local_data_studio.toml` は、TOML 形式のテキスト設定ファイルです。
TOML は、設定項目の名前と値を読みやすく記述するための形式です。
通常のアプリケーション設定は、このファイルにまとめます。

`.env` は、API キーなどの認証情報と、そのコンピューターだけに適用したい上書き設定に使います。
認証情報や個別の上書きが不要であれば、`.env` を作成する必要はありません。

次のように `--config` を付けて起動すると、使用する設定ファイルが明確になります。
相対パスも、その設定ファイルがあるワークスペースを基準に解決されます。

```bash
local-data-studio --config ./local_data_studio.toml
```

### `local_data_studio.toml` を作成する

リポジトリには、設定例を記載した [local_data_studio.example.toml](../local_data_studio.example.toml) が含まれています。
認証情報は含まれていません。
次のコマンドでコピーし、必要な項目を編集してください。

```bash
cp local_data_studio.example.toml local_data_studio.toml
```

主なセクションは次のとおりです。

* `[paths].data_dir`：閲覧するデータが入っているディレクトリ
* `[paths].data_file`：直接開く単一のデータファイル
* `[server]`：サーバーの設定
* `[settings]`：EDA、Embedding Atlas、元ファイルからの削除許可などの設定
* `[llm]`：SQL 生成と手動翻訳に使う LLM モデルプロファイル
* `[translation]`：翻訳先の初期値や翻訳リクエストの上限

たとえば、データの保存先と EDA で読み込む最大行数は、次のように指定します。

```toml
[paths]
data_dir = "/local/data/path"

[settings]
eda_row_limit = 50000
```

`[settings]` では、環境変数名を小文字の snake_case（単語をアンダースコアでつなぐ書き方）へ変えた名前を使用します。
たとえば、`EDA_ROW_LIMIT` は `eda_row_limit`、`ALLOW_DELETE_DATA` は `allow_delete_data` です。
テンプレートには、指定できる設定がすべて記載されています。
キーを省略した場合は、`.env` またはアプリケーションの既定値が使われます。

API キーは、`.env` またはターミナルのシェルに設定した環境変数へ保存します。
環境変数は、名前と値をアプリケーションへ渡す仕組みです。
各 LLM モデルプロファイルの `api_key_env` には、API キーを保存した環境変数の名前を指定します。

### 既定のパス

特に指定しない場合、次のファイルやディレクトリは、コマンドを実行したディレクトリを基準に検索または作成されます。

* `.env`
* `data`
* `cache`
* `models/embedder`

コマンドを実行したディレクトリは、「現在の作業ディレクトリ」または「カレントディレクトリ」と呼ばれます。
毎回同じ場所を基準に起動したい場合は、`--workspace-dir` または `--config` を指定してください。

個別のパスは、次のコマンドラインオプションで上書きできます。

* `--data-dir`
* `--data-file`
* `--cache-dir`
* `--models-dir`
* `--env-file`
* `--file-serve-roots`

### 設定の優先順位

同じ設定項目を複数の場所に指定した場合は、次の順に適用されます。
上にあるものほど優先されます。

1. コマンドラインオプション
2. OS の環境変数
3. `local_data_studio.toml`
4. `.env`
5. ワークスペースを基準とした既定値
6. 現在の作業ディレクトリを基準とした既定値

### 環境変数と TOML 設定

以下の環境変数は、TOML の `[settings]` セクションに小文字の snake_case 形式でも指定できます。

<details>
<summary><strong>データとパスの設定</strong></summary>

* `DATA_FILE`：単一のデータファイルを直接指定します。指定した場合は `DATA_DIR` より優先されます。
* `DATA_DIR`：データセットを検索するディレクトリです。`DATA_FILE` を使用しない場合は必須です。
* `FILE_SERVE_ROOTS`：ローカル画像の配信を許可するディレクトリを、カンマ区切りで指定します。
* `VIS_EXCLUDE_DIRS`：`DATA_DIR` の下にあるディレクトリのうち、データセットの検索対象から除外するものをカンマ区切りで指定します。
* `VIS_EXCLUDE_FILES`：`DATA_DIR` の下にあるファイルのうち、データセットの検索対象から除外するものをカンマ区切りで指定します。相対パスは `DATA_DIR` を基準に解決され、絶対パスも指定できます。

</details>

<details>
<summary><strong>LLM の認証情報</strong></summary>

* `OPENAI_API_KEY`、`ANTHROPIC_API_KEY`、`GEMINI_API_KEY`：LLM モデルプロファイルの `api_key_env` から参照する認証情報の例です。これらの値がブラウザーへ送信されることはありません。

</details>

<details>
<summary><strong>EDA の設定</strong></summary>

* `EDA_ROW_LIMIT`：データセット全体または SQL の実行結果から EDA レポートへ読み込む最大行数です。画面からは変更できません。`1` 以上の整数を指定でき、`-1` を指定すると行数を制限しません。
* `EDA_CELL_MAX_CHARS`：EDA で扱う文字列セルの最大文字数です。上限を超えた部分は `... (truncated)` として省略されます。
* `EDA_NESTED_POLICY`：リスト、構造体、オブジェクト、バイナリなどの入れ子になった値をどのように扱うかを指定します。`stringify` は文字列へ変換して残し、`drop` は対象の列を除外します。
* `EDA_CACHE_MAX_BYTES`：`./cache/eda` に保存する EDA レポート全体の最大容量です。既定値は 1 GiB で、上限を超えると古いレポートから削除されます。

</details>

<details>
<summary><strong>Embedding Atlas の設定</strong></summary>

* `EMBEDDER_MODELS_DIR`：ローカルの Hugging Face エンコーダーモデルを保存する親ディレクトリです。既定では、ワークスペースまたは現在の作業ディレクトリの `models/embedder` を使用します。
* `ATLAS_HOST`、`ATLAS_PORT`：Embedding Atlas の子プロセス（Local Data Studio から起動される別の処理）に使う IPv4 ループバックホストと、内部ポートを探し始める番号です。ループバックホストは、そのコンピューター自身だけを指す接続先です。`localhost` は `127.0.0.1` へ正規化されます。IPv6 と外部から接続できるホストは使用できません。ブラウザーがこのポートへ直接接続することはありません。
* `ATLAS_MAX_INSTANCES`：準備中と実行中を合わせた Atlas 子プロセス数の上限です。既定値は `4` で、`1` 以上を指定します。
* `ATLAS_SAMPLE`：埋め込み計算と次元削減を行い、Atlas 用キャッシュ（Parquet ファイル）に保存する行数の上限です。SQL を適用した後、乱数の初期値（シード）を 42 に固定して、同じ入力からは毎回同じ行が選ばれるように抽出します。未設定または `0` の場合は、選択されたすべての行を使用します。負の値は指定できません。
* `ATLAS_BATCH_SIZE`：埋め込み計算で一度に処理する行数（バッチサイズ）です。未設定または `0` の場合は、Embedding Atlas の既定値を使用します。
* `ATLAS_CACHE_MAX_BYTES`：`./cache/atlas` に保存する Embedding Atlas 関連キャッシュ全体の最大容量です。上限を超えると古いキャッシュファイルから削除されます。
* `ATLAS_TEXT_MAX_CHARS`：埋め込みへの入力と、Atlas 用キャッシュ（Parquet ファイル）に残すテキストセルの最大文字数です。`0` を指定すると省略しません。
* `ATLAS_EMBEDDING_DTYPE`：次元削減前の埋め込み配列に使用する数値精度です。`float32` または `float16` を指定できます。
* `ATLAS_UMAP_PROJECTION_MODE`：UMAP による次元削減の実行方式です。`full` は、抽出したすべての埋め込みをまとめて処理します。`anchor_transform` は、代表となる行で UMAP の配置を決め、残りの行を同じ 2 次元空間へ配置します。t-SNE と PCA は、抽出したすべての行をまとめて処理します。
* `ATLAS_UMAP_ANCHOR_SAMPLE`：`ATLAS_UMAP_PROJECTION_MODE=anchor_transform` の場合に、UMAP の学習へ使用する行数です。
* `ATLAS_TRUST_REMOTE_CODE`：`true` を指定すると、Local Data Studio がモデルを読み込む際に、選択したローカルエンコーダーモデルのリポジトリ内にあるコードの実行を許可します。信頼できるモデル以外では `false` のままにしてください。

</details>

<details>
<summary><strong>データ削除の設定</strong></summary>

* `ALLOW_DELETE_DATA`：`false` の場合は、元のデータファイルからの削除を禁止します。アプリのページを開いている間だけ、画面上で一時的に非表示にする操作は可能です。

</details>

## その他の起動方法

### ソースコードからセットアップする

開発やコードの変更を行う場合は、ソースコードから実行できます。
次のソフトウェアが必要です。

* Python 3.11〜3.13
* Git：GitHub からソースコードを取得するために使います。
* uv：Python の実行環境を作り、必要なライブラリをインストールするために使います。

以下のコマンドは、上から 1 つずつ実行してください。

1. **リポジトリを取得する**

   ```bash
   # GitHub からリポジトリをダウンロードする
   git clone https://github.com/Onely7/local_data_studio.git

   # ダウンロードしたディレクトリへ移動する
   cd local_data_studio
   ```

2. **開発環境を準備する**

   ```bash
   # プロジェクトの設定に基づいて、必要なライブラリをインストールする
   uv sync
   ```

3. **設定ファイルを作成する**

   ```bash
   # 設定ファイルのひな型をコピーする
   cp local_data_studio.example.toml local_data_studio.toml
   ```

4. **`local_data_studio.toml` を編集する**

   少なくとも、閲覧するディレクトリまたはファイルを指定します。

   ```toml
   [paths]
   data_dir = "/local/data/path"

   [settings]
   eda_row_limit = 50000
   ```

   設定項目の説明は、[設定ファイルとパス](#設定ファイルとパス)を参照してください。

5. **必要な場合だけ `.env` を作成する**

   LLM プロバイダーの API キーや、そのコンピューターだけに適用したい上書き設定が必要な場合に作成します。

   ```bash
   cp .env.example .env
   ```

   たとえば、LLM モデルプロファイルの `api_key_env` が参照する API キーを記述します。

   ```dotenv
   OPENAI_API_KEY=your_openai_api_key
   ```

   `.env` は Git の管理対象外です。

6. **Local Data Studio を起動する**

   ```bash
   # 設定ファイルを指定し、開発用の自動再読み込みを有効にして起動する
   uv run local-data-studio --config ./local_data_studio.toml --reload
   ```

7. **ブラウザーで開く**

   ターミナルに次のようなメッセージが表示されたら、起動は完了です。

   ```text
   INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
   INFO:     Application startup complete.
   ```

   ブラウザーで <http://127.0.0.1:8000> を開いてください。
   終了するときは、起動したターミナルで `Ctrl+C` を押します。

### リモートサーバーで使用する

別のコンピューター上で Local Data Studio を起動する場合は、SSH トンネルを使います。
SSH トンネルは、リモート側のポートを暗号化された SSH 接続経由で手元のコンピューターへ転送する方法です。

```bash
ssh -N -L 8000:127.0.0.1:8000 user@example-server
```

このコマンドでは、`-N` によってリモート側で別のコマンドを実行せず、`-L` によって手元のポート `8000` をリモート側の `127.0.0.1:8000` へ転送します。
`user@example-server` は、実際のユーザー名と接続先サーバー名に置き換えてください。
接続後、手元のブラウザーで <http://127.0.0.1:8000> を開きます。

Atlas ページは Local Data Studio を経由して配信されるため、Atlas の内部ポートを別途 SSH 転送する必要はありません。

## 利用上の注意

* 大規模なデータでは、検索や EDA に時間がかかることがあります。
* 行数のカウント、データ全体の検索、サンプル統計、EDA は、進捗の確認とキャンセルができるバックグラウンド処理として実行されます。
* `EDA_ROW_LIMIT=-1` を指定すると、選択したすべての行を分析用のメモリへ読み込みます。データ全体が無理なくメモリへ収まる場合だけ使用してください。
* テラバイト（TB）級の JSON 配列ファイルは推奨しません。大規模なデータを効率よく閲覧する場合は、JSONL または Parquet を推奨します。
* t-SNE は、データ量が増えると処理時間とメモリ使用量が急増します。大規模なデータでは、`ATLAS_SAMPLE` に現実的な上限を設定してください。

> [!WARNING]
> **Delete from file** は元のデータファイルを書き換えます。必要に応じて、操作前にバックアップを作成してください。

* `ALLOW_DELETE_DATA=false` の場合、行は元のデータファイルから削除されません。アプリのページを開いている間だけ、一時的に非表示にできます。
* `models/embedder` または設定したモデルディレクトリには、モデル本体を各自で配置してください。配布物にはモデルファイルを含めず、空のディレクトリを維持するためのプレースホルダーだけを含めています。

## 高度な設定と技術仕様

このセクションは、動作の詳細を確認したい利用者や管理者向けです。
通常の閲覧・分析だけを行う場合は、読み飛ばしても問題ありません。

<a id="大規模データのプレビューとバックグラウンド処理"></a>
<details>
<summary><strong>大規模データのプレビューとバックグラウンド処理</strong></summary>

非常に大きなデータセットでは、対応形式のプレビューに大きな `OFFSET` を使用せず、カーソル形式の `page_token` を使用します。
`OFFSET` は先頭から指定した行数を飛ばす方法で、値が大きいほど前の行をたどる処理が増える場合があります。
`page_token` は現在の位置を示す情報で、前後のページへ効率よく移動するために使われます。

行数のカウント、データ全体の検索、サンプル統計、EDA は、進捗の確認とキャンセルができるバックグラウンド処理として実行されます。

**Count Rows**、EDA、Atlas の実行後に表示されるメッセージは、ほかの操作を妨げない共通のコンパクトな形式に統一されています。

</details>

<a id="sql-生成翻訳用-llm-モデルプロファイル"></a>
<details>
<summary><strong>SQL 生成・翻訳用 LLM モデルプロファイル</strong></summary>

モデルプロファイルは、使用する LLM、認証情報、接続先、タイムアウトなどをまとめた設定です。
SQL 生成と翻訳に使うモデルプロファイルは、`local_data_studio.toml` の `[llm]` セクションから読み込みます。

モデル名には、`openai/`、`anthropic/`、`gemini/`、`hosted_vllm/` など、LiteLLM のプロバイダープレフィックスを明示してください。
プロバイダープレフィックスは、モデル名の先頭に付ける、接続先のプロバイダーを示す識別子です。
非推奨の `vllm/` は使用できません。

`model` には、1 件のモデル名、または同じプロバイダーに属するモデル名のリストを指定できます。
リスト内のモデルは、同じプロファイルの認証情報、接続先、タイムアウト、`provider_options` を共有します。
そのため、異なるプロバイダーのモデルを同じプロファイルに混在させることはできません。
各モデルは、有効にした機能ごとに個別の選択肢として表示されます。
`default_model` にプロファイル ID を指定した場合は、リストの先頭のモデルが最初に選択されます。

`sql_generation = true` を指定すると SQL コンソールで、`translation = true` を指定すると翻訳ツールバーで選択できます。
後方互換のため、`sql_generation` の既定値は `true` です。
翻訳は明示的に有効にする必要があり、`translation` の既定値は `false` です。
両方を無効にしたプロファイルは設定エラーになります。

SQL と翻訳の既定モデルは、`default_sql_generation_model` と `default_translation_model` で別々に指定します。
従来の `default_model` は SQL 用の別名として残されていますが、`default_sql_generation_model` と異なる値を同時に指定することはできません。

認証情報は、`api_key_env` が参照する環境変数へ保存します。
`provider_options` は、管理者が指定する信頼済みの設定です。
`reasoning_effort`、`thinking`、トークン上限、`top_k`、`extra_body` などを指定できます。
一方で、メッセージ、認証情報、ストリーミング、ツール、マルチモーダル入力、構造化レスポンスを置き換える設定は拒否されます。

Local Data Studio は、LLM が通常のテキストとして返した SQL だけを受け取り、ツール呼び出しは使用しません。
生成できる SQL は、単一の `SELECT` 文、または `WITH` 句による共通テーブル式（CTE）を使った `SELECT` 文に制限されます。

`OPENAI_MODEL` と `OPENAI_BASE_URL` は、Local Data Studio の設定項目としては使用しません。
Uvicorn から Local Data Studio のアプリケーション本体を直接起動する場合は、`LOCAL_DATA_STUDIO_CONFIG_FILE` で同じ TOML ファイルを指定できます。
この場合も、`[settings]` と `[llm]` の両方が反映されます。

任意の `[translation]` セクションでは、最初に選択する翻訳先の言語、行数、文字列数、文字数、チャンク、同時実行数、ブラウザーで確認を求める規模を設定できます。
`default_target_language` には、`ja` や `en` のような対応済みの言語コードを指定します。
サーバーはブラウザーから申告された件数をそのまま信用せず、リクエストごとに許可される上限値を再計算します。

</details>

<details>
<summary><strong>EDA の読み込みとキャッシュ</strong></summary>

現在のセッション（アプリを開いて操作している間）で非表示にした行がないデータセットでは、pandas の DataFrame を作る前に、データの読み込み元へ `EDA_ROW_LIMIT` を直接適用します。
そのため、上限より多い行を EDA 用の DataFrame へ先に読み込むことはありません。

データセット全体の EDA レポートは、ファイルのフィンガープリント、行数上限、プロファイルモードに基づいて `./cache/eda` へ保存されます。
ここでいうフィンガープリントは、同じファイルの同じ状態かどうかを識別するための情報です。

SQL の実行結果から作成したレポートは、ファイルのフィンガープリント、SQL、行数上限、プロファイルモードに基づいて、別のキャッシュとして保存されます。
EDA キャッシュ全体は既定で 1 GiB に制限され、`EDA_CACHE_MAX_BYTES` を超えると古いレポートから削除されます。

**Run EDA on Query Results** では、`rn` や `__rowid` のような内部処理用の補助列をレポートから除外します。

</details>

<details>
<summary><strong>Embedding Atlas の計算とキャッシュ</strong></summary>

Embedding Atlas の処理では、選択したローカルエンコーダーモデルを使って埋め込みを計算し、その埋め込みを 2 次元へ次元削減します。
データ量や使用するモデルによっては、完了まで時間がかかる場合があります。

表示用の行と次元削減後の座標を含む Parquet ファイルは、`./cache/atlas/datasets` に保存されます。
次の条件がすべて一致する場合だけ、既存のキャッシュを再利用します。

* データセットのフィンガープリント
* SQL
* 対象列
* モデル
* バックエンド
* プロンプトテンプレート
* バックエンドへの対応状況を判定するときに使ったモデル設定情報
* 次元削減手法とその設定

画像表示用の列は、元の URL、パス、`{bytes, path}` 形式を保持します。
エンコーダーへ渡す形式に変換した値は、内部の埋め込み入力用の列だけに保存します。

`ATLAS_SAMPLE=N` を指定すると、SQL による絞り込み後、pandas の DataFrame を作る前に、DuckDB 内で毎回同じ結果になるように行を抽出します。
そのため、埋め込み計算、次元削減、キャッシュ Parquet のために DataFrame 化される行も最大 `N` 行です。
`ATLAS_SAMPLE=0` の場合は、選択されたすべての行を処理します。

テキストとプロンプトテンプレートは、現在の埋め込みバッチに必要な分だけ展開します。
画像の `bytes` は、一時的なディスク上の作業領域へ順次保存し、現在のバッチに必要な分だけメモリへ読み込みます。
この一時領域は、処理の成功、失敗、キャンセルのいずれの場合も削除されます。

ただし、`full` モードの UMAP、t-SNE、PCA は、抽出済みの埋め込み行列全体を必要とします。
`anchor_transform` モードでは、基準となる行（アンカー）の埋め込みと、現在処理しているバッチだけを保持します。

UMAP は `full` と `anchor_transform` に対応しています。
t-SNE と PCA は、抽出済みのすべての埋め込みをまとめて次元削減します。

長いテキスト列と展開後のプロンプトは `ATLAS_TEXT_MAX_CHARS`、埋め込みのメモリ使用量は `ATLAS_EMBEDDING_DTYPE=float16`、キャッシュ容量は `ATLAS_CACHE_MAX_BYTES` で調整できます。

準備中と実行中の Atlas プロセスは、合わせて `ATLAS_MAX_INSTANCES` 件まで保持します。
実行中の Atlas インスタンスは、Local Data Studio を再起動せずに停止できます。

```bash
curl -X DELETE http://127.0.0.1:8000/api/atlas/instances/INSTANCE_ID
```

インスタンス ID は、**Open Atlas** URL の `/atlas/` と末尾の `/` の間にある値です。
この推測困難な値は中継先を識別するためのもので、認証機能ではありません。

Atlas のページと、起動確認に使うメタデータ取得先の両方が応答できる状態になった後にだけ、**Open Atlas** リンクが表示されます。
リンクは Local Data Studio と同じ接続元の `/atlas/{instance_id}/` を使うため、子プロセスの内部ポートはブラウザーへ公開されません。

認証機能を設定していない Local Data Studio を、信頼できないネットワークへ直接公開しないでください。
`127.0.0.1` などのループバックアドレスだけで待ち受けるか、SSH トンネル経由で使用してください。

</details>

<details>
<summary><strong>埋め込みモデルのバックエンド判定</strong></summary>

バックエンドへの対応状況は、モデル名だけでは判断しません。
Local Data Studio は、ローカルにある次の設定情報を、読み込み量に上限を設けて解析します。

* `modules.json`
* `config.json`
* `tokenizer`（トークナイザー：テキストをモデル用の単位へ分割する処理）と `processor`（プロセッサー：画像などの入力を整える処理）の設定
* `pooling`（プーリング：複数の特徴を 1 つの埋め込みへまとめる処理）の設定
* `normalization`（正規化：埋め込みの大きさを整える処理）のメタデータ

Sentence Transformers の判定結果は、`native`、`generic_fallback`、`metadata_only`、`unsupported`、`unknown` のいずれかです。
Transformers の判定結果は、`direct`、`remote_code`、`backbone_only`、`unsupported`、`unknown` のいずれかです。

Sentence Transformers の `generic_fallback` を選択できるのは、トークナイザーを確認できるテキスト専用の Transformers モデルだけです。
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

* `./cache/metadata`
* `./cache/index`
* `./cache/stats`
* `./cache/count`
* `./cache/search`
* `./cache/eda`
* `./cache/atlas`

Embedding Atlas のキャッシュは `./cache/atlas` の下へ保存され、次元削減後の座標を含む Parquet ファイルは `./cache/atlas/datasets` に保存されます。

EDA レポート全体の容量は `EDA_CACHE_MAX_BYTES`、Embedding Atlas 関連キャッシュ全体の容量は `ATLAS_CACHE_MAX_BYTES` で制限されます。
上限を超えると、それぞれ古いファイルから削除されます。

JSON キャッシュは、一時ファイルへの書き込みが完了してから、正常なキャッシュを一度に置き換えます。
そのため、書き込みが途中で中断しても、正常なキャッシュが不完全なファイルで上書きされません。

フィンガープリントを使用するキャッシュは、対象ファイルのパス、サイズ、更新時刻に基づいて無効化されます。
ここでいう無効化とは、元のファイルが変更されたと判断した場合に、古いキャッシュを再利用しないことです。

</details>

## 開発者向け情報

内部構成、主要モジュールの役割、開発時の起動方法、実装上の注意事項については、[IMPLEMENTATION_NOTES_ja.md](IMPLEMENTATION_NOTES_ja.md) を参照してください。

## コントリビューション

バグ報告や機能提案は、GitHub の Issue からお願いします。

コードを変更した場合は、コミット前に pre-commit を実行してください。
pre-commit は、コミット前にコードの整形や検査をまとめて実行する仕組みです。

```bash
# すべてのファイルを対象に、自動整形、Lint、型チェックを実行する
uv run pre-commit run --all-files
```

pre-commit をプロジェクトの実行環境へインストールせずに実行する場合は、次のコマンドも使用できます。

```bash
uvx pre-commit run --all-files
```

これらのコマンドは、主に次の処理をまとめて実行します。

* `uv run ruff format` または `uvx ruff format`：コードを自動整形します。
* `uv run ruff check` または `uvx ruff check`：Lint（コードの書き方に問題がないかを機械的に検査する処理）を実行します。
* `uv run ty check` または `uvx ty check`：型の不整合を検査します。

Ruff は、アプリケーションコードとテストコードの両方に対して、PEP 257 を基本とする Google スタイルの docstring を適用します。
docstring は、Python の関数やクラスなどの目的や使い方を説明する文字列です。

公開 API には、型や名前だけでは分からない制約、例外、副作用、リソースの所有関係を記載してください。
一方、非公開の実装には、処理内容をそのまま言い換えるだけの説明を追加しません。

すべてのエラーを解消してからコミットしてください。

## 謝辞

* [Dataset Viewer（Hugging Face）](https://github.com/huggingface/dataset-viewer)：UI と機能設計の参考にしています。
* [YData Profiling](https://github.com/ydataai/ydata-profiling)：EDA レポートの生成に使用しています。
* [Embedding Atlas](https://github.com/apple/embedding-atlas)：埋め込みのインタラクティブな可視化に使用しています。

## ライセンス

本リポジトリは MIT License の下で公開されています。
