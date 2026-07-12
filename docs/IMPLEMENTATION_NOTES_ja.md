[README_ja.md に戻る](README_ja.md)

# 開発者向けの実装メモ

このセクションでは、主なソースコードの役割を説明します。

- アプリケーション本体は `src/local_data_studio` にあります。静的 UI ファイルは `src/local_data_studio/static` にあり、Python パッケージへ含まれます。ワークスペース内の `local_data_studio.toml` を、パス、サーバー、EDA、Atlas、削除許可、LLM プロファイルの通常の設定場所とし、CLI では `--config` で明示的に選択します。`.env` は、設定済みのワークスペースを基準に、認証情報と任意のマシン固有上書きのためだけに読み込みます。実行時の `data`、`cache`、`models/embedder` は、選択したワークスペースまたは現在の作業ディレクトリを基準にします。設定の優先順位は、コマンドラインオプション、OS の環境変数、設定ファイル、`.env`、ワークスペースのデフォルト、現在の作業ディレクトリのデフォルトの順です。
- `src/local_data_studio/app.py` は、アプリケーション全体を組み立てる小さなエントリーポイントです。リクエストモデルと API ルートは `src/local_data_studio/server/api` にあり、データセットアクセス、分析、バックグラウンドジョブ、データ変更、共通サービス、静的ファイルのマウントに分割されています。ファイルシステム、DuckDB、EDA、ジョブに関するブロッキング処理は FastAPI のスレッドプールで実行し、ストリーミングアップロードだけを非同期処理のまま維持します。
- `src/local_data_studio/server/readers.py` は互換性のための窓口として維持し、形式ごとの実装は `src/local_data_studio/server/dataset_readers` に分割しています。JSONL のメタデータ推論は、行数とバイト数の固定上限へ達すると停止します。JSONL、CSV、TSV のプレビューでは、フィンガープリント付きの疎な行インデックスと、バイト位置またはページトークンを使用します。完成済みのインデックスは再利用し、チェックポイントはバッチ単位のトランザクションで保存します。CSV／TSV のスキーマ、プレビュー、検索、Raw 表示は、長いフィールドに対応する共通パーサーを使用します。Parquet のスキーマはフッターのメタデータだけから読み込みます。プレビューと Raw 表示には読み込み量を制限したレコードバッチを使い、オフセット互換処理には行単位のスキャンではなく行グループのメタデータを使用します。
- `src/local_data_studio/server/stats.py` は互換性のための窓口として維持し、`src/local_data_studio/server/column_stats` で値の型推論、カラム単位の集計、DuckDB の処理制御を分離しています。サンプル行は固定サイズのバッチで取得し、行全体の行列とカラムのコピーを同時に保持せず、カラムごとの集計器へ直接渡します。
- SQL 実行は `src/local_data_studio/server/sql.py` に集約しています。読み取り専用 SQL の検証、DuckDB のリソース制限、バックグラウンドジョブの協調的なキャンセルを扱います。
- SQL 生成には、LiteLLM Python SDK を遅延読み込みするアダプター経由で使用します。`server/llm_profiles.py` はサーバー管理のモデルプロファイルを検証し、`server/llm_prompt.py` はプロバイダー共通の 1 件のユーザーメッセージ作成と生成 SQL の検証を行います。`server/llm_client.py` は共通の補完リクエストを担当し、`server/llm_service.py` はプロファイルの選択と処理全体の制御を担当します。プロバイダーから返されたエラー本文や認証情報は、API レスポンスへ含めません。
- EDA レポート全体の処理制御は `src/local_data_studio/server/eda_reports.py`、プロファイリング設定と DataFrame の安全な整形は `src/local_data_studio/server/eda.py` に分離しています。レポートは `./cache/eda` に分離して保存し、共通の容量管理によって古いファイルから削除します。
- `src/local_data_studio/server/atlas.py` は互換性のための窓口として維持し、`src/local_data_studio/server/atlas_components` に、入出力の取り決め、モデルの対応状況に応じて処理を選ぶ埋め込みアダプター、安全なプロンプトテンプレート、画像変換、次元削減、データセットキャッシュ、ブラウザーから利用できるポートの割り当て、サブプロセス制御、処理全体の制御を分割しています。`server/embedder_capabilities.py` は、読み込み量に上限を設けたメタデータだけのモデル検査と、モデルの対応状況に関わる設定情報からキャッシュ判定用の識別値を作成する処理を担当します。エンコーダーは Atlas ジョブごとに 1 回だけ生成し、anchor と transform の各バッチで再利用します。全件方式と anchor 方式の埋め込みは上限付きのバッチへ分割し、各バッチ間で進捗更新とキャンセル確認を行います。`anchor_transform` では入力カラム全体を Python のリストへ変換せず、anchor と現在処理中の transform バッチだけを取得します。表示値の整形と 2 次元座標カラムの追加は、処理専用に作成した 1 つの DataFrame コピー上で行います。同じデータセット、クエリ、カラム、モデル、バックエンド、プロンプト、設定に対して同時にキャッシュが要求された場合は、1 回のキャッシュ生成を共有し、その待機中もキャンセルできます。
- Atlas の UMAP による次元削減では、キャッシュ結果を再現できるように乱数シードを固定します。また、乱数シードを固定した UMAP の実行方式に合わせて `n_jobs=1` を明示し、スレッド数の上書きに関する警告が表示されないようにしています。
- macOS では、子プロセス側の fork による `SIGSEGV (-11)` を避けるため、Atlas のサブプロセス起動が Python の `posix_spawn` 経路を使用する形に固定しています。Atlas コマンドには絶対パスを使用し、`Popen` へ `cwd` を渡さず、`close_fds=False` を維持してください。
- Atlas のポートは、サブプロセス起動の直前に選択します。`atlas_components/ports.py` で Chromium が禁止するポートと使用中のポートを除外し、子プロセスは `--no-auto-port` で起動します。選択したループバックアドレスが TCP 接続を受け付けた後にだけ URL を返します。これらは `ERR_UNSAFE_PORT`、ポート競合、Uvicorn の待受開始前にリンクが表示される問題を防ぐための制約です。
- バックグラウンドジョブは `src/local_data_studio/server/jobs.py` で管理します。`/api/jobs/*` を通して、進捗、キャンセル、結果、エラー状態を確認できます。

開発時には、Uvicorn を使ってアプリケーション本体を直接起動できます。Uvicorn は ASGI サーバーであり、ASGI は Python の Web サーバーと Web アプリケーションを接続するための共通仕様です。ここでいう ASGI アプリケーションは、`local_data_studio.app:app` で指定する Local Data Studio のアプリケーション本体を指します。通常の利用では、この用語を意識する必要はありません。

直接起動時にも同じプロジェクト設定を使うため、シェルで `LOCAL_DATA_STUDIO_CONFIG_FILE` を指定してから起動します。

```bash
LOCAL_DATA_STUDIO_CONFIG_FILE=./local_data_studio.toml \
  uv run uvicorn local_data_studio.app:app --reload
```
