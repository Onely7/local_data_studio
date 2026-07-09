# Embedding Atlas SIGSEGV 障害ログ

## 発生日

2026-07-09

## 対象

- `src/local_data_studio/server/atlas.py`
- Visualize Embedding の `Run Atlas` / `Run Atlas on Query Results`
- macOS 上で `uvicorn local_data_studio.app:app --reload` や `local-data-studio --reload` から起動される Embedding Atlas subprocess

## 現象

どのデータセットに対しても `Run Atlas` を実行すると、Embedding Atlas の URL が生成される前に以下のエラーで失敗した。

```text
500: embedding-atlas exited before producing a URL: SIGSEGV (-11)
```

`example.jsonl` のような画像 URL のみを含む軽量データでも発生したため、特定データセットや Parquet binary object だけの問題ではなかった。

## 切り分け結果

- 同じ cache parquet を端末から直接 `embedding_atlas.cli` に渡すと URL 生成に成功した。
- `ThreadPoolExecutor` から直接 `launch_embedding_atlas()` を呼ぶ単体スクリプトでも成功した。
- FastAPI の background job 経由では即時に SIGSEGV が再現した。
- macOS のクラッシュレポートに以下が記録されていた。

```text
*** multi-threaded process forked ***
crashed on child side of fork pre-exec
```

このため、原因は Embedding Atlas のデータ読み込みや projection ではなく、macOS 上のマルチスレッドプロセスから `subprocess.Popen` が fork 経路に入ることだった。

## 原因

Python 3.12 の `subprocess.Popen` は条件を満たす場合に `posix_spawn` を使うが、条件を満たさない場合は `fork/exec` 経路になる。

当時の `launch_embedding_atlas()` は以下の指定により `posix_spawn` 条件を満たしていなかった。

- `cwd=str(BASE_DIR)` を指定していた。
- デフォルトの `close_fds=True` のままだった。

uvicorn の reload worker はマルチスレッド状態で動作するため、macOS の system library の atfork handler が child side pre-exec でクラッシュし、`SIGSEGV (-11)` になった。

## 対応策

`src/local_data_studio/server/atlas.py` で Embedding Atlas subprocess を `posix_spawn` 互換の条件で起動するように変更した。

- Atlas CLI に渡す dataset path を絶対パス化する。
- `Popen` から `cwd` 指定を外す。
- `Popen(..., close_fds=False)` を指定する。

これにより Python 3.12 の `posix_spawn` 経路を使えるようになり、macOS の multi-threaded fork pre-exec crash を回避した。

## 再発防止

以下の回帰テストを追加した。

- `launch_embedding_atlas()` が `cwd` を渡さないこと。
- `launch_embedding_atlas()` が `close_fds=False` で `Popen` を呼ぶこと。
- `build_atlas_command()` が dataset path を絶対パスとして渡すこと。

今後 Atlas 起動処理を変更する場合は、`subprocess.Popen` の呼び出しが Python の `posix_spawn` 条件を壊していないか確認すること。

## 確認コマンド

```bash
uv run pytest
uv run ruff check
uv run ty check
```

API 経由の確認例:

```bash
curl -s -X POST http://127.0.0.1:8000/api/jobs/atlas \
  -H 'Content-Type: application/json' \
  -d '{"file":"example.jsonl","column":"image","model":"facebook/dinov3-vitl16-pretrain-lvd1689m"}'
```

返却された job id に対して以下を実行し、`status` が `succeeded` になり `result.url` が返ることを確認する。

```bash
curl -s http://127.0.0.1:8000/api/jobs/<job_id>
```

## 注意点

- `SIGSEGV (-11)` と表示されても、必ずしも Embedding Atlas の projection や入力データが原因とは限らない。
- macOS ではマルチスレッドプロセスからの `fork` は pre-exec crash の原因になり得る。
- `cwd`, `close_fds`, `pass_fds`, `preexec_fn`, `start_new_session` などは `posix_spawn` 使用条件に影響するため、Atlas subprocess の起動コードでは慎重に扱う。
