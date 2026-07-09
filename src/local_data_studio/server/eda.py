"""EDA report generation utilities (zarque_profiling)."""

import datetime
import decimal
import hashlib
import importlib
import json
from pathlib import Path
from typing import Any, cast

from matplotlib import font_manager

from .config import (
    CACHE_DIR,
    EDA_CELL_MAX_CHARS,
    EDA_FONT_FAMILY,
    EDA_FONT_PATH,
    EDA_NESTED_POLICY,
)
from .db import open_connection, relation_with_rowid_sql


def _install_matplotlib_compat() -> None:
    """Provide small compatibility aliases expected by older profiling code."""
    try:
        from matplotlib import MatplotlibDeprecationWarning, cbook  # noqa: PLC0415

        if not hasattr(cbook, "mplDeprecation"):
            cast(Any, cbook).mplDeprecation = MatplotlibDeprecationWarning
    except Exception:
        pass


def _import_zarque_profile_report() -> Any:
    """
    zarque_profiling is still built against Pydantic v1 symbols.
    During its import, expose v1 symbols under pydantic.* so zarque's
    model classes do not become a broken mix of v1 BaseSettings and v2 BaseModel.
    Restore the public pydantic module afterwards to keep this app on Pydantic v2.
    """
    import pydantic  # noqa: PLC0415
    from pydantic import v1 as pydantic_v1  # noqa: PLC0415

    names = ("BaseModel", "BaseSettings", "Field", "PrivateAttr")
    original = {name: pydantic.__dict__.get(name) for name in names}
    for name in names:
        setattr(pydantic, name, getattr(pydantic_v1, name))

    try:
        from zarque_profiling import ProfileReport as ZarqueProfileReport  # noqa: PLC0415
    finally:
        for name, value in original.items():
            if value is None:
                try:
                    delattr(pydantic, name)
                except AttributeError:
                    pass
            else:
                setattr(pydantic, name, value)

    return ZarqueProfileReport


_install_matplotlib_compat()
ProfileReport = _import_zarque_profile_report()


def eda_cache_key(path: Path, sample_rows: int, mode: str) -> str:
    """Build a stable cache key based on file metadata and options."""
    stat = path.stat()
    payload = f"{path.resolve()}|{stat.st_size}|{stat.st_mtime}|{sample_rows}|{mode}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def eda_cache_path(path: Path, sample_rows: int, mode: str) -> Path:
    """Return the cache path for an EDA report."""
    key = eda_cache_key(path, sample_rows, mode)[:12]
    stem = path.stem or "data"
    safe_stem = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)
    return CACHE_DIR / f"{safe_stem}-{mode}-{key}.html"


def load_eda_dataframe_polars(path: Path, sample_rows: int, deleted_ids: list[int]) -> Any:
    """
    zarque_profiling は Polars 前提なので、最終的に polars.DataFrame を返す。
    duckdb 側が .pl() を提供していればそれを使い、なければ pandas 経由で変換する。
    """
    with open_connection() as con:
        rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
        query = f"SELECT * EXCLUDE(__rowid) FROM ({rel_sql}) LIMIT {sample_rows}"
        cursor = con.execute(query, params)

        if hasattr(cursor, "pl"):
            try:
                return cursor.pl()
            except Exception:
                pass

        df_pd = cursor.df()

    import polars as pl  # noqa: PLC0415

    return pl.from_pandas(df_pd)


def sanitize_eda_dataframe(df: Any) -> Any:
    """
    EDA レポート生成で壊れやすい値(list, dict, bytes, date, decimal など)を
    文字列化しておく。最終的に polars.DataFrame を返す。
    zarque_profiling が苦手なネスト型(List/Struct/Object/Binary 等)を
    profiling できるように Utf8 へ落とす。
    """
    import polars as pl  # noqa: PLC0415

    max_chars = EDA_CELL_MAX_CHARS
    nested_policy = EDA_NESTED_POLICY
    # nested_policy: "stringify" or "drop"

    def stringify(v: Any) -> str | None:
        if v is None:
            return None
        if isinstance(v, bytes):
            s = v.hex()
        elif isinstance(v, (datetime.date, datetime.datetime, decimal.Decimal)):
            s = str(v)
        elif isinstance(v, (list, tuple, dict)):
            try:
                s = json.dumps(v, ensure_ascii=False)
            except TypeError:
                s = str(v)
        else:
            s = str(v)

        if max_chars > 0 and len(s) > max_chars:
            s = s[:max_chars] + "... (truncated)"
        return s

    # pandas df が来た場合は最低限従来どおり stringify するしかないので、そのまま返す
    if not isinstance(df, pl.DataFrame):
        return df

    out = df
    drop_cols: list[str] = []

    for name, dtype in out.schema.items():
        dtype_str = str(dtype)

        is_listish = dtype_str.startswith("List") or dtype_str.startswith("Array")
        is_structish = dtype_str.startswith("Struct")
        is_objectish = (dtype == pl.Object) or dtype_str.startswith("Object")
        is_binary = (dtype == pl.Binary) or dtype_str.startswith("Binary")

        if is_listish or is_structish or is_objectish or is_binary:
            if nested_policy == "drop":
                drop_cols.append(name)
                continue

            vals = out.get_column(name).to_list()
            new_vals = [stringify(v) for v in vals]
            out = out.with_columns(pl.Series(name, new_vals, dtype=pl.Utf8))

    if drop_cols:
        out = out.drop(drop_cols)

    return out


def _materialize_resource_file(pkg: str, filename: str) -> Path | None:
    """
    importlib.resources から設定ファイルを読み出し、CACHE_DIR に書き出して Path を返す。
    ZIP 配布でも壊れないように "必ず実ファイル化" する。
    """
    try:
        resources = importlib.import_module("importlib.resources")
        root = resources.files(pkg)
        res = root.joinpath(filename)
        if not res.is_file():
            return None
        dst = CACHE_DIR / f"{pkg.replace('.', '_')}-{filename}"
        if not dst.exists():
            dst.write_bytes(res.read_bytes())
        return dst
    except Exception:
        return None


def _select_profile_config_file(minimal: bool) -> Path:
    """
    typeguard が config_file=None を許さない系のバグを回避するため、
    常に config_file を Path として返す。
    minimal=True 相当は config_minimal.yaml を config_file として渡す。
    """
    target_name = "config_minimal.yaml" if minimal else "config_default.yaml"

    # 1) まずは各パッケージが提供している get_config を試す
    for module_name in (
        "zarque_profiling.utils.paths",
        "ydata_profiling.utils.paths",
        "pandas_profiling.utils.paths",
    ):
        try:
            mod = importlib.import_module(module_name)
            get_config = getattr(mod, "get_config", None)
            if callable(get_config):
                cfg = get_config(target_name)
                # Path っぽいものが返る前提
                return Path(str(cfg))
        except Exception:
            continue

    # 2) importlib.resources で同梱 YAML を実ファイル化
    for pkg in ("zarque_profiling", "ydata_profiling", "pandas_profiling"):
        p = _materialize_resource_file(pkg, target_name)
        if p is not None:
            return p

    # 3) 最後の手段: 空の YAML を作る
    fallback = CACHE_DIR / target_name
    if not fallback.exists():
        fallback.write_text("{}", encoding="utf-8")
    return fallback


def build_eda_report(df: Any, title: str, minimal: bool) -> Any:
    """
    - config_file は常に明示する (None を使わない)
    - config_file と minimal は同時指定しない
      minimal=True は config_minimal.yaml を指定することで表現する
    """
    cfg_path = _select_profile_config_file(minimal=minimal)

    # ここでは minimal 引数を False を絶対に渡す
    kwargs = {
        "title": title,
        "minimal": False,
        "config_file": cfg_path,
    }
    font_family = resolve_eda_font_family()
    if font_family:
        kwargs["font_family"] = font_family
    return ProfileReport(df, **kwargs)


def resolve_eda_font_family() -> str | None:
    """Resolve a font family name using the optional font path fallback."""
    if EDA_FONT_PATH:
        font_path = Path(EDA_FONT_PATH)
        try:
            cast(Any, font_manager.fontManager).addfont(str(font_path))
            font_properties = cast(Any, font_manager.FontProperties)(fname=str(font_path))
            return str(font_properties.get_name())
        except Exception:
            if EDA_FONT_FAMILY:
                return EDA_FONT_FAMILY
            return None
    return EDA_FONT_FAMILY
