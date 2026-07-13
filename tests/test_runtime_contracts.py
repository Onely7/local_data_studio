"""Characterization tests for stable public runtime contracts."""

from __future__ import annotations

from html.parser import HTMLParser
from unittest import TestCase

from fastapi.testclient import TestClient

from local_data_studio.app import app
from local_data_studio.server import atlas, readers


class _ElementCollector(HTMLParser):
    """Collect ordered element identifiers from packaged HTML."""

    def __init__(self) -> None:
        """Create an empty element collector."""
        super().__init__()
        self.ids: list[str] = []
        self.scripts: list[str] = []
        self.stylesheets: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Record stable static asset and DOM identifiers."""
        attributes = dict(attrs)
        if element_id := attributes.get("id"):
            self.ids.append(element_id)
        if tag == "script" and (source := attributes.get("src")):
            self.scripts.append(source)
        if tag == "link" and attributes.get("rel") == "stylesheet" and (source := attributes.get("href")):
            self.stylesheets.append(source)


EXPECTED_STATIC_IDS = (
    "current-file",
    "refresh-files",
    "dataset-search",
    "dataset-empty",
    "file-list",
    "drop-hint",
    "file-empty",
    "search-input",
    "search-btn",
    "clear-search",
    "page-size",
    "prev-page",
    "page-info",
    "next-page",
    "view-label",
    "dataset-meta",
    "data-table",
    "nl-model",
    "nl-input",
    "nl-generate",
    "nl-status",
    "sql-input",
    "run-query",
    "reset-view",
    "count-rows",
    "run-eda",
    "run-eda-query",
    "eda-profile-mode",
    "row-count",
    "eda-status",
    "eda-link",
    "atlas-column",
    "atlas-model",
    "atlas-backend",
    "atlas-projection",
    "atlas-prompt-controls",
    "atlas-prompt",
    "run-atlas",
    "run-atlas-query",
    "atlas-status",
    "atlas-link",
    "copy-row",
    "row-inspector-raw",
    "delete-row",
    "row-inspector",
    "image-overlay",
    "overlay-close",
    "overlay-image-label",
    "overlay-image",
    "overlay-title",
    "overlay-fields",
    "overlay-nav",
    "overlay-prev",
    "overlay-index",
    "overlay-next",
    "json-overlay",
    "json-title",
    "json-close",
    "copy-json",
    "json-body",
    "delete-overlay",
    "delete-message",
    "delete-cancel",
    "delete-soft",
    "delete-hard",
    "column-delete-overlay",
    "column-delete-message",
    "column-delete-cancel",
    "column-delete-soft",
    "column-delete-hard",
    "error-overlay",
    "error-message",
    "error-ok",
)


class RuntimeContractTests(TestCase):
    """Keep public imports and packaged UI wiring stable across refactors."""

    def test_packaged_ui_keeps_dom_order_and_stable_asset_urls(self) -> None:
        """Keep the existing DOM contract while client code moves into modules."""
        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(200, response.status_code)
        collector = _ElementCollector()
        collector.feed(response.text)

        self.assertEqual(EXPECTED_STATIC_IDS, tuple(collector.ids))
        self.assertEqual(["styles.css?v=20260712-litellm-models"], collector.stylesheets)
        self.assertEqual(["app.js?v=20260712-litellm-models"], collector.scripts)

    def test_static_entrypoint_loads_application_as_an_es_module(self) -> None:
        """Keep the stable app URL while implementation modules remain package assets."""
        with TestClient(app) as client:
            page = client.get("/").text
            entrypoint = client.get("/app.js")
            application = client.get("/app/application.js")
            stylesheet = client.get("/styles.css")

        self.assertIn('<script type="module" src="app.js?v=20260712-litellm-models"></script>', page)
        self.assertEqual('import "./app/application.js";\n', entrypoint.text)
        self.assertEqual(200, application.status_code)
        self.assertIn('from "./state.js"', application.text)
        self.assertNotIn('document.createElement("style")', application.text)
        self.assertIn(".info-grid {", stylesheet.text)

    def test_reader_and_atlas_facades_keep_supported_public_callables(self) -> None:
        """Keep imports used by API routes and extension code available."""
        for name in (
            "load_dataset_metadata",
            "fetch_preview_page",
            "fetch_raw_row",
            "search_dataset",
            "count_rows_with_progress",
            "build_line_index_with_progress",
        ):
            with self.subTest(name=name):
                self.assertTrue(callable(getattr(readers, name)))

        for name in ("discover_embedder_models", "resolve_embedder_model", "run_atlas_visualization"):
            with self.subTest(name=name):
                self.assertTrue(callable(getattr(atlas, name)))
