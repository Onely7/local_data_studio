"""Characterization tests for stable public runtime contracts."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from html.parser import HTMLParser
from pathlib import Path
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
    "translation-model",
    "translation-language",
    "translation-cancel",
    "translation-status",
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
    "json-translate-action",
    "json-body",
    "json-translation-result",
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
    "translation-confirm-overlay",
    "translation-confirm-title",
    "translation-confirm-message",
    "translation-confirm-cancel",
    "translation-confirm-run",
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
        self.assertEqual(["styles.css?v=20260714-code-view-actions"], collector.stylesheets)
        self.assertEqual(["app.js?v=20260714-controls"], collector.scripts)

    def test_static_entrypoint_loads_application_as_an_es_module(self) -> None:
        """Keep the stable app URL while implementation modules remain package assets."""
        with TestClient(app) as client:
            page = client.get("/").text
            entrypoint = client.get("/app.js")
            application = client.get("/app/application.js")
            select_controls = client.get("/app/selects.js")
            stylesheet = client.get("/styles.css")
            translation_icon = client.get("/icons/translation.svg")
            code_icon = client.get("/icons/code.svg")
            copy_icon = client.get("/icons/content-copy.svg")
            delete_icon = client.get("/icons/delete.svg")
            send_icon = client.get("/icons/send.svg")
            favicon = client.get("/favicon.svg")

        self.assertIn('<link rel="stylesheet" href="styles.css?v=20260714-code-view-actions" />', page)
        self.assertIn('<script type="module" src="app.js?v=20260714-controls"></script>', page)
        self.assertEqual('import "./app/application.js";\n', entrypoint.text)
        self.assertEqual(200, application.status_code)
        self.assertEqual(200, select_controls.status_code)
        self.assertIn('from "./state.js"', application.text)
        self.assertIn("export function enhanceSelectControls", select_controls.text)
        self.assertIn('classList.toggle("has-more-options"', select_controls.text)
        self.assertNotIn('document.createElement("style")', application.text)
        self.assertIn(".info-grid {", stylesheet.text)
        self.assertIn(".translation-result-header .expanded-copy-btn", stylesheet.text)
        self.assertIn(".json-actions .icon-action-btn", stylesheet.text)
        self.assertIn("#json-translate-action", stylesheet.text)
        self.assertIn("padding: 10px 12px 0;", stylesheet.text)
        self.assertIn(".json-actions .field-translate-btn", stylesheet.text)
        self.assertIn(".sidebar {\n    order: 1;", stylesheet.text)
        self.assertIn(".main {\n    order: 2;", stylesheet.text)
        self.assertIn(".inspector {\n    order: 3;", stylesheet.text)
        self.assertEqual("image/svg+xml", translation_icon.headers["content-type"])
        for response in (code_icon, copy_icon, delete_icon, send_icon):
            self.assertEqual(200, response.status_code)
            self.assertEqual("image/svg+xml", response.headers["content-type"])
        self.assertEqual("image/svg+xml", favicon.headers["content-type"])

    def test_static_modules_declare_cross_module_dependencies(self) -> None:
        """Prevent non-empty model data and deferred UI actions from using missing globals."""
        static_app = Path(__file__).parents[1] / "src" / "local_data_studio" / "static" / "app"
        application = (static_app / "application.js").read_text(encoding="utf-8")
        atlas = (static_app / "atlas.js").read_text(encoding="utf-8")
        images = (static_app / "images.js").read_text(encoding="utf-8")
        llm = (static_app / "llm.js").read_text(encoding="utf-8")
        translation = (static_app / "translation.js").read_text(encoding="utf-8")

        self.assertIn('import { escapeHtml } from "./formatting.js";', atlas)
        self.assertIn('import { escapeHtml } from "./formatting.js";', llm)
        self.assertIn("export function imageCandidate", images)
        self.assertIn("export function openAtlasUrl", atlas)
        self.assertIn("imageCandidate,", application)
        self.assertIn("openAtlasUrl,", application)
        self.assertIn('from "./translation.js"', application)
        self.assertIn('startJob("translation"', translation)
        self.assertIn('translationConfirmOverlay.classList.add("active")', translation)
        self.assertNotIn('translationConfirmOverlay.classList.add("open")', translation)

    def test_translation_client_classifies_visible_values_without_source_fetches(self) -> None:
        """Keep translation input browser-local and exclude obvious machine values."""
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is unavailable")
        source_app = Path(__file__).parents[1] / "src" / "local_data_studio" / "static" / "app"
        with tempfile.TemporaryDirectory() as temporary_directory:
            static_app = Path(temporary_directory) / "app"
            shutil.copytree(source_app, static_app)
            module = static_app / "translation.js"
            script = f"""
globalThis.document = {{ getElementById: () => null, querySelector: () => null, querySelectorAll: () => [] }};
globalThis.window = {{ location: {{ origin: "http://127.0.0.1:8000" }} }};
const translation = await import({str(module.as_uri())!r});
const values = translation.collectTranslatableStrings({{
  title: "Hello world",
  label: "cat",
  image: "images/example.png",
  audio: "audio/example.mp3",
  url: "https://example.com",
  numericText: "123.45",
  numericObject: {{ count: 10, score: "42" }},
  binaryObject: {{ bytes: "iVBORw0KGgoAAAANSUhEUg", path: "image.png" }},
  booleanValue: true,
}});
if (JSON.stringify(values) !== JSON.stringify(["Hello world", "cat"])) throw new Error(`unexpected translation values: ${{JSON.stringify(values)}}`);
if (translation.hasTranslatableText([1, "2", false])) throw new Error("numeric-only list must not be translatable");
if (!translation.hasTranslatableText({{ status: "error" }})) throw new Error("ordinary short words must remain translatable");
"""
            subprocess.run([node, "--experimental-default-type=module", "--input-type=module", "--eval", script], check=True)

    def test_static_model_renderers_run_with_non_empty_models(self) -> None:
        """Render configured LLM and embedder models without relying on legacy globals."""
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is unavailable")
        source_app = Path(__file__).parents[1] / "src" / "local_data_studio" / "static" / "app"
        with tempfile.TemporaryDirectory() as temporary_directory:
            static_app = Path(temporary_directory) / "app"
            shutil.copytree(source_app, static_app)
            script = f"""
globalThis.document = {{ getElementById: () => null, querySelector: () => null }};
globalThis.window = {{ location: {{ origin: "http://127.0.0.1:8000", protocol: "http:", href: "http://127.0.0.1:8000/" }} }};
const {{ elements }} = await import({str((static_app / "dom.js").as_uri())!r});
const {{ state }} = await import({str((static_app / "state.js").as_uri())!r});
const atlas = await import({str((static_app / "atlas.js").as_uri())!r});
const images = await import({str((static_app / "images.js").as_uri())!r});
const llm = await import({str((static_app / "llm.js").as_uri())!r});
const classList = {{ toggle: () => {{}}, add: () => {{}}, remove: () => {{}} }};
const element = (value = "") => ({{ value, innerHTML: "", textContent: "", title: "", disabled: false, classList, style: {{}} }});
elements.atlasColumn = element("image");
elements.atlasModel = element("model-id");
elements.atlasBackend = element("transformers");
elements.atlasPromptControls = element();
elements.atlasPrompt = element();
elements.runAtlas = element();
elements.runAtlasQuery = element();
state.embedderModels = [{{
  value: "model-id",
  name: "Model <One>",
  default_backend: "transformers",
  backends: {{
    "sentence-transformers": {{ available: false, status: "unsupported", reason: "" }},
    transformers: {{ available: true, status: "direct", reason: "" }},
  }},
}}];
atlas.renderAtlasModelOptions();
if (!elements.atlasModel.innerHTML.includes("Model &lt;One&gt;")) throw new Error("embedder model rendering failed");
if (images.imageCandidate("image.png", "image.png").src !== "image.png") throw new Error("image candidate export failed");
state.atlasUrl = "/atlas/runtime-test/";
atlas.openAtlasUrl();
if (window.location.href !== "http://127.0.0.1:8000/atlas/runtime-test/") throw new Error("Atlas URL export failed");
elements.nlModel = element();
elements.nlGenerate = element();
elements.nlStatus = element();
globalThis.fetch = async () => ({{
  ok: true,
  json: async () => ({{ models: [{{ id: "llm-id", label: "LLM <One>", available: true }}], default_model: "llm-id" }}),
}});
await llm.loadLlmModels();
if (!elements.nlModel.innerHTML.includes("LLM &lt;One&gt;")) throw new Error("LLM model rendering failed");
"""
            subprocess.run(
                [node, "--experimental-default-type=module", "--input-type=module", "--eval", script],
                check=True,
                cwd=static_app,
            )

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
