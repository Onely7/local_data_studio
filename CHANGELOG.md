# Changelog

Notable changes to Local Data Studio are recorded here. Release Please updates
this file from Conventional Commits when it prepares a release pull request.

## [2.0.0](https://github.com/Onely7/local_data_studio/compare/v1.1.1...v2.0.0) (2026-11-02)


### ⚠ BREAKING CHANGES

* **index:** Jumps past the row index return HTTP 409 with the detail {"code": "row_index_required", "row": ..., "message": ...} instead of HTTP 400, and build_line_index_with_progress reports indexed_rows and complete alongside row_count, which is None until the index is complete.
* **preview:** resolve_raw_image_file is renamed to resolve_raw_media_file, and preview responses carry audio markers instead of hex strings for audio bytes.
* **deps:** Sentence Transformers 6.2+, Transformers 5.19+, Pillow 13+, and LiteLLM 1.100.4+ are now required.
* **sql:** guard_select_sql_for_dataset was removed; synchronous callers of execute_query_guarded still receive HTTP 400 for full scans of large datasets.
* **preview:** image bytes in preview responses are thumbnails rather than the original bytes, and raw rows return image bytes as data URLs instead of hex strings.
* **readers:** Parquet and search page tokens use a new format, search responses include next_page_token, and line rows more than 1M rows past a checkpoint require the row index.

### Features

* **atlas:** embed audio columns ([56c17ba](https://github.com/Onely7/local_data_studio/commit/56c17ba96bb07d1fcceaf36ed781673b408af1e4))
* **atlas:** support EmbeddingGemma 2 and multimodal ST 6 models ([6d1a5ef](https://github.com/Onely7/local_data_studio/commit/6d1a5ef2400f66c98d6b7d2645c95d343857fe0d))
* **index:** index rows in the background and resume interrupted scans ([858033f](https://github.com/Onely7/local_data_studio/commit/858033f7ba2fd24420f1af472dea8def643f9f59))
* **json:** stream JSON arrays and read JSON Lines saved as .json ([ca7376a](https://github.com/Onely7/local_data_studio/commit/ca7376a66b0ff19d5827e35ae07822c9a53d4678))
* **preview:** play audio in dataset cells ([6010bc8](https://github.com/Onely7/local_data_studio/commit/6010bc8d5061354e0a9ed4ed82fff83c39cc0d77))
* **sql:** run full-scan queries on large datasets in the background ([be78154](https://github.com/Onely7/local_data_studio/commit/be78154f50f953d8de4d232a1e77b0d1e5e476da))
* **transcription:** transcribe audio cells with a configurable speech model ([0f0b332](https://github.com/Onely7/local_data_studio/commit/0f0b33221dc52e1bcc05ce0641c5bcf0220379fd))
* **ui:** jump to any row and page through search results ([3dd9ff9](https://github.com/Onely7/local_data_studio/commit/3dd9ff9cdbdec0bcc8b2386767982c3a00a3f739))
* **ui:** show row totals and index far jumps with progress ([7b5389d](https://github.com/Onely7/local_data_studio/commit/7b5389d1a33a56f4f9dfe6c00a57d15577bd19c7))
* support Python 3.14 ([7678336](https://github.com/Onely7/local_data_studio/commit/7678336d2df53ba67b331a7a69e61f585b132ded))


### Bug Fixes

* **atlas:** sample large datasets instead of loading every row ([6ba0907](https://github.com/Onely7/local_data_studio/commit/6ba0907499ac1967249ea95c89aefd09c53b8e67))
* **csv:** keep quoted line breaks inside one row ([fbc8f48](https://github.com/Onely7/local_data_studio/commit/fbc8f483a9bfb18b2f64af65e735ffd318767df7))
* **readers:** scan small line files to the end instead of rejecting jumps ([ad346ef](https://github.com/Onely7/local_data_studio/commit/ad346efa6a64d5cf450e7fb310bcf5a967cda07f))
* **ui:** keep pagination and panel buttons readable on narrow screens ([23f4ca2](https://github.com/Onely7/local_data_studio/commit/23f4ca28fa34acba254478eff6fc7397b99b8285))


### Performance Improvements

* **preview:** convert text-only pages without the thread pool ([0a2bb3c](https://github.com/Onely7/local_data_studio/commit/0a2bb3c983b02063ec64754ec1d71eeb063d1ae7))
* **preview:** send image thumbnails instead of full image bytes ([05eabcd](https://github.com/Onely7/local_data_studio/commit/05eabcdac895c87fd8a7dbaee9f0547183c70ab6))
* **readers:** read pages, jumps, and visible rows without rereading files ([be8e3ec](https://github.com/Onely7/local_data_studio/commit/be8e3ec7d4e99958218f444fdae46a7cdecb89ed))
* **readers:** scan, jump, and search large datasets in bounded time ([fef4c6f](https://github.com/Onely7/local_data_studio/commit/fef4c6fcc6c195c3ada2bcfaa23926b3973bcd70))


### Dependencies

* **deps:** bump astral-sh/setup-uv in the github-actions group ([#53](https://github.com/Onely7/local_data_studio/issues/53)) ([96d74c5](https://github.com/Onely7/local_data_studio/commit/96d74c55310d95aed8b054027cf4065659b5f146))
* remove the unused polars dependency ([d163e9e](https://github.com/Onely7/local_data_studio/commit/d163e9ec890197aa8ef7df09fba55f4f13704257))
* replace ydata-profiling with fg-data-profiling ([ab39eca](https://github.com/Onely7/local_data_studio/commit/ab39eca11bcbb77d3023629037d38e9f0924dd68))
* upgrade numpy, scipy, matplotlib and numba ([c4c4411](https://github.com/Onely7/local_data_studio/commit/c4c44118a9136208c15608ac6631f2bb9cbdbd83))
* upgrade pandas to 2.3.3 ([a0d645e](https://github.com/Onely7/local_data_studio/commit/a0d645e05d1622c5764441b61ee9971aab573f87))


### Documentation

* describe audio playback, transcription, and audio embeddings ([9bcb80d](https://github.com/Onely7/local_data_studio/commit/9bcb80da8d8ee6339f3a1a0cf7963a199eafe683))
* describe background indexing and large-file reading ([96d9924](https://github.com/Onely7/local_data_studio/commit/96d9924b68f1e549f65e97761ea840c195a2b266))
* describe large-dataset navigation, background SQL, and EmbeddingGemma 2 ([1cde4c2](https://github.com/Onely7/local_data_studio/commit/1cde4c26bb83d2f6a262b74c708787767cdca3ac))


### Build System

* **deps:** upgrade dependencies and resolve security advisories ([1b40f87](https://github.com/Onely7/local_data_studio/commit/1b40f87cabf8837102e20375c5d134ffb9f59537))

## [1.1.1](https://github.com/Onely7/local_data_studio/compare/v1.1.0...v1.1.1) (2026-07-23)


### Bug Fixes

* preserve compatibility in dependency updates ([caf5beb](https://github.com/Onely7/local_data_studio/commit/caf5bebafbb3da47432b27bef5caf78304328f76))


### Dependencies

* **deps:** bump https://github.com/astral-sh/ty-pre-commit ([9fa50ca](https://github.com/Onely7/local_data_studio/commit/9fa50caefd27154addf2cfc9d71afd7851242ba4))
* **deps:** bump the github-actions group with 2 updates ([6faf4ab](https://github.com/Onely7/local_data_studio/commit/6faf4ab65668734ae0ce5c247a50aeaef19e29cf))
* **deps:** bump the python-dependencies group with 4 updates ([2e5a599](https://github.com/Onely7/local_data_studio/commit/2e5a59918bb4c8666b3caf11107007298174f66c))
* **deps:** bump the python-dependencies group with 9 updates ([c7a47e5](https://github.com/Onely7/local_data_studio/commit/c7a47e50df31b5285fc669a4fe8a2a2bfceb4904))

## [1.1.0](https://github.com/Onely7/local_data_studio/compare/v1.0.0...v1.1.0) (2026-07-15)

### Translation

- Added manual translation for a cell or the currently visible values in a
  Preview column. Translation-enabled LiteLLM profiles are selected separately
  from SQL-generation profiles, and source datasets are never modified.
  ([3b5d102](https://github.com/Onely7/local_data_studio/commit/3b5d102500f8a250c0c5ca60525f403cca0e7b59),
  [5537318](https://github.com/Onely7/local_data_studio/commit/5537318fae385a527230250ed073031b8a2614fa))
- Added 68 target languages and independent defaults for the translation model
  and target language. The target language can be set in
  `local_data_studio.toml`, with browser preferences used as fallbacks.
  ([943033d](https://github.com/Onely7/local_data_studio/commit/943033ddd0e93f21162db6ebbb03de13ef7c704f))
- Added bounded, cancellable background translation jobs. Server-side limits
  cover rows, strings, characters, chunk size, and provider concurrency;
  malformed provider responses are validated and retried once.
  ([3b5d102](https://github.com/Onely7/local_data_studio/commit/3b5d102500f8a250c0c5ca60525f403cca0e7b59))
- Preserved nested list and object structure while translating natural-language
  strings. Numeric, Boolean, binary, identifier, URL, image, and audio values
  are excluded, and completed translations remain only in browser memory.
  ([5537318](https://github.com/Onely7/local_data_studio/commit/5537318fae385a527230250ed073031b8a2614fa))

### Data Analysis

- Applied `EDA_ROW_LIMIT` to the dataset source before pandas DataFrame
  materialization when no rows are hidden in the current session. This avoids
  loading an entire large JSONL dataset before selecting the requested EDA
  rows. Unlimited mode (`-1`) continues to load all selected rows.
  ([7d799ed](https://github.com/Onely7/local_data_studio/commit/7d799ed1e99b9d969f2c6677ab7388d3889646f1))

### Interface

- Added translation controls and copy actions to expanded values and JSON code
  views, with shared JSON syntax coloring for structured values.
- Refined the dataset selector, search and pagination controls, responsive
  one-column ordering, mobile scrolling, and compact desktop toolbar so the
  Preview keeps as much space as possible.
- Updated the visual theme, bundled action icons, application branding, and
  screenshots while retaining the existing dataset, SQL, EDA, and Atlas
  workflows.
  ([0b4f5fd](https://github.com/Onely7/local_data_studio/commit/0b4f5fd1565d65105e6c147e365d7f05224e27ad),
  [40944f7](https://github.com/Onely7/local_data_studio/commit/40944f739aef7ee86b546efcada9441b573b0092),
  [cfa2b54](https://github.com/Onely7/local_data_studio/commit/cfa2b54b8d5edc46efe4a1eaea353a07f6fb972b),
  [f9690c5](https://github.com/Onely7/local_data_studio/commit/f9690c51a918f31d6cbb07eeaeae4a7aeb08bcae))

### Release And Documentation

- Added Release Please automation for version updates, changelog maintenance,
  release pull requests, tags, and GitHub Releases.
  ([4a70f10](https://github.com/Onely7/local_data_studio/commit/4a70f105308545e1cd9789b5972f1ff085cd86b0))
- Updated the English and Japanese user guides, implementation notes,
  configuration example, and screenshots for translation, EDA loading, and the
  refined workspace.
  ([f23e800](https://github.com/Onely7/local_data_studio/commit/f23e800722f16cf00ff758761afa6f20467092d7),
  [0a6e32c](https://github.com/Onely7/local_data_studio/commit/0a6e32c942cc7ee19e08bb27ae96def1b44a0ea),
  [b53a213](https://github.com/Onely7/local_data_studio/commit/b53a213fa35ce8981c85e8278532c2d400878fe8))

## [1.0.0](https://github.com/Onely7/local_data_studio/releases/tag/v1.0.0) (2026-07-13)

- Published the first stable Local Data Studio package to PyPI.
