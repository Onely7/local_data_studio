# Changelog

Notable changes to Local Data Studio are recorded here. Release Please updates
this file from Conventional Commits when it prepares a release pull request.

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
