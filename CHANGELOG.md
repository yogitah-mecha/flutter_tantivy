# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed (breaking)
- **Music schema**: the index schema is now a music-track schema — `path` (`STRING | STORED | FAST`, primary key), `uid`, `title`/`artist`/`album` (`TEXT | STORED`), `title_sort`/`artist_sort` (`STRING | FAST`), `album_key` (facet `/artist/album`), `duration_ms` (`U64 | STORED | FAST`), `mtime`/`size`/`last_scanned_at` (`U64 | FAST`), `artwork_base64` (`STORED` only: cover art as base64, not indexed because image data can exceed Tantivy's maximum term length). `Document` carries these fields (`id`/`text` removed); `title_sort`, `artist_sort` and `album_key` are derived on write.
- Keys are paths: `getDocumentById` → `getDocumentByPath`; `deleteDocument`, `deleteDocumentsBatch`, `deleteDocumentNoCommit` and `searchSimilarDocuments` take paths.
- Search APIs search `title`, `artist` and `album`; `SearchResult.titleSnippet` is replaced by `artistSnippet`/`albumSnippet` (`snippet`, `fragmentRange`, `highlightedRanges` refer to `title`).
- Indexes created with the old schema must be deleted (opening one reports which field is missing).

### Added
- `listDocuments` (paged, ordered by `title_sort` or `artist_sort`), `getDocumentsByPaths`, `getScanStates` (path/mtime/size/last_scanned_at from fast fields), `listAlbums` / `listAlbumDocuments` (`album_key` facet), `searchDocumentsPrefix` (search as you type with typo fallback).
- flutter-elinux support (`elinux` platform, `elinux-x64` / `elinux-arm64` cargokit targets).

## [2026.10.17] - 2026-10-17

### Added
- **Fuzzy Search (`searchDocumentsFuzzy`)**: Added typo-tolerant search powered by Tantivy's `FuzzyTermQuery` and Levenshtein automaton (`#3136`), supporting configurable edit distance (`0..=2`), transposition cost, and prefix mode across `title` and `text` fields.
- **Regex Phrase Search (`searchDocumentsRegexPhrase`)**: Ported Tantivy's `RegexPhraseQuery` enhancements (`#3135`, `#3137`, `#3138`) to search sequences of regex patterns in order with configurable proximity `slop` and `maxExpansions`.
- **More-Like-This Similar Document Search (`searchSimilarDocuments`)**: Added `MoreLikeThisQuery` support to find documents similar to a reference document ID with customizable document frequency, term frequency, word length bounds, and boost factor.
- **Exists Query & `field:*` Syntax (`searchDocumentsExists`)**: Ported Tantivy's `ExistsQuery` and query parser `field:*` syntax (`#2974`, `#3068`) with fast-field columnar storage enabled by default on `id`, `title`, and `text`.
- **Lenient Query Parsing (`searchDocumentsLenient`)**: Added `QueryParser::parse_query_lenient` support (`#2905`, `#2960`, `#3100`) returning both matching results and non-fatal syntax warnings (`LenientSearchResponse`), with optional `conjunctionByDefault` (AND mode) and `titleBoost`.
- **Term Dictionary Prefix Suggestions (`suggestTerms`)**: Ported FST/SSTable term dictionary range streaming (`#3155`) to provide prefix autocomplete suggestions and document frequency counts (`TermSuggestion`).
- **Snippet Fragment Byte Range & Highlight Ranges (`fragmentRange`, `highlightedRanges`, `titleSnippet`)**: Ported Tantivy `#3134` (`Snippet::fragment_range`) and `Snippet::highlighted` to `SearchResult`, plus optional highlighted `titleSnippet`.
- **Index Space Usage & Segment File Listing (`getIndexSpaceUsage`, `listIndexSegmentFiles`)**: Ported `Searcher::space_usage()` (`IndexSpaceUsage`) and `IndexMeta::list_segment_files()` (`#3080`) for inspecting storage breakdown and active segment files.
- **Multi-Language Stemmer & Extended Tokenizers**: Added support for `"whitespace"`, `"raw"`, and 18-language stemming (`"en_stem"` / `"stemmer"` / `"stem:<language>"` for English, French, German, Spanish, Italian, Portuguese, Russian, Arabic, Danish, Dutch, Finnish, Greek, Hungarian, Norwegian, Romanian, Swedish, Tamil, Turkish) alongside `"default"` and `"cjk"` / `"ngram"`.
- **AI Agent Skills (`skills/flutter-tantivy`)**: Added AI agent skill definitions (`SKILL.md`, `references/api-reference.md`, `examples/search_repository.dart`), auto-discovery links for **Claude Code** (`.claude/skills/flutter-tantivy`, `CLAUDE.md`), **OpenAI Codex** (`.agents/skills/flutter-tantivy`, `.codex/skills/flutter-tantivy`, `AGENTS.md`), and **Gemini CLI / Google Antigravity** (`.agents/skills/flutter-tantivy`, `GEMINI.md`), plus a usage guide in `README.md`.

### Changed
- **Tantivy Upgrade**: Upgraded Tantivy engine from `0.26.1` to **`0.26.2`** (includes aggregation count overflow fixes, nested aggregation buffer flush fixes `#2992`, and buffered union `seek_danger` safety fixes `#3086`).
- **Flutter SDK & Package Dependencies Upgrade**: Upgraded minimum SDK constraints (`sdk: ^3.13.5`, `flutter: '>=3.47.0'`), `flutter_rust_bridge` to **`2.13.0`**, `ffi` to `^2.2.0`, `ffigen` to `^23.0.0`, `cupertino_icons` to `^2.0.0`, and `path_provider` to `^2.1.6`.
- **Tokenizer Registration & Re-initialization**: Automatically registers all built-in and custom tokenizers when opening existing indexes and supports re-initializing `initTantivy` when `dirPath` or `tokenizerType` changes.

---

## [2026.7.26] - 2026-07-26

### Added
- **Regex Search Support (`searchDocumentsRegex`)**: Added support for searching documents using regular expression patterns (`RegexQuery`). (Addresses Issue #1)
- **CJK / Multi-language Tokenizer (`tokenizerType: 'cjk'`)**: Added N-gram tokenizer option during `initTantivy` for Chinese, Japanese, and Korean (CJK) text tokenization and substring search. (Addresses Issues #2, #3, #4)
- **Search Snippets & Highlighting (`snippet`)**: Added search term highlighting using Tantivy's `SnippetGenerator`. Returns HTML-formatted snippets (e.g. `<b>Flutter</b> is a UI...`).
- **Pagination Support (`offset`)**: Added `offset` parameter to `searchDocuments` for paginated search results.
- **Total Hits Count (`totalHits`)**: `searchDocuments` now returns a `SearchResponse` containing total matching documents count (`totalHits`) and page results.
- **Document Title Support (`title`)**: Added optional `title` field to `Document`. Searching matches across both `title` and `text` fields.
- **Get Total Document Count (`getNumDocs()`)**: Added synchronous function to retrieve total number of indexed documents.
- **Delete All Documents (`deleteAllDocuments()`)**: Added function to clear all documents from the search index.
- **Close Tantivy (`closeTantivy()`)**: Added function to safely close and reset Tantivy resources.

### Changed
- **Date-Based Versioning (CalVer)**: Switched package versioning strategy to date-based versioning (`2026.7.26`).
- **Tantivy Upgrade**: Upgraded Tantivy engine from `0.25.0` to **`0.26.1`**.
- **flutter_rust_bridge Upgrade**: Upgraded `flutter_rust_bridge` from `2.11.1` to **`2.12.0`**.

---

## [0.1.0] - 2025-11-03

### Added
- Initial release of flutter_tantivy plugin
- Full-text search powered by Tantivy search engine
- Complete CRUD operations for document management
  - `addDocument()` - Add single document with auto-commit
  - `getDocumentById()` - Retrieve document by ID (synchronous)
  - `updateDocument()` - Update existing document
  - `deleteDocument()` - Delete document by ID
- Batch operations for improved performance
  - `addDocumentsBatch()` - Add multiple documents efficiently
  - `deleteDocumentsBatch()` - Delete multiple documents efficiently
- Advanced transaction control
  - `addDocumentNoCommit()` - Add document without committing
  - `deleteDocumentNoCommit()` - Delete document without committing
  - `commit()` - Manually commit pending changes
- Search functionality with relevance scoring
  - `searchDocuments()` - Full-text search with query parsing
  - Support for boolean operators (AND, OR, NOT)
  - Phrase search and wildcard support
- Index management
  - `initTantivy()` - Initialize or open persistent search index
  - Automatic index reload on commits
  - Thread-safe concurrent access
- Cross-platform support
  - Android (API 21+)
  - iOS (11.0+)
  - macOS (10.11+)
  - Linux
  - Windows
