use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tantivy::collector::{Count, FacetCollector, TopDocs};
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, ExistsQuery, FuzzyTermQuery, MoreLikeThisQuery, Occur,
    Query, QueryParser, RegexPhraseQuery, RegexQuery, TermQuery,
};
use tantivy::schema::*;
use tantivy::snippet::{Snippet, SnippetGenerator};
use tantivy::tokenizer::{
    Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer,
    TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::{
    DocAddress, Index, IndexReader, IndexWriter, Order, ReloadPolicy, Searcher, TantivyDocument,
    Term,
};

/// A music track: one document of the index.
///
/// Schema:
///
/// | field             | options                  | use                                 |
/// |-------------------|--------------------------|-------------------------------------|
/// | `path`            | `STRING | STORED | FAST` | primary key (exact match, delete)   |
/// | `uid`             | `STRING | STORED`        | stable id of the track              |
/// | `title`           | `TEXT | STORED`          | tokenized, for search               |
/// | `artist`          | `TEXT | STORED`          | tokenized, for search               |
/// | `album`           | `TEXT | STORED`          | tokenized, for search               |
/// | `title_sort`      | `STRING | FAST`          | lowercased title, for ordering      |
/// | `artist_sort`     | `STRING | FAST`          | lowercased artist, for ordering     |
/// | `album_key`       | `FACET`                  | `/artist/album`, for browsing       |
/// | `duration_ms`     | `U64 | STORED | FAST`    | track length                        |
/// | `mtime`           | `U64 | FAST`             | change detection (file mtime, ms)   |
/// | `size`            | `U64 | FAST`             | change detection (file size, bytes) |
/// | `artwork_base64`  | `STORED`                 | cover art, base64 (not indexed)     |
/// | `last_scanned_at` | `U64 | FAST`             | last scan time (ms since epoch)     |
///
/// `title_sort`, `artist_sort` and `album_key` are derived from the other
/// fields when the document is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    /// Absolute file path. Primary key of the index.
    pub path: String,
    /// Stable id of the track.
    pub uid: String,
    pub title: String,
    pub artist: String,
    pub album: Option<String>,
    pub duration_ms: Option<u64>,
    /// File modification time (milliseconds since the Unix epoch).
    pub mtime: u64,
    /// File size in bytes.
    pub size: u64,
    /// Cover art as base64-encoded image bytes. Stored only (not indexed):
    /// image data can exceed Tantivy's maximum term length.
    pub artwork_base64: Option<String>,
    /// When the file was last scanned (milliseconds since the Unix epoch).
    pub last_scanned_at: u64,
}

/// `path`, `mtime`, `size` and `last_scanned_at` of an indexed track, read
/// from fast fields only, for comparing the index with the files on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanState {
    pub path: String,
    pub mtime: u64,
    pub size: u64,
    pub last_scanned_at: u64,
}

/// An album of the `album_key` facet (`/artist/album`) and its track count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlbumInfo {
    pub artist: String,
    pub album: String,
    pub track_count: u64,
}

/// Order of [list_documents].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// By `title_sort`, A→Z.
    Title,
    /// By `artist_sort`, A→Z.
    Artist,
}

/// 텍스트 내 바이트 오프셋 범위 구조체 (하이라이트 구간 및 스니펫 프래그먼트 범위)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextRange {
    pub start: usize,
    pub end: usize,
}

/// Flutter에서 사용할 검색 결과 구조체
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub score: f32,
    pub doc: Document,
    /// HTML snippet of `title` with the matches highlighted.
    pub snippet: Option<String>,
    /// HTML snippet of `artist` with the matches highlighted.
    pub artist_snippet: Option<String>,
    /// HTML snippet of `album` with the matches highlighted.
    pub album_snippet: Option<String>,
    /// Byte range of [SearchResult::snippet]'s fragment inside `title`.
    pub fragment_range: Option<TextRange>,
    /// Byte ranges of the matches inside the `title` fragment.
    pub highlighted_ranges: Option<Vec<TextRange>>,
}

/// Flutter에서 사용할 검색 응답 구조체 (검색 결과 목록 + 총 매칭 문서 수)
#[derive(Debug, Clone)]
pub struct SearchResponse {
    pub total_hits: usize,
    pub results: Vec<SearchResult>,
}

/// 관대한(Lenient) 쿼리 파싱 검색 응답 구조체 (검색 결과 + 파싱 경고/오류 목록)
#[derive(Debug, Clone)]
pub struct LenientSearchResponse {
    pub total_hits: usize,
    pub results: Vec<SearchResult>,
    pub errors: Vec<String>,
}

/// 인덱스 용어 사전(Term Dictionary) 자동완성/추천 항목 구조체
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermSuggestion {
    pub term: String,
    pub doc_freq: u32,
}

/// 인덱스 디스크/메모리 공간 사용량 통계 구조체 (`Searcher::space_usage` 포팅)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpaceUsage {
    pub total_bytes: u64,
    pub num_docs: u64,
    pub num_segments: usize,
    pub store_bytes: u64,
    pub postings_bytes: u64,
    pub positions_bytes: u64,
    pub fast_fields_bytes: u64,
    pub termdict_bytes: u64,
    pub fieldnorms_bytes: u64,
    pub deletes_bytes: u64,
}

/// Tantivy의 핵심 로직을 관리하는 구조체
struct TantivyApi {
    dir_path: String,
    tokenizer_type: String,
    index: Index,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
    path_field: Field,
    uid_field: Field,
    title_field: Field,
    artist_field: Field,
    album_field: Field,
    title_sort_field: Field,
    artist_sort_field: Field,
    album_key_field: Field,
    duration_ms_field: Field,
    mtime_field: Field,
    size_field: Field,
    artwork_base64_field: Field,
    last_scanned_at_field: Field,
}

impl TantivyApi {
    /// The tokenized, searchable fields.
    fn text_fields(&self) -> [Field; 3] {
        [self.title_field, self.artist_field, self.album_field]
    }
}

/// Builds the index schema. `tokenizer_name` is used by the text fields.
fn build_schema(tokenizer_name: &str) -> Schema {
    let mut b = Schema::builder();
    let text = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer_name)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();

    b.add_text_field("path", STRING | STORED | FAST);
    b.add_text_field("uid", STRING | STORED);
    b.add_text_field("title", text.clone());
    b.add_text_field("artist", text.clone());
    b.add_text_field("album", text);
    b.add_text_field("title_sort", STRING | FAST);
    b.add_text_field("artist_sort", STRING | FAST);
    b.add_facet_field("album_key", FacetOptions::default());
    b.add_u64_field("duration_ms", STORED | FAST);
    b.add_u64_field("mtime", FAST);
    b.add_u64_field("size", FAST);
    b.add_text_field("artwork_base64", STORED);
    b.add_u64_field("last_scanned_at", FAST);
    b.build()
}

/// Sort key stored in `title_sort` / `artist_sort`.
fn sort_key(value: &str) -> String {
    value.trim().to_lowercase()
}

/// The `album_key` facet of a track, or `None` without an album.
fn album_facet(artist: &str, album: Option<&str>) -> Option<Facet> {
    let album = album.map(str::trim).filter(|a| !a.is_empty())?;
    Some(Facet::from_path([artist.trim(), album]))
}

/// Builds the Tantivy document stored for [doc].
fn to_tantivy_doc(api: &TantivyApi, doc: &Document) -> TantivyDocument {
    let mut d = TantivyDocument::new();
    d.add_text(api.path_field, &doc.path);
    d.add_text(api.uid_field, &doc.uid);
    d.add_text(api.title_field, &doc.title);
    d.add_text(api.artist_field, &doc.artist);
    d.add_text(api.title_sort_field, sort_key(&doc.title));
    d.add_text(api.artist_sort_field, sort_key(&doc.artist));
    if let Some(album) = doc.album.as_deref().filter(|a| !a.trim().is_empty()) {
        d.add_text(api.album_field, album);
    }
    if let Some(facet) = album_facet(&doc.artist, doc.album.as_deref()) {
        d.add_facet(api.album_key_field, facet);
    }
    if let Some(duration_ms) = doc.duration_ms {
        d.add_u64(api.duration_ms_field, duration_ms);
    }
    d.add_u64(api.mtime_field, doc.mtime);
    d.add_u64(api.size_field, doc.size);
    if let Some(artwork) = doc.artwork_base64.as_deref().filter(|a| !a.is_empty()) {
        d.add_text(api.artwork_base64_field, artwork);
    }
    d.add_u64(api.last_scanned_at_field, doc.last_scanned_at);
    d
}

/// Reads a [Document] back: stored fields from the doc store, `mtime`,
/// `size` and `last_scanned_at` from their fast-field columns.
fn read_document(
    searcher: &Searcher,
    api: &TantivyApi,
    retrieved: &TantivyDocument,
    address: DocAddress,
) -> Result<Document> {
    let text = |field: Field| {
        retrieved
            .get_first(field)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    let fast = searcher.segment_reader(address.segment_ord).fast_fields();
    let fast_u64 = |name: &str| -> Result<u64> {
        Ok(fast.u64(name)?.first(address.doc_id).unwrap_or_default())
    };
    Ok(Document {
        path: text(api.path_field).unwrap_or_default(),
        uid: text(api.uid_field).unwrap_or_default(),
        title: text(api.title_field).unwrap_or_default(),
        artist: text(api.artist_field).unwrap_or_default(),
        album: text(api.album_field),
        duration_ms: retrieved
            .get_first(api.duration_ms_field)
            .and_then(|v| v.as_u64()),
        mtime: fast_u64("mtime")?,
        size: fast_u64("size")?,
        artwork_base64: text(api.artwork_base64_field),
        last_scanned_at: fast_u64("last_scanned_at")?,
    })
}

/// Loads the documents at [addresses], in order.
fn read_documents(
    searcher: &Searcher,
    api: &TantivyApi,
    addresses: impl IntoIterator<Item = DocAddress>,
) -> Result<Vec<Document>> {
    addresses
        .into_iter()
        .map(|address| {
            let retrieved = searcher.doc::<TantivyDocument>(address)?;
            read_document(searcher, api, &retrieved, address)
        })
        .collect()
}

/// 전역 상태를 Lazy와 Arc<Mutex<...>>로 안전하게 관리
static STATE: Lazy<Arc<Mutex<Option<TantivyApi>>>> = Lazy::new(|| Arc::new(Mutex::new(None)));

/// 토크나이저 타입 문자열을 정규화하고 실제 등록될 토크나이저 이름을 반환
fn resolve_tokenizer_name(tokenizer_type: &str) -> (&'static str, Option<Language>) {
    let lower = tokenizer_type.trim().to_lowercase();
    match lower.as_str() {
        "cjk" | "ngram" => ("ngram", None),
        "whitespace" => ("whitespace", None),
        "raw" => ("raw", None),
        "stem" | "stemmer" | "en_stem" | "stem:english" | "stem_english" => {
            ("en_stem", Some(Language::English))
        }
        "stem:arabic" | "stem_arabic" => ("stem_arabic", Some(Language::Arabic)),
        "stem:danish" | "stem_danish" => ("stem_danish", Some(Language::Danish)),
        "stem:dutch" | "stem_dutch" => ("stem_dutch", Some(Language::Dutch)),
        "stem:finnish" | "stem_finnish" => ("stem_finnish", Some(Language::Finnish)),
        "stem:french" | "stem_french" => ("stem_french", Some(Language::French)),
        "stem:german" | "stem_german" => ("stem_german", Some(Language::German)),
        "stem:greek" | "stem_greek" => ("stem_greek", Some(Language::Greek)),
        "stem:hungarian" | "stem_hungarian" => ("stem_hungarian", Some(Language::Hungarian)),
        "stem:italian" | "stem_italian" => ("stem_italian", Some(Language::Italian)),
        "stem:norwegian" | "stem_norwegian" => ("stem_norwegian", Some(Language::Norwegian)),
        "stem:portuguese" | "stem_portuguese" => ("stem_portuguese", Some(Language::Portuguese)),
        "stem:romanian" | "stem_romanian" => ("stem_romanian", Some(Language::Romanian)),
        "stem:russian" | "stem_russian" => ("stem_russian", Some(Language::Russian)),
        "stem:spanish" | "stem_spanish" => ("stem_spanish", Some(Language::Spanish)),
        "stem:swedish" | "stem_swedish" => ("stem_swedish", Some(Language::Swedish)),
        "stem:tamil" | "stem_tamil" => ("stem_tamil", Some(Language::Tamil)),
        "stem:turkish" | "stem_turkish" => ("stem_turkish", Some(Language::Turkish)),
        _ => ("default", None),
    }
}

/// 인덱스에 커스텀 토크나이저(CJK N-gram, Whitespace, 18개 언어 Stemmer)를 모두 등록
fn register_custom_tokenizers(index: &Index) -> Result<()> {
    let ngram_tokenizer = NgramTokenizer::new(1, 3, false)?;
    let ngram_analyzer = TextAnalyzer::builder(ngram_tokenizer)
        .filter(LowerCaser)
        .build();
    index.tokenizers().register("ngram", ngram_analyzer);

    let whitespace_analyzer = TextAnalyzer::builder(WhitespaceTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build();
    index
        .tokenizers()
        .register("whitespace", whitespace_analyzer);

    index.tokenizers().register("raw", RawTokenizer::default());

    let stemmer_languages = [
        ("en_stem", Language::English),
        ("stem_english", Language::English),
        ("stem_arabic", Language::Arabic),
        ("stem_danish", Language::Danish),
        ("stem_dutch", Language::Dutch),
        ("stem_finnish", Language::Finnish),
        ("stem_french", Language::French),
        ("stem_german", Language::German),
        ("stem_greek", Language::Greek),
        ("stem_hungarian", Language::Hungarian),
        ("stem_italian", Language::Italian),
        ("stem_norwegian", Language::Norwegian),
        ("stem_portuguese", Language::Portuguese),
        ("stem_romanian", Language::Romanian),
        ("stem_russian", Language::Russian),
        ("stem_spanish", Language::Spanish),
        ("stem_swedish", Language::Swedish),
        ("stem_tamil", Language::Tamil),
        ("stem_turkish", Language::Turkish),
    ];

    for (name, lang) in stemmer_languages {
        let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(Stemmer::new(lang))
            .build();
        index.tokenizers().register(name, analyzer);
    }

    Ok(())
}

/// 스니펫 프래그먼트의 원본 텍스트 내 바이트 오프셋 범위를 계산 (Tantivy #3134 `Snippet::fragment_range` 포팅)
fn compute_fragment_range(text: &str, snippet: &Snippet) -> Option<TextRange> {
    let fragment = snippet.fragment();
    if fragment.is_empty() {
        return Some(TextRange { start: 0, end: 0 });
    }
    let normalized = text.trim();
    normalized.find(fragment).map(|start| TextRange {
        start,
        end: start + fragment.len(),
    })
}

/// 검색된 문서 목록과 쿼리로부터 SearchResult 벡터를 생성하는 내부 헬퍼
fn collect_search_results(
    searcher: &Searcher,
    api: &TantivyApi,
    query: &dyn Query,
    top_docs: Vec<(f32, DocAddress)>,
    enable_snippet: bool,
) -> Result<Vec<SearchResult>> {
    let snippet_gen = |field: Field| {
        if enable_snippet {
            SnippetGenerator::create(searcher, query, field).ok()
        } else {
            None
        }
    };
    let title_gen = snippet_gen(api.title_field);
    let artist_gen = snippet_gen(api.artist_field);
    let album_gen = snippet_gen(api.album_field);
    let html_of = |gen: &Option<SnippetGenerator>, doc: &TantivyDocument| {
        gen.as_ref().and_then(|gen| {
            let s = gen.snippet_from_doc(doc);
            (!s.is_empty()).then(|| s.to_html())
        })
    };

    let mut results = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
        let doc = read_document(searcher, api, &retrieved_doc, doc_address)?;

        let (snippet, fragment_range, highlighted_ranges) = if let Some(ref gen) = title_gen {
            let s = gen.snippet_from_doc(&retrieved_doc);
            if s.is_empty() {
                (
                    Some(String::new()),
                    Some(TextRange { start: 0, end: 0 }),
                    Some(Vec::new()),
                )
            } else {
                let html = s.to_html();
                let frag_range = compute_fragment_range(&doc.title, &s);
                let highlights = s
                    .highlighted()
                    .iter()
                    .map(|r| TextRange {
                        start: r.start,
                        end: r.end,
                    })
                    .collect();
                (Some(html), frag_range, Some(highlights))
            }
        } else {
            (None, None, None)
        };

        results.push(SearchResult {
            score,
            artist_snippet: html_of(&artist_gen, &retrieved_doc),
            album_snippet: html_of(&album_gen, &retrieved_doc),
            doc,
            snippet,
            fragment_range,
            highlighted_ranges,
        });
    }

    Ok(results)
}

/// `field:*` 형태의 Exists 쿼리 문법을 지원하는 전처리 헬퍼 (Tantivy #2974 포팅)
/// `title:*`, `text:*`, `id:*` 등의 토큰이 단독 또는 복합 쿼리에 포함된 경우 regex `field:/.+/`로 변환하거나 ExistsQuery를 구성합니다.
fn preprocess_exists_query_syntax(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;
    let mut in_quotes = false;

    while i < chars.len() {
        if chars[i] == '"' && (i == 0 || chars[i - 1] != '\\') {
            in_quotes = !in_quotes;
            out.push(chars[i]);
            i += 1;
            continue;
        }

        if !in_quotes {
            let rem: String = chars[i..].iter().collect();
            let prev_is_boundary =
                i == 0 || chars[i - 1].is_whitespace() || matches!(chars[i - 1], '(' | '+' | '-');

            if prev_is_boundary {
                let mut matched_field = None;
                for field in ["title", "artist", "album", "path", "uid"] {
                    let pattern = format!("{field}:*");
                    if let Some(after) = rem.strip_prefix(&pattern) {
                        let next_ch = after.chars().next();
                        let next_is_boundary = next_ch.is_none()
                            || next_ch.unwrap().is_whitespace()
                            || matches!(next_ch.unwrap(), ')' | '^');
                        if next_is_boundary {
                            matched_field = Some((field, pattern.len()));
                            break;
                        }
                    }
                }

                if let Some((field, pat_len)) = matched_field {
                    out.push_str(&format!("{field}:/.+/"));
                    i += pat_len;
                    continue;
                }
            }
        }

        out.push(chars[i]);
        i += 1;
    }

    out
}

/// Tantivy 인덱스를 초기화하는 함수
/// `tokenizer_type`: `"default"`, `"cjk"`/`"ngram"`, `"whitespace"`, `"raw"`, `"en_stem"`/`"stemmer"` 또는 `"stem:<language>"` 선택 가능
#[flutter_rust_bridge::frb(sync)]
pub fn init_tantivy(dir_path: String, tokenizer_type: Option<String>) -> Result<()> {
    let mut state_lock = STATE.lock().unwrap();
    let t_type = tokenizer_type.unwrap_or_else(|| "default".to_string());

    if let Some(existing) = state_lock.as_ref() {
        if existing.dir_path == dir_path && existing.tokenizer_type == t_type {
            return Ok(());
        }
    }

    let index_dir = PathBuf::from(&dir_path);
    std::fs::create_dir_all(&index_dir)?;

    let (tokenizer_name, _) = resolve_tokenizer_name(&t_type);

    let (index, schema) = if index_dir.join("meta.json").exists() {
        let index = Index::open_in_dir(&index_dir)?;
        let schema = index.schema();
        (index, schema)
    } else {
        let index = Index::create_in_dir(&index_dir, build_schema(tokenizer_name))?;
        (index.clone(), index.schema())
    };

    register_custom_tokenizers(&index)?;

    let field = |name: &str| {
        schema.get_field(name).map_err(|_| {
            anyhow!(
                "'{name}' field not found: the index in {dir_path} uses an older schema, \
                 delete the directory to rebuild it"
            )
        })
    };
    let path_field = field("path")?;
    let uid_field = field("uid")?;
    let title_field = field("title")?;
    let artist_field = field("artist")?;
    let album_field = field("album")?;
    let title_sort_field = field("title_sort")?;
    let artist_sort_field = field("artist_sort")?;
    let album_key_field = field("album_key")?;
    let duration_ms_field = field("duration_ms")?;
    let mtime_field = field("mtime")?;
    let size_field = field("size")?;
    let artwork_base64_field = field("artwork_base64")?;
    let last_scanned_at_field = field("last_scanned_at")?;

    let writer = index.writer(50_000_000)?; // 50MB heap

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    let api = TantivyApi {
        dir_path,
        tokenizer_type: t_type,
        index,
        writer: Mutex::new(writer),
        reader,
        path_field,
        uid_field,
        title_field,
        artist_field,
        album_field,
        title_sort_field,
        artist_sort_field,
        album_key_field,
        duration_ms_field,
        mtime_field,
        size_field,
        artwork_base64_field,
        last_scanned_at_field,
    };

    *state_lock = Some(api);

    Ok(())
}

/// Tantivy 상태를 해제하고 인덱스 및 리소스를 닫는 함수
#[flutter_rust_bridge::frb(sync)]
pub fn close_tantivy() -> Result<()> {
    let mut state_lock = STATE.lock().unwrap();
    *state_lock = None;
    Ok(())
}

/// [CREATE/UPDATE] 단일 문서를 추가하는 함수 (즉시 commit)
pub fn add_document(doc: Document) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer = api.writer.lock().unwrap();

    writer.delete_term(Term::from_field_text(api.path_field, &doc.path));

    let tantivy_doc = to_tantivy_doc(api, &doc);

    writer.add_document(tantivy_doc)?;
    writer.commit()?;

    Ok(())
}

/// [READ] 쿼리로 문서를 검색하는 함수 (페이지네이션, 하이라이팅 스니펫, `field:*` Exists 문법 및 총 결과 수 지원)
pub fn search_documents(
    query: String,
    top_k: usize,
    offset: Option<usize>,
    enable_snippet: Option<bool>,
) -> Result<SearchResponse> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let mut query_parser = QueryParser::for_index(&api.index, api.text_fields().to_vec());
    query_parser.allow_regexes();
    let effective_query = preprocess_exists_query_syntax(&query);
    let parsed_query = query_parser.parse_query(&effective_query)?;

    let offset_val = offset.unwrap_or(0);
    let top_docs_collector = TopDocs::with_limit(top_k)
        .and_offset(offset_val)
        .order_by_score();

    let (top_docs, total_hits) = searcher.search(&parsed_query, &(top_docs_collector, Count))?;

    let results = collect_search_results(
        &searcher,
        api,
        &*parsed_query,
        top_docs,
        enable_snippet.unwrap_or(false),
    )?;

    Ok(SearchResponse {
        total_hits,
        results,
    })
}

/// [READ] 관대한(Lenient) 쿼리 파서로 문서를 검색하는 함수 (문법 오류가 있는 사용자 입력도 안전하게 처리하며 필드 부스트 및 기본 AND 모드 지원)
pub fn search_documents_lenient(
    query: String,
    top_k: usize,
    offset: Option<usize>,
    enable_snippet: Option<bool>,
    conjunction_by_default: Option<bool>,
    title_boost: Option<f32>,
) -> Result<LenientSearchResponse> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let mut query_parser = QueryParser::for_index(&api.index, api.text_fields().to_vec());
    query_parser.allow_regexes();
    if conjunction_by_default.unwrap_or(false) {
        query_parser.set_conjunction_by_default();
    }
    if let Some(boost) = title_boost {
        query_parser.set_field_boost(api.title_field, boost);
    }

    let effective_query = preprocess_exists_query_syntax(&query);
    let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&effective_query);
    let errors = parse_errors.into_iter().map(|e| e.to_string()).collect();

    let offset_val = offset.unwrap_or(0);
    let top_docs_collector = TopDocs::with_limit(top_k)
        .and_offset(offset_val)
        .order_by_score();

    let (top_docs, total_hits) = searcher.search(&parsed_query, &(top_docs_collector, Count))?;

    let results = collect_search_results(
        &searcher,
        api,
        &*parsed_query,
        top_docs,
        enable_snippet.unwrap_or(false),
    )?;

    Ok(LenientSearchResponse {
        total_hits,
        results,
        errors,
    })
}

/// [READ] 정규표현식(Regex) 패턴으로 문서를 검색하는 함수
pub fn search_documents_regex(
    pattern: String,
    top_k: usize,
    offset: Option<usize>,
    enable_snippet: Option<bool>,
) -> Result<SearchResponse> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let mut regexes: Vec<Box<dyn Query>> = Vec::new();
    for field in api.text_fields() {
        regexes.push(Box::new(RegexQuery::from_pattern(&pattern, field)?));
    }
    let query = BooleanQuery::union(regexes);

    let offset_val = offset.unwrap_or(0);
    let top_docs_collector = TopDocs::with_limit(top_k)
        .and_offset(offset_val)
        .order_by_score();

    let (top_docs, total_hits) = searcher.search(&query, &(top_docs_collector, Count))?;

    let results = collect_search_results(
        &searcher,
        api,
        &query,
        top_docs,
        enable_snippet.unwrap_or(false),
    )?;

    Ok(SearchResponse {
        total_hits,
        results,
    })
}

/// [READ] 정규표현식 구문(Regex Phrase) 패턴 시퀀스와 슬롭(slop)으로 문서를 검색하는 함수 (Tantivy #3135, #3137, #3138 포팅)
pub fn search_documents_regex_phrase(
    patterns: Vec<String>,
    slop: Option<u32>,
    max_expansions: Option<u32>,
    top_k: usize,
    offset: Option<usize>,
    enable_snippet: Option<bool>,
) -> Result<SearchResponse> {
    if patterns.is_empty() {
        return Ok(SearchResponse {
            total_hits: 0,
            results: Vec::new(),
        });
    }
    if patterns.len() == 1 {
        return search_documents_regex(
            patterns.into_iter().next().unwrap(),
            top_k,
            offset,
            enable_snippet,
        );
    }

    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let is_ngram = resolve_tokenizer_name(&api.tokenizer_type).0 == "ngram";
    let slop_val = if is_ngram {
        slop.unwrap_or(16).max(16)
    } else {
        slop.unwrap_or(0)
    };

    let normalized_patterns: Vec<String> = patterns
        .into_iter()
        .map(|p| {
            let lower = p.to_lowercase();
            if is_ngram
                && lower.chars().count() > 3
                && !lower.contains(['.', '*', '+', '?', '[', ']', '(', ')', '|', '^', '$', '\\'])
            {
                lower.chars().take(3).collect()
            } else {
                lower
            }
        })
        .collect();

    let terms_with_offset: Vec<(usize, String)> =
        normalized_patterns.into_iter().enumerate().collect();

    let mut phrases: Vec<Box<dyn Query>> = Vec::new();
    for field in api.text_fields() {
        let mut phrase =
            RegexPhraseQuery::new_with_offset_and_slop(field, terms_with_offset.clone(), slop_val);
        if let Some(max_exp) = max_expansions {
            phrase.set_max_expansions(max_exp);
        }
        phrases.push(Box::new(phrase));
    }

    let query = BooleanQuery::union(phrases);

    let offset_val = offset.unwrap_or(0);
    let top_docs_collector = TopDocs::with_limit(top_k)
        .and_offset(offset_val)
        .order_by_score();

    let (top_docs, total_hits) = searcher.search(&query, &(top_docs_collector, Count))?;

    let results = collect_search_results(
        &searcher,
        api,
        &query,
        top_docs,
        enable_snippet.unwrap_or(false),
    )?;

    Ok(SearchResponse {
        total_hits,
        results,
    })
}

/// [READ] 오타 허용(Fuzzy / Levenshtein Automaton 기반)으로 문서를 검색하는 함수 (Tantivy #3136 포팅)
pub fn search_documents_fuzzy(
    term: String,
    distance: Option<u8>,
    transposition_cost_one: Option<bool>,
    prefix: Option<bool>,
    top_k: usize,
    offset: Option<usize>,
    enable_snippet: Option<bool>,
) -> Result<SearchResponse> {
    let trimmed = term.trim();
    if trimmed.is_empty() {
        return Ok(SearchResponse {
            total_hits: 0,
            results: Vec::new(),
        });
    }

    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let dist = distance.unwrap_or(1).min(2);
    let trans_cost_one = transposition_cost_one.unwrap_or(true);
    let is_prefix = prefix.unwrap_or(false);
    let is_ngram = resolve_tokenizer_name(&api.tokenizer_type).0 == "ngram";

    let words: Vec<String> = trimmed
        .split_whitespace()
        .map(|w| w.to_lowercase())
        .collect();

    let mut candidate_terms: Vec<String> = Vec::new();
    for w in &words {
        candidate_terms.push(w.clone());
        if is_ngram {
            let chars: Vec<char> = w.chars().collect();
            if chars.len() > 3 {
                for window in chars.windows(3) {
                    candidate_terms.push(window.iter().collect());
                }
            }
        }
    }

    let mut subqueries: Vec<Box<dyn Query>> = Vec::with_capacity(candidate_terms.len() * 3);
    for w in &candidate_terms {
        for field in api.text_fields() {
            let term = Term::from_field_text(field, w);
            let fuzzy = if is_prefix {
                FuzzyTermQuery::new_prefix(term, dist, trans_cost_one)
            } else {
                FuzzyTermQuery::new(term, dist, trans_cost_one)
            };
            subqueries.push(Box::new(fuzzy));
        }
    }

    let query = BooleanQuery::union(subqueries);

    let offset_val = offset.unwrap_or(0);
    let top_docs_collector = TopDocs::with_limit(top_k)
        .and_offset(offset_val)
        .order_by_score();

    let (top_docs, total_hits) = searcher.search(&query, &(top_docs_collector, Count))?;

    let results = collect_search_results(
        &searcher,
        api,
        &query,
        top_docs,
        enable_snippet.unwrap_or(false),
    )?;

    Ok(SearchResponse {
        total_hits,
        results,
    })
}

/// [READ] Tracks similar to the track at `path` (`MoreLikeThisQuery`).
#[allow(clippy::too_many_arguments)]
pub fn search_similar_documents(
    path: String,
    top_k: usize,
    min_doc_frequency: Option<u64>,
    max_doc_frequency: Option<u64>,
    min_term_frequency: Option<usize>,
    min_word_length: Option<usize>,
    max_word_length: Option<usize>,
    boost_factor: Option<f32>,
) -> Result<SearchResponse> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let id_term = Term::from_field_text(api.path_field, &path);
    let id_query = TermQuery::new(id_term.clone(), IndexRecordOption::Basic);
    let target_hits = searcher.search(&id_query, &TopDocs::with_limit(1).order_by_score())?;

    let Some((_, doc_address)) = target_hits.first() else {
        return Ok(SearchResponse {
            total_hits: 0,
            results: Vec::new(),
        });
    };

    let mut builder = MoreLikeThisQuery::builder()
        .with_min_doc_frequency(min_doc_frequency.unwrap_or(1))
        .with_min_term_frequency(min_term_frequency.unwrap_or(1));

    if let Some(max_df) = max_doc_frequency {
        builder = builder.with_max_doc_frequency(max_df);
    }
    if let Some(min_wl) = min_word_length {
        builder = builder.with_min_word_length(min_wl);
    }
    if let Some(max_wl) = max_word_length {
        builder = builder.with_max_word_length(max_wl);
    }
    if let Some(boost) = boost_factor {
        builder = builder.with_boost_factor(boost);
    }

    let mlt_query = builder.with_document(*doc_address);

    // Exclude the reference document itself from similar results
    let exclude_self = BooleanQuery::new(vec![
        (Occur::Must, Box::new(mlt_query)),
        (
            Occur::MustNot,
            Box::new(TermQuery::new(id_term, IndexRecordOption::Basic)),
        ),
    ]);

    let top_docs_collector = TopDocs::with_limit(top_k).order_by_score();
    let (top_docs, total_hits) = searcher.search(&exclude_self, &(top_docs_collector, Count))?;

    let results = collect_search_results(&searcher, api, &exclude_self, top_docs, false)?;

    Ok(SearchResponse {
        total_hits,
        results,
    })
}

/// [READ] 특정 필드에 값이 존재하는 문서를 검색하는 함수 (`ExistsQuery` / Tantivy #2974, #3068 포팅)
pub fn search_documents_exists(
    field_name: String,
    top_k: usize,
    offset: Option<usize>,
) -> Result<SearchResponse> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let schema = api.index.schema();
    let field = schema
        .get_field(&field_name)
        .map_err(|_| anyhow!("Field '{field_name}' not found in schema"))?;
    let field_entry = schema.get_field_entry(field);

    let query: Box<dyn Query> = if field_entry.is_fast() {
        Box::new(ExistsQuery::new(field_name, true))
    } else if field == api.path_field {
        Box::new(AllQuery)
    } else {
        Box::new(RegexQuery::from_pattern(".+", field)?)
    };

    let offset_val = offset.unwrap_or(0);
    let top_docs_collector = TopDocs::with_limit(top_k)
        .and_offset(offset_val)
        .order_by_score();

    let (top_docs, total_hits) = searcher.search(&*query, &(top_docs_collector, Count))?;
    let results = collect_search_results(&searcher, api, &*query, top_docs, false)?;

    Ok(SearchResponse {
        total_hits,
        results,
    })
}

/// [READ] 접두사(prefix)로 인덱스 용어 사전(Term Dictionary)을 스트리밍 조회하여 추천 검색어 및 문서 빈도를 반환하는 함수 (Tantivy #3155 포팅)
pub fn suggest_terms(
    prefix: String,
    limit: Option<usize>,
    include_title: Option<bool>,
) -> Result<Vec<TermSuggestion>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    let is_ngram = resolve_tokenizer_name(&api.tokenizer_type).0 == "ngram";
    let raw_prefix = prefix.trim().to_lowercase();
    let normalized_prefix: String = if is_ngram && raw_prefix.chars().count() > 3 {
        raw_prefix.chars().take(3).collect()
    } else {
        raw_prefix
    };
    let max_items = limit.unwrap_or(10);
    let mut fields = vec![api.artist_field, api.album_field];
    if include_title.unwrap_or(true) {
        fields.push(api.title_field);
    }

    let mut term_freqs: BTreeMap<String, u32> = BTreeMap::new();

    for segment_reader in searcher.segment_readers() {
        for &field in &fields {
            let Ok(inverted_index) = segment_reader.inverted_index(field) else {
                continue;
            };
            let termdict = inverted_index.terms();
            let mut range_builder = termdict.range();
            if !normalized_prefix.is_empty() {
                range_builder = range_builder.ge(normalized_prefix.as_bytes());
            }
            let Ok(mut stream) = range_builder.into_stream() else {
                continue;
            };

            while stream.advance() {
                let Ok(term_str) = std::str::from_utf8(stream.key()) else {
                    continue;
                };
                if !normalized_prefix.is_empty() && !term_str.starts_with(&normalized_prefix) {
                    break;
                }
                let doc_freq = stream.value().doc_freq;
                *term_freqs.entry(term_str.to_string()).or_insert(0) += doc_freq;
            }
        }
    }

    let mut suggestions: Vec<TermSuggestion> = term_freqs
        .into_iter()
        .map(|(term, doc_freq)| TermSuggestion { term, doc_freq })
        .collect();

    suggestions.sort_by(|a, b| {
        b.doc_freq
            .cmp(&a.doc_freq)
            .then_with(|| a.term.cmp(&b.term))
    });
    suggestions.truncate(max_items);

    Ok(suggestions)
}

/// [READ] The track at `path`, if indexed.
#[flutter_rust_bridge::frb(sync)]
pub fn get_document_by_path(path: String) -> Result<Option<Document>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let query = TermQuery::new(
        Term::from_field_text(api.path_field, &path),
        IndexRecordOption::Basic,
    );
    let top_docs = searcher.search(&query, &TopDocs::with_limit(1).order_by_score())?;

    match top_docs.first() {
        Some((_, address)) => Ok(read_documents(&searcher, api, [*address])?.pop()),
        None => Ok(None),
    }
}

/// [READ] 인덱스에 등록된 전체 문서 수를 가져오는 함수
#[flutter_rust_bridge::frb(sync)]
pub fn get_num_docs() -> Result<u64> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    Ok(searcher.num_docs())
}

/// [READ] 인덱스의 디스크/컴포넌트별 공간 사용량 통계를 가져오는 함수 (`Searcher::space_usage` 포팅)
#[flutter_rust_bridge::frb(sync)]
pub fn get_index_space_usage() -> Result<IndexSpaceUsage> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let usage = searcher.space_usage()?;

    let mut store_bytes = 0u64;
    let mut postings_bytes = 0u64;
    let mut positions_bytes = 0u64;
    let mut fast_fields_bytes = 0u64;
    let mut termdict_bytes = 0u64;
    let mut fieldnorms_bytes = 0u64;
    let mut deletes_bytes = 0u64;

    for seg in usage.segments() {
        store_bytes += seg.store().total().get_bytes();
        postings_bytes += seg.postings().total().get_bytes();
        positions_bytes += seg.positions().total().get_bytes();
        fast_fields_bytes += seg.fast_fields().total().get_bytes();
        termdict_bytes += seg.termdict().total().get_bytes();
        fieldnorms_bytes += seg.fieldnorms().total().get_bytes();
        deletes_bytes += seg.deletes().get_bytes();
    }

    Ok(IndexSpaceUsage {
        total_bytes: usage.total().get_bytes(),
        num_docs: searcher.num_docs(),
        num_segments: usage.segments().len(),
        store_bytes,
        postings_bytes,
        positions_bytes,
        fast_fields_bytes,
        termdict_bytes,
        fieldnorms_bytes,
        deletes_bytes,
    })
}

/// [READ] 현재 인덱스 메타데이터에 포함된 세그먼트 파일 상대 경로 목록을 가져오는 함수 (Tantivy #3080 `IndexMeta::list_segment_files` 포팅)
#[flutter_rust_bridge::frb(sync)]
pub fn list_index_segment_files() -> Result<Vec<String>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let meta = api.index.load_metas()?;
    let files: HashSet<PathBuf> = meta
        .segments
        .iter()
        .flat_map(|seg| seg.list_files())
        .collect();

    let mut file_names: Vec<String> = files
        .into_iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    file_names.sort();

    Ok(file_names)
}

/// [UPDATE] 문서를 업데이트하는 함수
pub fn update_document(doc: Document) -> Result<()> {
    add_document(doc)
}

/// [DELETE] Removes the track at `path` (commits).
pub fn delete_document(path: String) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer = api.writer.lock().unwrap();
    let id_term = Term::from_field_text(api.path_field, &path);

    writer.delete_term(id_term);
    writer.commit()?;

    Ok(())
}

/// [DELETE] 모든 문서를 일괄 삭제하는 함수
pub fn delete_all_documents() -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer = api.writer.lock().unwrap();
    writer.delete_all_documents()?;
    writer.commit()?;

    Ok(())
}

/// [BATCH] 여러 문서를 한 번에 추가하는 함수
pub fn add_documents_batch(docs: Vec<Document>) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer = api.writer.lock().unwrap();

    for doc in docs {
        writer.delete_term(Term::from_field_text(api.path_field, &doc.path));

        let tantivy_doc = to_tantivy_doc(api, &doc);

        writer.add_document(tantivy_doc)?;
    }

    writer.commit()?;

    Ok(())
}

/// [BATCH] Removes the tracks at `paths` (one commit).
pub fn delete_documents_batch(paths: Vec<String>) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer = api.writer.lock().unwrap();

    for path in paths {
        let id_term = Term::from_field_text(api.path_field, &path);
        writer.delete_term(id_term);
    }

    writer.commit()?;

    Ok(())
}

/// [UTILITY] 명시적으로 commit을 수행하는 함수
#[flutter_rust_bridge::frb(sync)]
pub fn commit() -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer = api.writer.lock().unwrap();
    writer.commit()?;

    Ok(())
}

/// [CREATE] commit 없이 문서를 추가하는 함수
pub fn add_document_no_commit(doc: Document) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let writer = api.writer.lock().unwrap();

    writer.delete_term(Term::from_field_text(api.path_field, &doc.path));

    let tantivy_doc = to_tantivy_doc(api, &doc);

    writer.add_document(tantivy_doc)?;

    Ok(())
}

/// [DELETE] Removes the track at `path` without committing.
pub fn delete_document_no_commit(path: String) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let writer = api.writer.lock().unwrap();
    let id_term = Term::from_field_text(api.path_field, &path);

    writer.delete_term(id_term);

    Ok(())
}

/// [READ] A page of tracks ordered by `title_sort` or `artist_sort` (A→Z).
pub fn list_documents(offset: usize, limit: usize, sort: SortOrder) -> Result<Vec<Document>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let sort_field = match sort {
        SortOrder::Title => "title_sort",
        SortOrder::Artist => "artist_sort",
    };
    let collector = TopDocs::with_limit(limit)
        .and_offset(offset)
        .order_by_string_fast_field(sort_field, Order::Asc);
    let top_docs = searcher.search(&AllQuery, &collector)?;
    read_documents(&searcher, api, top_docs.into_iter().map(|(_, a)| a))
}

/// [READ] The tracks at `paths`, in the same order. Unknown paths are skipped.
pub fn get_documents_by_paths(paths: Vec<String>) -> Result<Vec<Document>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let mut addresses = Vec::with_capacity(paths.len());
    for path in &paths {
        let query = TermQuery::new(
            Term::from_field_text(api.path_field, path),
            IndexRecordOption::Basic,
        );
        let top = searcher.search(&query, &TopDocs::with_limit(1).order_by_score())?;
        if let Some((_, address)) = top.first() {
            addresses.push(*address);
        }
    }
    read_documents(&searcher, api, addresses)
}

/// [READ] `path`, `mtime`, `size` and `last_scanned_at` of every track, read
/// from fast fields only (no stored documents are loaded).
pub fn get_scan_states() -> Result<Vec<ScanState>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let mut states = Vec::with_capacity(searcher.num_docs() as usize);
    for segment in searcher.segment_readers() {
        let fast = segment.fast_fields();
        let path = fast
            .str("path")?
            .ok_or_else(|| anyhow!("'path' is not a fast field"))?;
        let mtime = fast.u64("mtime")?;
        let size = fast.u64("size")?;
        let last_scanned_at = fast.u64("last_scanned_at")?;
        let mut buffer = String::new();
        for doc_id in segment.doc_ids_alive() {
            let Some(ord) = path.term_ords(doc_id).next() else {
                continue;
            };
            buffer.clear();
            path.ord_to_str(ord, &mut buffer)?;
            states.push(ScanState {
                path: buffer.clone(),
                mtime: mtime.first(doc_id).unwrap_or_default(),
                size: size.first(doc_id).unwrap_or_default(),
                last_scanned_at: last_scanned_at.first(doc_id).unwrap_or_default(),
            });
        }
    }
    Ok(states)
}

/// [READ] Every album of the `album_key` facet with its track count, ordered
/// by artist then album (case-insensitive).
pub fn list_albums() -> Result<Vec<AlbumInfo>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    // 1st level: artists.
    let mut artists_collector = FacetCollector::for_field("album_key");
    artists_collector.add_facet(Facet::root());
    let artists = searcher.search(&AllQuery, &artists_collector)?;
    let artist_facets: Vec<Facet> = artists
        .get(Facet::root())
        .map(|(facet, _)| facet.clone())
        .collect();
    if artist_facets.is_empty() {
        return Ok(Vec::new());
    }

    // 2nd level: albums of each artist.
    let mut albums_collector = FacetCollector::for_field("album_key");
    for facet in &artist_facets {
        albums_collector.add_facet(facet.clone());
    }
    let counts = searcher.search(&AllQuery, &albums_collector)?;

    let mut albums = Vec::new();
    for artist_facet in &artist_facets {
        for (album_facet, count) in counts.get(artist_facet.clone()) {
            if let [artist, album] = album_facet.to_path().as_slice() {
                albums.push(AlbumInfo {
                    artist: artist.to_string(),
                    album: album.to_string(),
                    track_count: count,
                });
            }
        }
    }
    albums.sort_by(|a, b| {
        (sort_key(&a.artist), sort_key(&a.album)).cmp(&(sort_key(&b.artist), sort_key(&b.album)))
    });
    Ok(albums)
}

/// [READ] The tracks of an album (`album_key` = `/artist/album`), by title.
pub fn list_album_documents(artist: String, album: String) -> Result<Vec<Document>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let Some(facet) = album_facet(&artist, Some(&album)) else {
        return Ok(Vec::new());
    };
    let query = TermQuery::new(
        Term::from_facet(api.album_key_field, &facet),
        IndexRecordOption::Basic,
    );
    let count = searcher.search(&query, &Count)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    let collector = TopDocs::with_limit(count).order_by_string_fast_field("title_sort", Order::Asc);
    let top_docs = searcher.search(&query, &collector)?;
    read_documents(&searcher, api, top_docs.into_iter().map(|(_, a)| a))
}

/// Builds the query matching one search word in one field.
type WordQuery<'a> = &'a dyn Fn(&str, Field) -> Result<Box<dyn Query>>;

/// Escapes `text` for Tantivy's regex syntax.
fn regex_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if !c.is_alphanumeric() {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// [READ] Search as you type over title, artist and album: every word of
/// `query` must match the start of a word in one of them (title matches
/// rank higher). When nothing matches, each word may have one typo instead.
pub fn search_documents_prefix(query: String, top_k: usize) -> Result<SearchResponse> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    // Same tokens as the indexed text.
    let mut analyzer = api.index.tokenizer_for_field(api.title_field)?;
    let mut words = Vec::new();
    let mut stream = analyzer.token_stream(&query);
    while let Some(token) = stream.next() {
        if !words.contains(&token.text) {
            words.push(token.text.clone());
        }
    }
    if words.is_empty() || top_k == 0 {
        return Ok(SearchResponse {
            total_hits: 0,
            results: Vec::new(),
        });
    }

    api.reader.reload()?;
    let searcher = api.reader.searcher();
    let boosts = [
        (api.title_field, 2.0f32),
        (api.artist_field, 1.0),
        (api.album_field, 1.0),
    ];

    // Every word must match at least one field.
    let build = |per_word: WordQuery| -> Result<BooleanQuery> {
        let mut musts: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for word in &words {
            let mut shoulds: Vec<(Occur, Box<dyn Query>)> = Vec::new();
            for (field, boost) in boosts {
                shoulds.push((
                    Occur::Should,
                    Box::new(BoostQuery::new(per_word(word, field)?, boost)),
                ));
            }
            musts.push((Occur::Must, Box::new(BooleanQuery::new(shoulds))));
        }
        Ok(BooleanQuery::new(musts))
    };

    let prefix = build(&|word, field| {
        Ok(Box::new(RegexQuery::from_pattern(
            &format!("{}.*", regex_escape(word)),
            field,
        )?))
    })?;
    let collector = TopDocs::with_limit(top_k).order_by_score();
    let (mut top_docs, mut total_hits) = searcher.search(&prefix, &(collector, Count))?;
    let mut used: Box<dyn Query> = Box::new(prefix);

    if top_docs.is_empty() {
        let fuzzy = build(&|word, field| {
            Ok(Box::new(FuzzyTermQuery::new_prefix(
                Term::from_field_text(field, word),
                1,
                true,
            )))
        })?;
        let collector = TopDocs::with_limit(top_k).order_by_score();
        (top_docs, total_hits) = searcher.search(&fuzzy, &(collector, Count))?;
        used = Box::new(fuzzy);
    }

    let results = collect_search_results(&searcher, api, &*used, top_docs, false)?;
    Ok(SearchResponse {
        total_hits,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tantivy state is a process-wide singleton, so tests must not overlap.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn track(path: &str, title: &str, artist: &str, album: Option<&str>) -> Document {
        Document {
            path: path.into(),
            uid: format!("uid-{path}"),
            title: title.into(),
            artist: artist.into(),
            album: album.map(Into::into),
            duration_ms: Some(215_000),
            mtime: 1_700_000_000_000,
            size: 4_096,
            // A large image: stored fine although it is not indexable.
            artwork_base64: Some("iVBORw0KGgo".repeat(10_000)),
            last_scanned_at: 1_800_000_000_000,
        }
    }

    fn paths(docs: &[Document]) -> Vec<&str> {
        docs.iter().map(|d| d.path.as_str()).collect()
    }

    fn result_paths(res: &SearchResponse) -> Vec<&str> {
        let mut p: Vec<&str> = res.results.iter().map(|r| r.doc.path.as_str()).collect();
        p.sort();
        p
    }

    fn library() -> Vec<Document> {
        vec![
            track(
                "/m/c.mp3",
                "hotel California",
                "Eagles",
                Some("Hotel California"),
            ),
            track(
                "/m/a.mp3",
                "Bohemian Rhapsody",
                "Queen",
                Some("A Night at the Opera"),
            ),
            track(
                "/m/b.mp3",
                "Crazy in Love",
                "Beyonce",
                Some("Dangerously in Love"),
            ),
            track(
                "/m/d.mp3",
                "Love of My Life",
                "Queen",
                Some("A Night at the Opera"),
            ),
            track("/m/e.mp3", "Untagged", "Unknown Artist", None),
        ]
    }

    #[test]
    fn music_schema_lifecycle() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();
        init_tantivy(dir_path.clone(), None).unwrap();

        add_documents_batch(library()).unwrap();
        assert_eq!(get_num_docs().unwrap(), 5);

        // Stored + fast fields round-trip.
        let a = get_document_by_path("/m/a.mp3".into()).unwrap().unwrap();
        assert_eq!(a, library()[1]);
        let by_paths = get_documents_by_paths(vec![
            "/m/d.mp3".into(),
            "/missing".into(),
            "/m/a.mp3".into(),
        ])
        .unwrap();
        assert_eq!(paths(&by_paths), ["/m/d.mp3", "/m/a.mp3"]);

        // Ordering by title_sort (case-insensitive) with offset/limit.
        let page = list_documents(0, 10, SortOrder::Title).unwrap();
        assert_eq!(
            paths(&page),
            ["/m/a.mp3", "/m/b.mp3", "/m/c.mp3", "/m/d.mp3", "/m/e.mp3"]
        );
        let page = list_documents(1, 2, SortOrder::Title).unwrap();
        assert_eq!(paths(&page), ["/m/b.mp3", "/m/c.mp3"]);
        let by_artist = list_documents(0, 1, SortOrder::Artist).unwrap();
        assert_eq!(paths(&by_artist), ["/m/b.mp3"]);

        // Query parser over title/artist/album, with snippets.
        let res = search_documents("queen".into(), 10, None, Some(true)).unwrap();
        assert_eq!(result_paths(&res), ["/m/a.mp3", "/m/d.mp3"]);
        assert!(res.results[0]
            .artist_snippet
            .as_deref()
            .unwrap()
            .contains("<b>"));
        let res = search_documents("title:love".into(), 10, None, None).unwrap();
        assert_eq!(result_paths(&res), ["/m/b.mp3", "/m/d.mp3"]);
        let res =
            search_documents_lenient("\"opera".into(), 10, None, None, None, Some(2.0)).unwrap();
        assert!(!res.errors.is_empty());

        // Prefix search: every word, any field; typo fallback.
        let res = search_documents_prefix("que opera".into(), 10).unwrap();
        assert_eq!(result_paths(&res), ["/m/a.mp3", "/m/d.mp3"]);
        let res = search_documents_prefix("cal".into(), 10).unwrap();
        assert_eq!(result_paths(&res), ["/m/c.mp3"]);
        let res = search_documents_prefix("califronia".into(), 10).unwrap();
        assert_eq!(result_paths(&res), ["/m/c.mp3"]);
        assert!(search_documents_prefix("  ".into(), 10)
            .unwrap()
            .results
            .is_empty());

        // Regex / fuzzy / exists / suggestions on the new fields.
        let res = search_documents_regex("eag.*".into(), 10, None, None).unwrap();
        assert_eq!(result_paths(&res), ["/m/c.mp3"]);
        let res =
            search_documents_fuzzy("quen".into(), Some(1), None, None, 10, None, None).unwrap();
        assert_eq!(result_paths(&res), ["/m/a.mp3", "/m/d.mp3"]);
        let res = search_documents_exists("album".into(), 10, None).unwrap();
        assert_eq!(res.total_hits, 4);
        assert!(!suggest_terms("lo".into(), Some(5), Some(true))
            .unwrap()
            .is_empty());

        // Albums via the facet.
        assert_eq!(
            list_albums().unwrap(),
            vec![
                AlbumInfo {
                    artist: "Beyonce".into(),
                    album: "Dangerously in Love".into(),
                    track_count: 1
                },
                AlbumInfo {
                    artist: "Eagles".into(),
                    album: "Hotel California".into(),
                    track_count: 1
                },
                AlbumInfo {
                    artist: "Queen".into(),
                    album: "A Night at the Opera".into(),
                    track_count: 2
                },
            ]
        );
        let opera = list_album_documents("Queen".into(), "A Night at the Opera".into()).unwrap();
        assert_eq!(paths(&opera), ["/m/a.mp3", "/m/d.mp3"]);

        // Upsert by path; scan states from fast fields.
        let mut changed = track("/m/a.mp3", "Bohemian Rhapsody (Remaster)", "Queen", None);
        changed.mtime = 42;
        changed.size = 7;
        update_document(changed).unwrap();
        assert_eq!(get_num_docs().unwrap(), 5);
        let mut states = get_scan_states().unwrap();
        states.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(
            states[0],
            ScanState {
                path: "/m/a.mp3".into(),
                mtime: 42,
                size: 7,
                last_scanned_at: 1_800_000_000_000
            }
        );
        assert_eq!(list_albums().unwrap()[2].track_count, 1);

        // Delete, reopen, delete all.
        delete_documents_batch(vec!["/m/a.mp3".into(), "/m/b.mp3".into()]).unwrap();
        assert_eq!(get_num_docs().unwrap(), 3);
        assert!(get_document_by_path("/m/a.mp3".into()).unwrap().is_none());
        close_tantivy().unwrap();
        init_tantivy(dir_path, None).unwrap();
        assert_eq!(get_num_docs().unwrap(), 3);
        let similar = search_similar_documents(
            "/m/c.mp3".into(),
            10,
            Some(1),
            None,
            Some(1),
            None,
            None,
            None,
        )
        .unwrap();
        assert!(similar.results.iter().all(|r| r.doc.path != "/m/c.mp3"));
        delete_all_documents().unwrap();
        assert_eq!(get_num_docs().unwrap(), 0);
        close_tantivy().unwrap();
    }

    #[test]
    fn cjk_tokenizer_finds_substrings() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        init_tantivy(dir.path().to_str().unwrap().into(), Some("cjk".into())).unwrap();
        add_document(track(
            "/m/k.mp3",
            "봄날",
            "방탄소년단",
            Some("You Never Walk Alone"),
        ))
        .unwrap();
        let res = search_documents("소년".into(), 10, None, None).unwrap();
        assert_eq!(result_paths(&res), ["/m/k.mp3"]);
        close_tantivy().unwrap();
    }
}
