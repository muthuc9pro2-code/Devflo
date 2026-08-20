MEBIBYTE = 1024 * 1024
GIBIBYTE = 1024 * MEBIBYTE
INGESTION_RAW_BATCH_BYTES = 10 * MEBIBYTE
INGESTION_RAW_BATCH_ITEMS = 20000
EVIDENCE_TIMELINE_PAGE_SIZE = 10000
MAX_INVESTIGATION_UPLOAD_BYTES = GIBIBYTE
UPLOAD_COPY_CHUNK_BYTES = MEBIBYTE
ARTIFACT_DETECTION_SAMPLE_BYTES = 64 * 1024
FILE_READ_CHUNK_BYTES = 64 * 1024
MAX_DIAGNOSTIC_RECORD_BYTES = MEBIBYTE
JSON_STREAM_BUFFER_BYTES = 64 * 1024
MAX_FINGERPRINT_LENGTH = 255
MAX_REPRESENTATIVE_LINE_BYTES = 64 * 1024 - 1

# Bounds for the SIMPLE-path Gemini context (build_simple_llm_context) -
# a large uncorrelated investigation can retain thousands of Evidence rows
# under bounded-memory ingestion, but that guarantee is worthless if every
# one of them is later dumped into a single Gemini request. The CORRELATED
# path is already implicitly bounded (roots_per_component + a small fixed
# number of additional source-matched nodes per component); these give
# SIMPLE mode an analogous explicit, deterministic bound instead of
# scattering magic numbers at the call site.
SIMPLE_LLM_MAX_EVIDENCE_RECORDS = 200
SIMPLE_LLM_MAX_CONTEXT_BYTES = 2 * MEBIBYTE

# iter_temporal_candidates' two-pointer sliding window is O(n) amortized
# only when events are temporally sparse relative to the window (the
# window naturally shrinks). A busy production service can easily emit
# thousands of events within any single 5-second window - in that dense
# case the window never shrinks and the per-event inner scan degrades
# toward O(n^2) candidate pairs (confirmed directly: ~2000 evidence rows
# packed into one 5-second window did not finish correlating within 60s).
# Capping each event to its nearest TEMPORAL_CANDIDATE_MAX_NEIGHBORS by
# time bounds total candidate-pair work to O(n * K) regardless of density,
# without weakening burst correlation: adjacent events' neighbor sets
# still overlap enough to transitively chain an entire dense burst into
# one connected component (see
# test_dense_temporal_burst_still_correlates_within_bounded_time).
TEMPORAL_CANDIDATE_MAX_NEIGHBORS = 40

# Zero-evidence SIMPLE unstructured fallback (Sections 9-11): a small,
# artifact-level bounded slice of "this looked like real diagnostic text"
# content, captured during an artifact's ORIGINAL ingestion pass (never a
# second read/re-OCR) and used only when the WHOLE analysis otherwise
# retains zero structured Evidence - never a parallel evidence model, and
# never large enough to look like "send the raw file to Gemini instead".
SIMPLE_FALLBACK_MAX_ARTIFACT_BYTES = 2 * MEBIBYTE
SIMPLE_FALLBACK_MAX_TEXT_BYTES = 64 * 1024
SIMPLE_FALLBACK_MAX_TOTAL_CONTEXT_BYTES = 256 * 1024

# Section 20: hard safety-net bound on how much evidence a single CORRELATED
# graph (and therefore its serialized SSE/frontend payload) is allowed to
# carry - the 1 GiB bounded-streaming-ingestion guarantee is meaningless if
# finalize later loads/serializes an unbounded Evidence table. Evidence
# stays fully persisted in MySQL regardless; this only bounds what one
# request/response cycle has to build and transmit. 5000 is comfortably
# above any real single-incident investigation's actual evidence volume
# (see test_correlation_engine.py's dense-burst benchmark: correlation
# itself remains well within a background Celery task's time budget at
# this scale, after the TEMPORAL_CANDIDATE_MAX_NEIGHBORS/enforce_dag fixes).
CORRELATED_MAX_EVIDENCE_RECORDS = 5000
CORRELATED_MAX_CONTEXT_BYTES = 20 * MEBIBYTE

# Section 21: same idea for the SIMPLE path's frontend/SSE result
# (build_simple_payload) - Gemini's own SIMPLE context is already bounded
# separately (SIMPLE_LLM_MAX_EVIDENCE_RECORDS, far smaller); this only
# protects the response/serialization size for a giant uncorrelated
# investigation. Evidence stays fully persisted in MySQL regardless.
SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS = 5000
SIMPLE_FRONTEND_MAX_CONTEXT_BYTES = 20 * MEBIBYTE

# Optional source-code input (GitHub URL or ZIP), kept separate from
# diagnostic artifact limits above.
SOURCE_STORAGE_ROOT = "uploads/sources"
MAX_SOURCE_ARCHIVE_BYTES = 200 * MEBIBYTE
MAX_SOURCE_TOTAL_BYTES = 500 * MEBIBYTE
MAX_SOURCE_FILES = 20_000
MAX_SOURCE_CONTEXT_FILE_BYTES = 5 * MEBIBYTE
# Total bytes of source-file content correlate_event() will keep cached
# across an analysis run, so repeated stack frames into the same hot file
# don't re-read/re-split it from disk each time. Bounded, not evicted.
SOURCE_CONTEXT_CACHE_BYTES = 32 * MEBIBYTE
SOURCE_CONTEXT_LINES = 15
GITHUB_CLONE_TIMEOUT_SECONDS = 30
