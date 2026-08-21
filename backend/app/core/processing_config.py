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

# Upload & OCR resource limits: separate from MAX_INVESTIGATION_UPLOAD_BYTES
# (the 1 GiB combined diagnostic byte budget, unchanged) - these instead
# bound artifact/task/DB fan-out (a huge batch of tiny files) and RapidOCR's
# own CPU/memory cost specifically, which the byte budget alone does not
# protect against on the 2 vCPU deployment target.
MAX_DIAGNOSTIC_ARTIFACTS = 200
MAX_OCR_IMAGES_PER_INVESTIGATION = 20
MAX_OCR_IMAGE_BYTES = 20 * MEBIBYTE
MAX_OCR_IMAGE_PIXELS = 25_000_000

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

# select_bounded_evidence_from_db's own Python-side aggregate lookups
# (repeated-fingerprint counts, cross-artifact trace/request-id bridges)
# still scale with the number of DISTINCT values in an analysis, not the
# bounded max_records above - bounded separately so a pathological analysis
# with huge fingerprint/identity cardinality (but few repeats/bridges each)
# still can't grow these Python dict/set aggregates unboundedly.
BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS = CORRELATED_MAX_EVIDENCE_RECORDS * 4

# iter_identity_candidate_pairs (correlation_engine.py): a shared identity
# (trace_id/request_id/resolved_identity/span_id) group at or under this
# size keeps today's full pairwise candidate behavior; a larger group falls
# back to pairing each row (in stable temporal order) with only its next
# IDENTITY_CANDIDATE_MAX_NEIGHBORS neighbors, bounding candidate-pair work
# to O(n*K) instead of O(n^2) for a giant shared-identity group while still
# keeping the whole group transitively connected as one component. Distinct
# from TEMPORAL_CANDIDATE_MAX_NEIGHBORS above, which bounds the SEPARATE
# temporal-fallback path only.
IDENTITY_FULL_PAIRWISE_MAX_GROUP_SIZE = 128
IDENTITY_CANDIDATE_MAX_NEIGHBORS = 32

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
GITHUB_CLONE_TIMEOUT_SECONDS = 60

# Bounded per-event budget for structured JSON fields that are real,
# diagnostically useful, but not one of the ~20 canonical fields Devflo
# already extracts (e.g. error_code/available_connections/max_pool_size on
# a real record) - preserved as ParsedEvent/Evidence.diagnostic_attributes
# instead of silently dropped, without ever retaining the whole raw event
# or adding per-field schema columns. Small and fixed: this is context for
# Gemini/humans, not a second evidence model.
DIAGNOSTIC_ATTRIBUTES_MAX_BYTES = 2 * 1024

# Small process-local cache of already-built SourceIndex objects, keyed by
# analysis_id - supplements (never replaces) the persisted index manifest
# on disk (see source_archive.py) so a single worker process handling
# several artifacts from the same analysis sequentially does not even need
# to re-read/re-parse that manifest file per artifact. Bounded so a worker
# that has processed many different analyses cannot grow this unboundedly.
SOURCE_INDEX_PROCESS_CACHE_MAX_ENTRIES = 8

# Section 10: the final byte budget for a CORRELATED investigation's
# Gemini context (build_llm_context), separate from and smaller than the
# frontend/SSE graph budget (CORRELATED_MAX_CONTEXT_BYTES above) - a large
# bounded graph (up to CORRELATED_MAX_EVIDENCE_RECORDS) can still be far
# more than Gemini needs to explain the incident well.
CORRELATED_GEMINI_CONTEXT_MAX_BYTES = 3 * MEBIBYTE
