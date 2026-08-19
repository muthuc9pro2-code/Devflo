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
