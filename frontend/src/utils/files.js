export const DIAGNOSTIC_ACCEPT = [
  '.log',
  '.txt',
  '.json',
  '.jsonl',
  '.ndjson',
  '.har',
  '.out',
  '.err',
  '.png',
  '.jpg',
  '.jpeg',
  '.webp',
].join(',')

export const MAX_DIAGNOSTIC_BYTES = 1024 ** 3
export const MAX_SOURCE_ZIP_BYTES = 200 * 1024 ** 2

// Mirrors backend app/core/processing_config.py's upload/OCR resource
// limits (MAX_DIAGNOSTIC_ARTIFACTS, MAX_OCR_IMAGES_PER_INVESTIGATION,
// MAX_OCR_IMAGE_BYTES) - only the ones cheaply knowable from browser File
// metadata (count, name, size). The 25 MP decoded-pixel check stays
// backend-authoritative; the browser never decodes every selected image
// just to count pixels.
export const MAX_DIAGNOSTIC_ARTIFACTS = 200
export const MAX_OCR_IMAGES_PER_INVESTIGATION = 20
export const MAX_OCR_IMAGE_BYTES = 20 * 1024 ** 2

const EXPLICIT_DIAGNOSTIC_SUFFIX = /\.(?:log|txt|json|jsonl|ndjson|har|out|err|png|jpe?g|webp)$/i
const ROTATED_TEXT_SUFFIX = /\.(?:log|txt|json|jsonl|ndjson|har|out|err)\.\d+$/i
const SUPPORTED_IMAGE_SUFFIX = /\.(?:png|jpe?g|webp)$/i

export function isSupportedDiagnosticFilename(filename) {
  return EXPLICIT_DIAGNOSTIC_SUFFIX.test(filename) || ROTATED_TEXT_SUFFIX.test(filename)
}

export function isSupportedImageFilename(filename) {
  return SUPPORTED_IMAGE_SUFFIX.test(filename)
}

export function diagnosticRejectionReason(filename) {
  if (/\.zip$/i.test(filename)) {
    return 'ZIP files are not diagnostic artifacts. Use Optional source code for a repository ZIP.'
  }
  return 'Unsupported diagnostic file'
}

export function selectionKey({ file }) {
  return `${file.name}:${file.size}:${file.lastModified}`
}

export function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / 1024 ** exponent
  return `${exponent === 0 ? value : value.toFixed(1)} ${units[exponent]}`
}
