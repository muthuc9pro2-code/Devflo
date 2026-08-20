export function isPresent(value) {
  return value !== null && value !== undefined && value !== ''
}

export function humanize(value) {
  if (!isPresent(value)) return ''
  return String(value)
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
}

export function formatRelativeMs(value, { signed = true } = {}) {
  if (!Number.isFinite(Number(value))) return null
  const numeric = Number(value)
  const absolute = Math.abs(numeric)
  const precision = Number.isInteger(absolute) ? 0 : Math.min(3, String(absolute).split('.')[1]?.length || 0)
  const formatted = absolute.toFixed(precision)
  if (!signed) return `${formatted} ms`
  if (numeric === 0) return '0 ms'
  return `${numeric < 0 ? '−' : '+'}${formatted} ms`
}

export function formatTimestamp(value) {
  if (!value) return null
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium',
  }).format(date)
}
