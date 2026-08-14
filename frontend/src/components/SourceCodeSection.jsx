import { useCallback, useRef, useState } from 'react'

function formatBytes(bytes) {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / 1024 ** exponent
  return `${exponent === 0 ? value : value.toFixed(1)} ${units[exponent]}`
}

// Either-or: a source ZIP and a GitHub URL can't both be sent (the backend
// rejects the request if both are present), so picking one clears the other.
export default function SourceCodeSection({ sourceZip, onSourceZipChange, githubUrl, onGithubUrlChange }) {
  const inputRef = useRef(null)
  const [isDragging, setIsDragging] = useState(false)

  const zipDisabled = githubUrl.trim().length > 0
  const urlDisabled = sourceZip !== null

  const handleZipFile = useCallback(
    (file) => {
      if (!file) return
      onSourceZipChange(file)
      onGithubUrlChange('')
    },
    [onSourceZipChange, onGithubUrlChange],
  )

  const onDrop = useCallback(
    (event) => {
      event.preventDefault()
      setIsDragging(false)
      if (zipDisabled) return
      handleZipFile(event.dataTransfer.files?.[0] ?? null)
    },
    [handleZipFile, zipDisabled],
  )

  const onInputChange = useCallback(
    (event) => {
      handleZipFile(event.target.files?.[0] ?? null)
      event.target.value = ''
    },
    [handleZipFile],
  )

  return (
    <section className="source-section">
      <div className="source-heading">
        <h2>Add source code</h2>
        <span className="optional-badge">Optional</span>
      </div>
      <p className="source-subtitle">
        Improves correlation accuracy by matching stack traces to your actual source files. Provide
        either a ZIP of your repository or a GitHub URL - not both.
      </p>

      <div className="source-options">
        <div className={`source-option${zipDisabled ? ' source-option-disabled' : ''}`}>
          {sourceZip ? (
            <div className="file-item">
              <div className="file-info">
                <span className="file-name">{sourceZip.name}</span>
                <span className="file-size">{formatBytes(sourceZip.size)}</span>
              </div>
              <button
                type="button"
                className="btn-remove"
                onClick={() => onSourceZipChange(null)}
                aria-label="Remove source ZIP"
              >
                ✕
              </button>
            </div>
          ) : (
            <div
              className={`dropzone dropzone-compact${isDragging ? ' dropzone-active' : ''}`}
              onDrop={onDrop}
              onDragOver={(event) => {
                event.preventDefault()
                if (!zipDisabled) setIsDragging(true)
              }}
              onDragLeave={() => setIsDragging(false)}
              onClick={() => !zipDisabled && inputRef.current?.click()}
              role="button"
              tabIndex={zipDisabled ? -1 : 0}
              aria-disabled={zipDisabled}
              onKeyDown={(event) => {
                if (!zipDisabled && (event.key === 'Enter' || event.key === ' ')) inputRef.current?.click()
              }}
            >
              <input
                ref={inputRef}
                type="file"
                accept=".zip"
                className="dropzone-input"
                onChange={onInputChange}
                disabled={zipDisabled}
              />
              <p className="dropzone-title">Drop a source code ZIP here</p>
              <p className="dropzone-subtitle">
                or <span className="link-like">choose a file</span>
              </p>
            </div>
          )}
        </div>

        <div className="source-divider">
          <span>or</span>
        </div>

        <div className={`source-option${urlDisabled ? ' source-option-disabled' : ''}`}>
          <label className="field" htmlFor="github-url">
            paste a GitHub repo URL
            <input
              id="github-url"
              type="text"
              placeholder="https://github.com/owner/repo"
              value={githubUrl}
              disabled={urlDisabled}
              onChange={(event) => onGithubUrlChange(event.target.value)}
            />
          </label>
        </div>
      </div>
    </section>
  )
}
