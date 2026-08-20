import { useCallback, useRef, useState } from 'react'
import { formatBytes } from '../utils/files'

// Either-or: a source ZIP and a GitHub URL can't both be sent (the backend
// rejects the request if both are present), so picking one clears the other.
export default function SourceCodeSection({
  sourceZip,
  onSourceZipChange,
  githubUrl,
  onGithubUrlChange,
  error,
  disabled = false,
}) {
  const inputRef = useRef(null)
  const [isDragging, setIsDragging] = useState(false)

  const zipDisabled = disabled || githubUrl.trim().length > 0
  const urlDisabled = disabled || sourceZip !== null

  const handleZipFile = useCallback(
    (file) => {
      if (!file) return
      onSourceZipChange(file)
    },
    [onSourceZipChange],
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
    <section className={`source-section${disabled ? ' selection-disabled' : ''}`} aria-disabled={disabled}>
      <div className="source-heading">
        <h2>Optional source code</h2>
        <span className="optional-badge">Optional</span>
      </div>
      <p className="source-subtitle">
        Match diagnostic locations to your repository. Provide a repository ZIP or an HTTPS GitHub URL.
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
                disabled={disabled}
                onClick={() => onSourceZipChange(null)}
                aria-label="Remove source ZIP"
              >
                <span aria-hidden="true">×</span>
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
                if (!zipDisabled && (event.key === 'Enter' || event.key === ' ')) {
                  if (event.key === ' ') event.preventDefault()
                  inputRef.current?.click()
                }
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
              <p className="dropzone-title">Repository ZIP</p>
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
            GitHub repository URL
            <input
              id="github-url"
              type="url"
              placeholder="https://github.com/owner/repo"
              value={githubUrl}
              disabled={urlDisabled}
              onChange={(event) => onGithubUrlChange(event.target.value)}
            />
          </label>
        </div>
      </div>
      {error && <p className="field-error" role="alert">{error}</p>}
    </section>
  )
}
