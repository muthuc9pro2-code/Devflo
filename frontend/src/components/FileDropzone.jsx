import { useCallback, useRef, useState } from 'react'

function formatBytes(bytes) {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / 1024 ** exponent
  return `${exponent === 0 ? value : value.toFixed(1)} ${units[exponent]}`
}

export default function FileDropzone({ files, onAddFiles, onRemoveFile }) {
  const inputRef = useRef(null)
  const [isDragging, setIsDragging] = useState(false)

  const handleFiles = useCallback(
    (fileList) => {
      const incoming = Array.from(fileList)
      if (incoming.length > 0) onAddFiles(incoming)
    },
    [onAddFiles],
  )

  const onDrop = useCallback(
    (event) => {
      event.preventDefault()
      setIsDragging(false)
      handleFiles(event.dataTransfer.files)
    },
    [handleFiles],
  )

  const onInputChange = useCallback(
    (event) => {
      handleFiles(event.target.files)
      event.target.value = ''
    },
    [handleFiles],
  )

  return (
    <div className="dropzone-wrapper">
      <div
        className={`dropzone${isDragging ? ' dropzone-active' : ''}`}
        onDrop={onDrop}
        onDragOver={(event) => {
          event.preventDefault()
          setIsDragging(true)
        }}
        onDragLeave={() => setIsDragging(false)}
        onClick={() => inputRef.current?.click()}
        role="button"
        tabIndex={0}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === ' ') inputRef.current?.click()
        }}
      >
        <input ref={inputRef} type="file" multiple className="dropzone-input" onChange={onInputChange} />
        <p className="dropzone-title">Drop diagnostic files here</p>
        <p className="dropzone-subtitle">
          or <span className="link-like">choose files</span>
        </p>
      </div>

      {files.length > 0 && (
        <ul className="file-list">
          {files.map((file, index) => (
            <li key={`${file.name}-${file.size}-${file.lastModified}-${index}`} className="file-item">
              <div className="file-info">
                <span className="file-name">{file.name}</span>
                <span className="file-size">{formatBytes(file.size)}</span>
              </div>
              <button
                type="button"
                className="btn-remove"
                onClick={(event) => {
                  event.stopPropagation()
                  onRemoveFile(index)
                }}
                aria-label={`Remove ${file.name}`}
              >
                ✕
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
