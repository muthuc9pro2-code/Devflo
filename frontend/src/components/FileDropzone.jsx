import { useCallback, useEffect, useRef, useState } from 'react'
import { DIAGNOSTIC_ACCEPT, formatBytes } from '../utils/files'

function candidateFromFile(file, displayPath = '') {
  return {
    file,
    displayPath: displayPath || file.webkitRelativePath || file.name,
  }
}

function readDirectoryEntries(reader) {
  return new Promise((resolve, reject) => {
    const entries = []

    const readBatch = () => {
      reader.readEntries((batch) => {
        if (batch.length === 0) {
          resolve(entries)
          return
        }
        entries.push(...batch)
        readBatch()
      }, reject)
    }

    readBatch()
  })
}

async function walkEntry(entry) {
  if (entry.isFile) {
    const file = await new Promise((resolve, reject) => entry.file(resolve, reject))
    return [candidateFromFile(file, entry.fullPath?.replace(/^\//, ''))]
  }

  if (!entry.isDirectory) return []
  const children = await readDirectoryEntries(entry.createReader())
  const nested = await Promise.all(children.map(walkEntry))
  return nested.flat()
}

async function candidatesFromDrop(dataTransfer) {
  const items = Array.from(dataTransfer.items || []).filter((item) => item.kind === 'file')
  const entryCandidates = []
  const ordinaryFiles = []

  for (const item of items) {
    const entry = item.webkitGetAsEntry?.()
    if (entry) entryCandidates.push(entry)
    else {
      const file = item.getAsFile()
      if (file) ordinaryFiles.push(candidateFromFile(file))
    }
  }

  if (entryCandidates.length > 0) {
    const walked = await Promise.all(entryCandidates.map(walkEntry))
    return [...walked.flat(), ...ordinaryFiles]
  }

  return Array.from(dataTransfer.files || []).map((file) => candidateFromFile(file))
}

export default function FileDropzone({
  files,
  rejectedFiles,
  onAddFiles,
  onRemoveFile,
  onClearRejected,
  onScanningChange,
  disabled = false,
}) {
  const fileInputRef = useRef(null)
  const folderInputRef = useRef(null)
  const [isDragging, setIsDragging] = useState(false)
  const [isScanning, setIsScanning] = useState(false)
  const controlsDisabled = disabled || isScanning

  const handleFiles = useCallback(
    (fileList) => {
      if (controlsDisabled) return
      const incoming = Array.from(fileList).map((file) => candidateFromFile(file))
      if (incoming.length > 0) onAddFiles(incoming)
    },
    [controlsDisabled, onAddFiles],
  )

  const onDrop = useCallback(
    async (event) => {
      event.preventDefault()
      if (controlsDisabled) return
      setIsDragging(false)
      setIsScanning(true)
      onScanningChange?.(true)
      const fallback = Array.from(event.dataTransfer.files || []).map((file) => candidateFromFile(file))
      try {
        const candidates = await candidatesFromDrop(event.dataTransfer)
        if (candidates.length > 0) onAddFiles(candidates)
      } catch {
        if (fallback.length > 0) onAddFiles(fallback)
      } finally {
        setIsScanning(false)
        onScanningChange?.(false)
      }
    },
    [controlsDisabled, onAddFiles, onScanningChange],
  )

  const onInputChange = useCallback(
    (event) => {
      handleFiles(event.target.files)
      event.target.value = ''
    },
    [handleFiles],
  )

  // Lets a user copy files in their OS file manager and paste (Ctrl/Cmd+V)
  // them straight in, without needing to focus the dropzone first. Scoped to
  // document so it works regardless of which element has focus, but it's a
  // no-op unless the clipboard actually carries files - pasting text into
  // another field (e.g. the GitHub URL input) is unaffected.
  useEffect(() => {
    const onPaste = (event) => {
      const pastedFiles = event.clipboardData?.files
      if (controlsDisabled || !pastedFiles || pastedFiles.length === 0) return
      event.preventDefault()
      handleFiles(pastedFiles)
    }
    document.addEventListener('paste', onPaste)
    return () => document.removeEventListener('paste', onPaste)
  }, [controlsDisabled, handleFiles])

  return (
    <div className={`dropzone-wrapper${disabled ? ' selection-disabled' : ''}`}>
      <div
        className={`dropzone${isDragging ? ' dropzone-active' : ''}`}
        onDrop={onDrop}
        onDragOver={(event) => {
          event.preventDefault()
          if (!controlsDisabled) setIsDragging(true)
        }}
        onDragLeave={() => setIsDragging(false)}
        aria-label="Diagnostic file drop area"
        aria-disabled={disabled}
      >
        <input
          ref={fileInputRef}
          type="file"
          multiple
          accept={DIAGNOSTIC_ACCEPT}
          className="dropzone-input"
          onChange={onInputChange}
          disabled={controlsDisabled}
        />
        <input
          ref={folderInputRef}
          type="file"
          multiple
          webkitdirectory=""
          className="dropzone-input"
          onChange={onInputChange}
          disabled={controlsDisabled}
        />
        <div className="dropzone-mark" aria-hidden="true">↓</div>
        <p className="dropzone-title">
          {isScanning ? 'Scanning folder…' : 'Drop diagnostics or a folder'}
        </p>
        <p className="dropzone-subtitle">Logs, traces, JSON, HAR and diagnostic screenshots</p>
        <div className="dropzone-actions">
          <button type="button" className="btn-secondary" disabled={controlsDisabled} onClick={() => fileInputRef.current?.click()}>
            Choose files
          </button>
          <button type="button" className="btn-secondary" disabled={controlsDisabled} onClick={() => folderInputRef.current?.click()}>
            Choose folder
          </button>
        </div>
        <p className="dropzone-paste">Paste files with Ctrl/Cmd+V</p>
      </div>

      {files.length > 0 && (
        <section className="selection-section" aria-labelledby="selected-diagnostics-heading">
          <div className="selection-heading">
            <h2 id="selected-diagnostics-heading">Selected diagnostics</h2>
            <span>{files.length}</span>
          </div>
          <ul className="file-list">
          {files.map((selection, index) => (
            <li key={selection.key} className="file-item">
              <div className="file-info">
                <span className="file-name" title={selection.displayPath}>{selection.displayPath}</span>
                <span className="file-size">{formatBytes(selection.file.size)}</span>
              </div>
              <button
                type="button"
                className="btn-remove"
                disabled={disabled}
                onClick={(event) => {
                  event.stopPropagation()
                  onRemoveFile(index)
                }}
                aria-label={`Remove ${selection.displayPath}`}
              >
                <span aria-hidden="true">×</span>
              </button>
            </li>
          ))}
          </ul>
        </section>
      )}

      {rejectedFiles.length > 0 && (
        <section className="selection-section rejected-section" aria-labelledby="rejected-diagnostics-heading">
          <div className="selection-heading">
            <h2 id="rejected-diagnostics-heading">Skipped files</h2>
            <button type="button" className="text-button" disabled={disabled} onClick={onClearRejected}>Clear</button>
          </div>
          <ul className="file-list rejected-list">
            {rejectedFiles.map((rejected) => (
              <li key={rejected.id} className="file-item">
                <div className="file-info">
                  <span className="file-name" title={rejected.displayPath}>{rejected.displayPath}</span>
                  <span className="file-reason">{rejected.reason}</span>
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  )
}
