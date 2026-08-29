import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { uploadFiles } from '../api/analysis'
import { ApiError } from '../api/client'
import FileDropzone from '../components/FileDropzone'
import SourceCodeSection from '../components/SourceCodeSection'
import {
  diagnosticRejectionReason,
  formatBytes,
  isSupportedDiagnosticFilename,
  isSupportedImageFilename,
  MAX_DIAGNOSTIC_ARTIFACTS,
  MAX_DIAGNOSTIC_BYTES,
  MAX_OCR_IMAGE_BYTES,
  MAX_OCR_IMAGES_PER_INVESTIGATION,
  MAX_SOURCE_ZIP_BYTES,
  selectionKey,
} from '../utils/files'

const GITHUB_REPOSITORY_URL = /^https:\/\/github\.com\/[\w.-]+\/[\w.-]+?(?:\.git)?\/?$/

function persistUploadManifest(analysisId, selections) {
  try {
    sessionStorage.setItem(
      `devflo:analysis:${analysisId}`,
      JSON.stringify({ files: selections.map((selection) => selection.displayPath) }),
    )
  } catch {
    // The analysis remains fully usable when session storage is unavailable.
  }
}

export default function NewInvestigationPage({ onUploaded, onUploadingChange }) {
  const [files, setFiles] = useState([])
  const filesRef = useRef([])
  const [rejectedFiles, setRejectedFiles] = useState([])
  const [sourceZip, setSourceZip] = useState(null)
  const [githubUrl, setGithubUrl] = useState('')
  const [sourceError, setSourceError] = useState('')
  const [uploadState, setUploadState] = useState('idle')
  const [errorMessage, setErrorMessage] = useState('')
  const [selectionScanning, setSelectionScanning] = useState(false)
  const mountedRef = useRef(true)
  const uploadInFlightRef = useRef(false)
  const selectionScanningRef = useRef(false)

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
    }
  }, [])

  const totalBytes = useMemo(
    () => files.reduce((total, selection) => total + selection.file.size, 0),
    [files],
  )
  const diagnosticsTooLarge = totalBytes > MAX_DIAGNOSTIC_BYTES

  const imageCount = useMemo(
    () => files.filter((selection) => isSupportedImageFilename(selection.file.name)).length,
    [files],
  )
  const oversizedImage = useMemo(
    () => files.find((selection) => (
      isSupportedImageFilename(selection.file.name) && selection.file.size > MAX_OCR_IMAGE_BYTES
    )),
    [files],
  )
  const tooManyFiles = files.length > MAX_DIAGNOSTIC_ARTIFACTS
  const tooManyImages = imageCount > MAX_OCR_IMAGES_PER_INVESTIGATION

  const selectionLimitReason = useMemo(() => {
    if (tooManyFiles) {
      return `Selected ${files.length} files exceed the ${MAX_DIAGNOSTIC_ARTIFACTS}-file limit per investigation.`
    }
    if (tooManyImages) {
      return `Selected ${imageCount} images exceed the ${MAX_OCR_IMAGES_PER_INVESTIGATION}-image limit per investigation.`
    }
    if (oversizedImage) {
      return `${oversizedImage.displayPath} exceeds the ${formatBytes(MAX_OCR_IMAGE_BYTES)} per-image limit.`
    }
    return ''
  }, [tooManyFiles, tooManyImages, oversizedImage, files.length, imageCount])

  const handleScanningChange = useCallback((scanning) => {
    selectionScanningRef.current = scanning
    setSelectionScanning(scanning)
  }, [])

  const addFiles = useCallback((incoming) => {
    if (uploadInFlightRef.current) return
    const next = [...filesRef.current]
    const knownKeys = new Set(next.map((selection) => selection.key))
    const rejected = []

    incoming.forEach((candidate, index) => {
      const file = candidate.file || candidate
      const displayPath = candidate.displayPath || file.webkitRelativePath || file.name
      const normalized = { file, displayPath }
      const key = selectionKey(normalized)

      if (!isSupportedDiagnosticFilename(file.name)) {
        rejected.push({
          id: `${key}:unsupported:${index}:${Date.now()}`,
          displayPath,
          reason: diagnosticRejectionReason(file.name),
        })
        return
      }

      if (knownKeys.has(key)) {
        rejected.push({
          id: `${key}:duplicate:${index}:${Date.now()}`,
          displayPath,
          reason: 'Already selected',
        })
        return
      }

      knownKeys.add(key)
      next.push({ ...normalized, key })
    })

    filesRef.current = next
    setFiles(next)
    if (rejected.length > 0) setRejectedFiles((current) => [...current, ...rejected])
    setUploadState('idle')
    setErrorMessage('')
  }, [])

  const removeFile = useCallback((index) => {
    if (uploadInFlightRef.current) return
    const next = filesRef.current.filter((_, currentIndex) => currentIndex !== index)
    filesRef.current = next
    setFiles(next)
    setErrorMessage('')
  }, [])

  const handleSourceZipChange = useCallback((file) => {
    if (uploadInFlightRef.current) return
    if (file === null) {
      setSourceZip(null)
      setSourceError('')
      return
    }

    if (!/\.zip$/i.test(file.name)) {
      setSourceZip(null)
      setSourceError('Source code must be provided as a .zip repository archive.')
      return
    }

    if (file.size > MAX_SOURCE_ZIP_BYTES) {
      setSourceZip(null)
      setSourceError(`Source ZIPs may not exceed ${formatBytes(MAX_SOURCE_ZIP_BYTES)}.`)
      return
    }

    setSourceZip(file)
    setGithubUrl('')
    setSourceError('')
  }, [])

  const handleGithubUrlChange = useCallback((value) => {
    if (uploadInFlightRef.current) return
    setGithubUrl(value)
    setSourceError('')
  }, [])

  const startInvestigation = async () => {
    if (uploadInFlightRef.current) return
    if (selectionScanningRef.current) return
    if (files.length === 0) {
      setErrorMessage('Select at least one supported diagnostic artifact first.')
      return
    }
    if (diagnosticsTooLarge) {
      setErrorMessage('Selected diagnostics exceed the 1 GiB combined upload limit.')
      return
    }
    if (selectionLimitReason) {
      setErrorMessage(selectionLimitReason)
      return
    }

    const repositoryUrl = githubUrl.trim()
    if (repositoryUrl && !GITHUB_REPOSITORY_URL.test(repositoryUrl)) {
      setSourceError('Enter an HTTPS github.com repository URL, such as https://github.com/owner/repo.')
      return
    }

    uploadInFlightRef.current = true
    setUploadState('uploading')
    setErrorMessage('')
    onUploadingChange?.(true)

    try {
      const result = await uploadFiles(
        files.map((selection) => selection.file),
        { githubUrl: repositoryUrl, sourceZip },
      )
      persistUploadManifest(result.id, files)
      window.dispatchEvent(new Event('devflo:history-refresh'))
      // Keep the lock held through the handoff: onUploaded() triggers
      // AppShell's navigate() first, and releasing the lock right after
      // (same synchronous tick, so React batches both) means navigation
      // and unlock land in the same render - never a frame where controls
      // are enabled while this page is still the one on screen.
      if (mountedRef.current) onUploaded(result)
      onUploadingChange?.(false)
    } catch (error) {
      if (mountedRef.current) {
        setUploadState('error')
        setErrorMessage(
          error instanceof ApiError
            ? error.message
            : 'Devflo could not start this investigation. Please try again.',
        )
      }
      onUploadingChange?.(false)
    } finally {
      uploadInFlightRef.current = false
    }
  }

  return (
    <div className="new-investigation-page view-enter">
      <header className="new-investigation-header">
        <p className="eyebrow">New investigation</p>
        <h1>Investigate a backend incident</h1>
        <p>Bring together diagnostics, traces and screenshots without changing how your systems run.</p>
      </header>

      <FileDropzone
        files={files}
        rejectedFiles={rejectedFiles}
        onAddFiles={addFiles}
        onRemoveFile={removeFile}
        onClearRejected={() => setRejectedFiles([])}
        onScanningChange={handleScanningChange}
        disabled={uploadState === 'uploading'}
      />

      <SourceCodeSection
        sourceZip={sourceZip}
        onSourceZipChange={handleSourceZipChange}
        githubUrl={githubUrl}
        onGithubUrlChange={handleGithubUrlChange}
        error={sourceError}
        disabled={uploadState === 'uploading'}
      />

      {diagnosticsTooLarge && (
        <div className="alert-error" role="alert">
          Selected diagnostics total {formatBytes(totalBytes)} and exceed the 1 GiB limit.
        </div>
      )}
      {!diagnosticsTooLarge && selectionLimitReason && (
        <div className="alert-error" role="alert">{selectionLimitReason}</div>
      )}
      {errorMessage && <div className="alert-error" role="alert">{errorMessage}</div>}
      {uploadState === 'uploading' && (
        <div className="upload-status" role="status">
          <span className="spinner" aria-hidden="true" />
          <span>Uploading diagnostics…</span>
        </div>
      )}

      <div className="action-row">
        <div className="selection-total">
          {files.length > 0 ? `${files.length} file${files.length === 1 ? '' : 's'} · ${formatBytes(totalBytes)}` : 'No diagnostics selected'}
        </div>
        <button
          className="btn-primary"
          type="button"
          onClick={startInvestigation}
          disabled={
            uploadState === 'uploading'
            || selectionScanning
            || diagnosticsTooLarge
            || Boolean(selectionLimitReason)
          }
        >
          {uploadState === 'uploading'
            ? 'Uploading…'
            : (selectionScanning ? 'Scanning folder…' : 'Start investigation')}
        </button>
      </div>
    </div>
  )
}
