import { useCallback, useState } from 'react'
import { useAuth } from '../context/useAuth'
import { uploadFiles } from '../api/analysis'
import { ApiError } from '../api/client'
import FileDropzone from '../components/FileDropzone'
import SourceCodeSection from '../components/SourceCodeSection'

function fileKey(file) {
  return `${file.name}-${file.size}-${file.lastModified}`
}

export default function DashboardPage() {
  const { user, logout } = useAuth()
  const [files, setFiles] = useState([])
  const [sourceZip, setSourceZip] = useState(null)
  const [githubUrl, setGithubUrl] = useState('')
  const [uploadState, setUploadState] = useState('idle') // idle | uploading | error | started
  const [errorMessage, setErrorMessage] = useState('')
  const [analysis, setAnalysis] = useState(null)

  const addFiles = useCallback((incoming) => {
    setFiles((prev) => {
      const existingKeys = new Set(prev.map(fileKey))
      const additions = incoming.filter((file) => !existingKeys.has(fileKey(file)))
      return [...prev, ...additions]
    })
    setUploadState('idle')
    setErrorMessage('')
  }, [])

  const removeFile = useCallback((index) => {
    setFiles((prev) => prev.filter((_, i) => i !== index))
  }, [])

  const startInvestigation = async () => {
    if (files.length === 0) {
      setUploadState('error')
      setErrorMessage('Select at least one diagnostic file first.')
      return
    }

    setUploadState('uploading')
    setErrorMessage('')

    try {
      const result = await uploadFiles(files, { githubUrl, sourceZip })
      setAnalysis(result)
      setUploadState('started')
      setFiles([])
      setSourceZip(null)
      setGithubUrl('')
    } catch (err) {
      setUploadState('error')
      setErrorMessage(err instanceof ApiError ? err.message : 'Something went wrong. Please try again.')
    }
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <span className="brand">Devflo</span>
        <div className="header-right">
          <span className="user-email">{user?.email}</span>
          <button className="btn-ghost" onClick={logout}>
            Log out
          </button>
        </div>
      </header>

      <main className="main-content">
        <h1>New investigation</h1>
        <p className="subtitle">Upload diagnostic artifacts to start an investigation.</p>

        <FileDropzone files={files} onAddFiles={addFiles} onRemoveFile={removeFile} />

        {files.length > 0 && (
          <SourceCodeSection
            sourceZip={sourceZip}
            onSourceZipChange={setSourceZip}
            githubUrl={githubUrl}
            onGithubUrlChange={setGithubUrl}
          />
        )}

        {errorMessage && <div className="alert-error">{errorMessage}</div>}

        <div className="action-row">
          <button className="btn-primary" onClick={startInvestigation} disabled={uploadState === 'uploading'}>
            {uploadState === 'uploading' ? 'Uploading…' : 'Start Investigation'}
          </button>
        </div>

        {analysis && (
          <div className="analysis-result">
            <h2>Investigation started</h2>
            <dl>
              <dt>ID</dt>
              <dd>{analysis.id}</dd>
              <dt>File</dt>
              <dd>{analysis.original_filename}</dd>
              <dt>Status</dt>
              <dd>
                <span className={`status-pill status-${analysis.status}`}>{analysis.status}</span>
              </dd>
              <dt>Created</dt>
              <dd>{new Date(analysis.created_at).toLocaleString()}</dd>
            </dl>
            <p className="hint">
              The backend does not currently expose an endpoint to poll investigation progress or
              results, so this reflects the state returned when the investigation was created.
            </p>
          </div>
        )}
      </main>
    </div>
  )
}
