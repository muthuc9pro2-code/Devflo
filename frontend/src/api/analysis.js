import { request } from './client'

// The backend accepts one or more repeated `file` parts (Annotated[list[UploadFile], File()]),
// plus an optional `github_url` Form field or an optional `source_zip` File part - the two are
// mutually exclusive (POST /analysis/upload rejects the request if both are present).
export function uploadFiles(files, source = {}) {
  const formData = new FormData()
  for (const file of files) {
    formData.append('file', file)
  }
  if (source.githubUrl) {
    formData.append('github_url', source.githubUrl)
  } else if (source.sourceZip) {
    formData.append('source_zip', source.sourceZip)
  }
  return request('/analysis/upload', {
    method: 'POST',
    body: formData,
  })
}

export function getAnalysisHistory({ cursor, limit = 20 } = {}) {
  const params = new URLSearchParams({ limit: String(limit) })
  if (cursor) params.set('cursor', cursor)
  return request(`/analysis/history?${params.toString()}`)
}

export function getAnalysisDetail(analysisId) {
  return request(`/analysis/${encodeURIComponent(analysisId)}`)
}

function parseEventData(event) {
  try {
    return JSON.parse(event.data)
  } catch {
    return null
  }
}

export function subscribeToAnalysisEvents(
  analysisId,
  { onState, onProgress, onArtifactOutcome, onResult, onOpen, onError },
) {
  const source = new EventSource(
    `/analyses/${encodeURIComponent(analysisId)}/events`,
    { withCredentials: true },
  )

  const listen = (name, handler) => {
    source.addEventListener(name, (event) => {
      const data = parseEventData(event)
      if (data !== null) handler?.(data)
    })
  }

  listen('state', onState)
  listen('progress', onProgress)
  listen('artifact_outcome', onArtifactOutcome)
  listen('investigation_result', onResult)
  source.onopen = () => onOpen?.()
  source.onerror = (event) => onError?.(event)

  return () => source.close()
}
