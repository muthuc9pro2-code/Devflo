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
