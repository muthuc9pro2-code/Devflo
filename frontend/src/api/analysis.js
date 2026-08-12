import { request } from './client'

// The backend accepts one or more repeated `file` parts (Annotated[list[UploadFile], File()]).
export function uploadFiles(files) {
  const formData = new FormData()
  for (const file of files) {
    formData.append('file', file)
  }
  return request('/analysis/upload', {
    method: 'POST',
    body: formData,
  })
}
