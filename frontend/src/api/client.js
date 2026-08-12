export class ApiError extends Error {
  constructor(message, status) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function parseErrorMessage(response) {
  try {
    const data = await response.json()
    if (Array.isArray(data.detail)) {
      return data.detail.map((item) => item.msg).join('; ')
    }
    if (typeof data.detail === 'string') {
      return data.detail
    }
  } catch {
    // response had no JSON body
  }
  return response.statusText || `Request failed with status ${response.status}`
}

async function rawRequest(path, options) {
  let response
  try {
    response = await fetch(path, {
      ...options,
      credentials: 'include',
      headers:
        options.body instanceof FormData
          ? options.headers
          : { 'Content-Type': 'application/json', ...options.headers },
    })
  } catch {
    throw new ApiError('Unable to reach the Devflo server. Is the backend running?', 0)
  }
  return response
}

// Not-authenticated paths where a 401 means "bad credentials", not "expired
// session" — retrying them via /auth/refresh would be meaningless.
const NO_REFRESH_RETRY = new Set(['/auth/login', '/auth/refresh', '/auth/register'])

export async function request(path, options = {}) {
  const response = await rawRequest(path, options)

  if (response.status === 401 && !NO_REFRESH_RETRY.has(path)) {
    const refreshResponse = await rawRequest('/auth/refresh', { method: 'POST' })
    if (refreshResponse.ok) {
      const retryResponse = await rawRequest(path, options)
      if (!retryResponse.ok) {
        throw new ApiError(await parseErrorMessage(retryResponse), retryResponse.status)
      }
      return retryResponse.status === 204 ? null : retryResponse.json()
    }
  }

  if (!response.ok) {
    throw new ApiError(await parseErrorMessage(response), response.status)
  }

  return response.status === 204 ? null : response.json()
}
