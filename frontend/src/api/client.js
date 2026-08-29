export class ApiError extends Error {
  constructor(message, status, code = null) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

async function parseError(response) {
  try {
    const data = await response.json()
    let message
    if (Array.isArray(data.detail)) {
      message = data.detail.map((item) => item.msg).join('; ')
    } else if (typeof data.detail === 'string') {
      message = data.detail
    }
    return {
      message: message || response.statusText || `Request failed with status ${response.status}`,
      code: typeof data.code === 'string' ? data.code : null,
    }
  } catch {
    // response had no JSON body
  }
  return {
    message: response.statusText || `Request failed with status ${response.status}`,
    code: null,
  }
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
const NO_REFRESH_RETRY = new Set([
  '/auth/login',
  '/auth/logout',
  '/auth/refresh',
  '/auth/register',
  '/auth/verification-session',
  '/auth/verify-email',
  '/auth/forgot-password',
  '/auth/reset-password',
])

let refreshPromise = null
let sessionEpoch = 0
let logoutPending = false

function reportExpiredSession() {
  sessionEpoch += 1
  window.dispatchEvent(new Event('devflo:session-expired'))
}

// One centralized signal for a controlled backend 503 (core DB/service
// unavailable - see main.py's OperationalError handler), so individual
// pages/components never need their own DB-specific error handling. The
// top-level Auth/App layer listens for this and renders one global
// "temporarily unavailable" screen instead.
function reportServiceUnavailable() {
  window.dispatchEvent(new Event('devflo:service-unavailable'))
}

async function throwApiError(response) {
  const error = await parseError(response)
  if (response.status === 503 && error.code === 'service_unavailable') {
    reportServiceUnavailable()
  }
  throw new ApiError(error.message, response.status, error.code)
}

function refreshAccessCookie() {
  if (logoutPending) return null
  if (refreshPromise) return refreshPromise

  const pending = rawRequest('/auth/refresh', { method: 'POST' })
  const tracked = pending.finally(() => {
    if (refreshPromise === tracked) refreshPromise = null
  })
  refreshPromise = tracked
  return refreshPromise
}

export function markSessionEstablished() {
  logoutPending = false
  sessionEpoch += 1
}

export async function prepareForLogout() {
  logoutPending = true
  sessionEpoch += 1
  if (!refreshPromise) return
  try {
    await refreshPromise
  } catch {
    // Logout still needs to run last when a refresh request failed.
  }
}

export function finishLogout() {
  sessionEpoch += 1
  logoutPending = false
}

export async function request(path, options = {}) {
  const requestEpoch = sessionEpoch
  const response = await rawRequest(path, options)

  if (
    response.status === 401
    && !NO_REFRESH_RETRY.has(path)
    && !logoutPending
    && requestEpoch === sessionEpoch
  ) {
    const refreshResponse = await refreshAccessCookie()
    if (requestEpoch !== sessionEpoch || logoutPending || !refreshResponse) {
      const error = await parseError(response)
      throw new ApiError(error.message, response.status, error.code)
    }
    if (refreshResponse.ok) {
      const retryResponse = await rawRequest(path, options)
      if (!retryResponse.ok) {
        if (retryResponse.status === 401 && requestEpoch === sessionEpoch && !logoutPending) {
          reportExpiredSession()
        }
        await throwApiError(retryResponse)
      }
      return retryResponse.status === 204 ? null : retryResponse.json()
    }
    if (refreshResponse.status === 401 || refreshResponse.status === 403) {
      reportExpiredSession()
    }
    await throwApiError(refreshResponse)
  }

  if (!response.ok) {
    await throwApiError(response)
  }

  return response.status === 204 ? null : response.json()
}
