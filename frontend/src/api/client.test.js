import { afterEach, describe, expect, it, vi } from 'vitest'

describe('verification handoff API retry behavior', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.resetModules()
  })

  it('does not try refresh-token recovery when the handoff cookie is unavailable', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      statusText: 'Unauthorized',
      json: vi.fn().mockResolvedValue({
        detail: 'Verification handoff unavailable or expired',
      }),
    })
    vi.stubGlobal('fetch', fetchMock)
    const { request } = await import('./client')

    await expect(
      request('/auth/verification-session', { method: 'POST' }),
    ).rejects.toMatchObject({ status: 401 })

    expect(fetchMock).toHaveBeenCalledOnce()
    expect(fetchMock).toHaveBeenCalledWith('/auth/verification-session', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
    })
  })

  it('reports a definitive refresh rejection as an expired session', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        status: 401,
        ok: false,
        statusText: 'Unauthorized',
        json: vi.fn().mockResolvedValue({ detail: 'Invalid access token' }),
      })
      .mockResolvedValueOnce({
        status: 401,
        ok: false,
        statusText: 'Unauthorized',
        json: vi.fn().mockResolvedValue({ detail: 'Invalid refresh token' }),
      })
    vi.stubGlobal('fetch', fetchMock)
    const expired = vi.fn()
    window.addEventListener('devflo:session-expired', expired)
    const { request } = await import('./client')

    await expect(request('/analysis/history')).rejects.toMatchObject({ status: 401 })

    expect(expired).toHaveBeenCalledOnce()
    window.removeEventListener('devflo:session-expired', expired)
  })

  it('does not expire the frontend session for a temporary refresh 503', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        status: 401,
        ok: false,
        statusText: 'Unauthorized',
        json: vi.fn().mockResolvedValue({ detail: 'Invalid access token' }),
      })
      .mockResolvedValueOnce({
        status: 503,
        ok: false,
        statusText: 'Service Unavailable',
        json: vi.fn().mockResolvedValue({ detail: 'Temporarily unavailable' }),
      })
    vi.stubGlobal('fetch', fetchMock)
    const expired = vi.fn()
    window.addEventListener('devflo:session-expired', expired)
    const { request } = await import('./client')

    await expect(request('/analysis/history')).rejects.toMatchObject({ status: 503 })

    expect(expired).not.toHaveBeenCalled()
    window.removeEventListener('devflo:session-expired', expired)
  })

  it('reports only explicitly tagged core-service 503 responses globally', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      status: 503,
      ok: false,
      statusText: 'Service Unavailable',
      json: vi.fn().mockResolvedValue({
        detail: 'Devflo is temporarily unavailable',
        code: 'service_unavailable',
      }),
    })
    vi.stubGlobal('fetch', fetchMock)
    const unavailable = vi.fn()
    window.addEventListener('devflo:service-unavailable', unavailable)
    const { request } = await import('./client')

    await expect(request('/analysis/history')).rejects.toMatchObject({
      status: 503,
      code: 'service_unavailable',
    })

    expect(unavailable).toHaveBeenCalledOnce()
    window.removeEventListener('devflo:service-unavailable', unavailable)
  })

  it('keeps provider and unrelated 503 responses local without a global outage', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      status: 503,
      ok: false,
      statusText: 'Service Unavailable',
      json: vi.fn().mockResolvedValue({
        detail: 'Unable to send verification email. Please try again.',
      }),
    })
    vi.stubGlobal('fetch', fetchMock)
    const unavailable = vi.fn()
    window.addEventListener('devflo:service-unavailable', unavailable)
    const { request } = await import('./client')

    await expect(
      request('/auth/register', { method: 'POST', body: '{}' }),
    ).rejects.toMatchObject({
      status: 503,
      message: 'Unable to send verification email. Please try again.',
      code: null,
    })

    expect(unavailable).not.toHaveBeenCalled()
    window.removeEventListener('devflo:service-unavailable', unavailable)
  })

  it.each([
    '/auth/login',
    '/auth/register',
    '/auth/logout',
    '/auth/refresh',
    '/auth/verification-session',
    '/auth/verify-email',
    '/auth/forgot-password',
    '/auth/reset-password',
    '/auth/reset-password-status',
  ])('does not refresh after a public auth 401 from %s', async (path) => {
    const fetchMock = vi.fn().mockResolvedValue({
      status: 401,
      ok: false,
      statusText: 'Unauthorized',
      json: vi.fn().mockResolvedValue({ detail: 'Public auth failure' }),
    })
    vi.stubGlobal('fetch', fetchMock)
    const { request } = await import('./client')

    await expect(request(path, { method: 'POST' })).rejects.toMatchObject({ status: 401 })

    expect(fetchMock).toHaveBeenCalledOnce()
  })
})
