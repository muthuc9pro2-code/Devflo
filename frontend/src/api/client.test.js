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
})
