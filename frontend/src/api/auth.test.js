import { beforeEach, describe, expect, it, vi } from 'vitest'

const client = vi.hoisted(() => ({
  finishLogout: vi.fn(),
  markSessionEstablished: vi.fn(),
  prepareForLogout: vi.fn(),
  request: vi.fn(),
}))

vi.mock('./client', () => client)

import { completeVerificationSession, resetPasswordStatus, verifyEmail } from './auth'

describe('verification auth API', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('does not mark the verification-link browser as authenticated', async () => {
    client.request.mockResolvedValue({ message: 'Email verified successfully' })

    await verifyEmail('verification token')

    expect(client.request).toHaveBeenCalledWith(
      '/auth/verify-email',
      {
        method: 'POST',
        body: JSON.stringify({ token: 'verification token' }),
      },
    )
    expect(client.markSessionEstablished).not.toHaveBeenCalled()
  })

  it('marks a session only after the handoff is authenticated', async () => {
    client.request
      .mockResolvedValueOnce({ status: 'pending' })
      .mockResolvedValueOnce({ status: 'authenticated' })

    await expect(completeVerificationSession()).resolves.toEqual({ status: 'pending' })
    expect(client.markSessionEstablished).not.toHaveBeenCalled()

    await expect(completeVerificationSession()).resolves.toEqual({
      status: 'authenticated',
    })
    expect(client.request).toHaveBeenLastCalledWith('/auth/verification-session', {
      method: 'POST',
    })
    expect(client.markSessionEstablished).toHaveBeenCalledOnce()
  })
})

describe('resetPasswordStatus', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('sends the token as JSON body and returns the classification unchanged', async () => {
    client.request.mockResolvedValue({ status: 'used' })

    await expect(resetPasswordStatus('reset-token')).resolves.toEqual({ status: 'used' })

    expect(client.request).toHaveBeenCalledWith('/auth/reset-password-status', {
      method: 'POST',
      body: JSON.stringify({ token: 'reset-token' }),
    })
  })
})
