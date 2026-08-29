import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '../api/client'

const mocks = vi.hoisted(() => ({
  logout: vi.fn(),
  navigate: vi.fn(),
  resetPassword: vi.fn(),
  search: '',
}))

vi.mock('../api/auth', () => ({ resetPassword: mocks.resetPassword }))
vi.mock('../context/useAuth', () => ({
  useAuth: () => ({ logout: mocks.logout }),
}))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ search: mocks.search, navigate: mocks.navigate }),
}))

import ResetPasswordPage from './ResetPasswordPage'

class FakeBroadcastChannel {
  static instances = []

  constructor(name) {
    this.name = name
    this.postMessage = vi.fn()
    this.close = vi.fn()
    FakeBroadcastChannel.instances.push(this)
  }
}

async function submitPasswordReset() {
  fireEvent.change(screen.getByLabelText(/^New password/), {
    target: { value: 'new-password' },
  })
  fireEvent.change(screen.getByLabelText(/^Confirm new password/), {
    target: { value: 'new-password' },
  })
  fireEvent.click(screen.getByRole('button', { name: 'Reset password' }))
  await act(async () => {})
}

describe('ResetPasswordPage success state', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/reset-password#token=reset-token')
    FakeBroadcastChannel.instances = []
    window.BroadcastChannel = FakeBroadcastChannel
    mocks.resetPassword.mockResolvedValue({ message: 'Password reset successfully' })
    mocks.logout.mockResolvedValue(undefined)
  })

  afterEach(() => {
    vi.useRealTimers()
    delete window.BroadcastChannel
  })

  it('shows only stable success content, removes the token URL, and never redirects', async () => {
    render(<ResetPasswordPage />)
    await submitPasswordReset()

    expect(screen.getByRole('heading', { name: 'Devflo' })).toBeTruthy()
    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Back to sign in' })).toBeTruthy()
    expect(mocks.resetPassword).toHaveBeenCalledWith({
      token: 'reset-token',
      newPassword: 'new-password',
    })
    expect(window.location.pathname).toBe('/reset-password')
    expect(window.location.search).toBe('')
    expect(window.location.hash).toBe('')
    expect(mocks.navigate).not.toHaveBeenCalled()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000)
    })
    expect(mocks.navigate).not.toHaveBeenCalled()
  })

  it('clears stale auth state before manually navigating to reset-success login', async () => {
    render(<ResetPasswordPage />)
    await submitPasswordReset()

    fireEvent.click(screen.getByRole('button', { name: 'Back to sign in' }))
    await act(async () => {})

    expect(mocks.logout).toHaveBeenCalledOnce()
    expect(mocks.navigate).toHaveBeenLastCalledWith('/login?reset=success', {
      replace: true,
    })
    expect(mocks.logout.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.navigate.mock.invocationCallOrder.at(-1),
    )
  })

  it('still reaches sign in when best-effort server cookie cleanup fails', async () => {
    mocks.logout.mockRejectedValue(new Error('network unavailable'))
    render(<ResetPasswordPage />)
    await submitPasswordReset()

    fireEvent.click(screen.getByRole('button', { name: 'Back to sign in' }))
    await act(async () => {})

    expect(mocks.logout).toHaveBeenCalledOnce()
    expect(mocks.navigate).toHaveBeenLastCalledWith('/login?reset=success', {
      replace: true,
    })
  })

  it('emits only the non-sensitive reset-success event and closes the channel', async () => {
    render(<ResetPasswordPage />)
    await submitPasswordReset()

    expect(FakeBroadcastChannel.instances).toHaveLength(1)
    const channel = FakeBroadcastChannel.instances[0]
    expect(channel.name).toBe('devflo-auth-events')
    expect(channel.postMessage).toHaveBeenCalledWith({ type: 'password-reset-success' })
    const event = channel.postMessage.mock.calls[0][0]
    expect(Object.keys(event)).toEqual(['type'])
    expect(JSON.stringify(event)).not.toMatch(/email|token|jwt|user|cookie/i)
    expect(event).not.toHaveProperty('password')
    expect(channel.close).toHaveBeenCalledOnce()
  })

  it('still completes normally when BroadcastChannel is unavailable', async () => {
    delete window.BroadcastChannel

    render(<ResetPasswordPage />)
    await submitPasswordReset()

    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(mocks.navigate).not.toHaveBeenCalled()
  })

  it('supports and immediately scrubs a legacy query-token link', async () => {
    mocks.search = '?token=legacy-reset-token'
    window.history.replaceState({}, '', '/reset-password?token=legacy-reset-token')

    render(<ResetPasswordPage />)
    await submitPasswordReset()

    expect(mocks.resetPassword).toHaveBeenCalledWith({
      token: 'legacy-reset-token',
      newPassword: 'new-password',
    })
    expect(window.location.pathname).toBe('/reset-password')
    expect(window.location.search).toBe('')
    expect(window.location.hash).toBe('')
  })
})

describe('ResetPasswordPage same-password rejection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/reset-password#token=reset-token')
  })

  it('shows the backend message, stays on the form, and lets the user retry with a different password', async () => {
    mocks.resetPassword.mockRejectedValueOnce(
      new ApiError('Choose a password different from your current password.', 400),
    )

    render(<ResetPasswordPage />)
    await submitPasswordReset()

    expect(
      screen.getByText('Choose a password different from your current password.'),
    ).toBeTruthy()
    expect(screen.queryByText('Password reset successfully.')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Back to sign in' })).toBeNull()
    expect(mocks.navigate).not.toHaveBeenCalled()
    expect(screen.getByRole('button', { name: 'Reset password' })).toBeTruthy()

    mocks.resetPassword.mockResolvedValueOnce({ message: 'Password reset successfully' })
    fireEvent.change(screen.getByLabelText(/^New password/), {
      target: { value: 'a-different-password' },
    })
    fireEvent.change(screen.getByLabelText(/^Confirm new password/), {
      target: { value: 'a-different-password' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Reset password' }))
    await act(async () => {})

    expect(mocks.resetPassword).toHaveBeenLastCalledWith({
      token: 'reset-token',
      newPassword: 'a-different-password',
    })
    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
  })
})
