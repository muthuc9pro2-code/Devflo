import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  logout: vi.fn(),
  navigate: vi.fn(),
  resetPassword: vi.fn(),
}))

vi.mock('../api/auth', () => ({ resetPassword: mocks.resetPassword }))
vi.mock('../context/useAuth', () => ({
  useAuth: () => ({ logout: mocks.logout }),
}))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ search: '?token=reset-token', navigate: mocks.navigate }),
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
    expect(mocks.navigate).toHaveBeenCalledTimes(1)
    expect(mocks.navigate).toHaveBeenCalledWith('/reset-password', { replace: true })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000)
    })
    expect(mocks.navigate).toHaveBeenCalledTimes(1)
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
    expect(mocks.navigate).toHaveBeenCalledWith('/reset-password', { replace: true })
  })
})
