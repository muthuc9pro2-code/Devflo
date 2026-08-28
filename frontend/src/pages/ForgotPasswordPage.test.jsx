import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  forgotPassword: vi.fn(),
  logout: vi.fn(),
  navigate: vi.fn(),
}))

vi.mock('../api/auth', () => ({ forgotPassword: mocks.forgotPassword }))
vi.mock('../context/useAuth', () => ({
  useAuth: () => ({ logout: mocks.logout }),
}))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ navigate: mocks.navigate }),
}))

import ForgotPasswordPage from './ForgotPasswordPage'

class FakeBroadcastChannel {
  static instances = []

  constructor(name) {
    this.name = name
    this.onmessage = null
    this.close = vi.fn()
    FakeBroadcastChannel.instances.push(this)
  }
}

async function submitForgotPassword() {
  fireEvent.change(screen.getByLabelText('Email'), {
    target: { value: 'user@example.com' },
  })
  fireEvent.click(screen.getByRole('button', { name: 'Send reset link' }))
  await act(async () => {})
}

describe('ForgotPasswordPage reset notification', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    FakeBroadcastChannel.instances = []
    window.BroadcastChannel = FakeBroadcastChannel
    mocks.forgotPassword.mockResolvedValue({
      message: 'If an account exists for this email, a password reset link has been sent.',
    })
    mocks.logout.mockResolvedValue(undefined)
  })

  afterEach(() => {
    delete window.BroadcastChannel
  })

  it('changes the post-submit state on reset success and only navigates on button click', async () => {
    render(<ForgotPasswordPage />)
    const channel = FakeBroadcastChannel.instances[0]

    expect(channel.name).toBe('devflo-auth-events')
    await submitForgotPassword()
    expect(screen.getByText('Check your email')).toBeTruthy()
    expect(mocks.navigate).not.toHaveBeenCalled()

    await act(async () => {
      channel.onmessage({ data: { type: 'password-reset-success' } })
    })

    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Back to sign in' })).toBeTruthy()
    expect(mocks.forgotPassword).toHaveBeenCalledTimes(1)
    expect(mocks.navigate).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('button', { name: 'Back to sign in' }))
    await act(async () => {})
    expect(mocks.logout).toHaveBeenCalledOnce()
    expect(mocks.navigate).toHaveBeenCalledWith('/login?reset=success', {
      replace: true,
    })
  })

  it('ignores the event before the Check your email state', async () => {
    render(<ForgotPasswordPage />)
    const channel = FakeBroadcastChannel.instances[0]

    await act(async () => {
      channel.onmessage({ data: { type: 'password-reset-success' } })
    })

    expect(screen.queryByText('Password reset successfully.')).toBeNull()
    expect(screen.getByRole('button', { name: 'Send reset link' })).toBeTruthy()
  })

  it('works without BroadcastChannel and remains on Check your email', async () => {
    delete window.BroadcastChannel

    render(<ForgotPasswordPage />)
    await submitForgotPassword()

    expect(screen.getByText('Check your email')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Back to login' })).toBeTruthy()
  })

  it('removes the listener and closes the channel on unmount', () => {
    const { unmount } = render(<ForgotPasswordPage />)
    const channel = FakeBroadcastChannel.instances[0]
    expect(channel.onmessage).toBeTypeOf('function')

    unmount()

    expect(channel.onmessage).toBeNull()
    expect(channel.close).toHaveBeenCalledOnce()
  })
})
