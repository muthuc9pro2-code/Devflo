import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  resetPassword: vi.fn(),
}))

vi.mock('../api/auth', () => ({ resetPassword: mocks.resetPassword }))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ search: '?token=reset-token', navigate: mocks.navigate }),
}))

import ResetPasswordPage from './ResetPasswordPage'

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
    mocks.resetPassword.mockResolvedValue({ message: 'Password reset successfully' })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('shows success, removes the token URL, and redirects after 2.5 seconds', async () => {
    render(<ResetPasswordPage />)
    await submitPasswordReset()

    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(screen.getByText('You can now sign in with your new password.')).toBeTruthy()
    expect(mocks.navigate).toHaveBeenCalledWith('/reset-password', { replace: true })
    expect(mocks.navigate).not.toHaveBeenCalledWith('/login?reset=success', {
      replace: true,
    })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2499)
    })
    expect(mocks.navigate).not.toHaveBeenCalledWith('/login?reset=success', {
      replace: true,
    })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1)
    })
    expect(mocks.navigate).toHaveBeenCalledWith('/login?reset=success', {
      replace: true,
    })
  })

  it('clears the redirect timer on unmount', async () => {
    const { unmount } = render(<ResetPasswordPage />)
    await submitPasswordReset()
    unmount()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2500)
    })
    expect(mocks.navigate).not.toHaveBeenCalledWith('/login?reset=success', {
      replace: true,
    })
  })

  it('allows an immediate manual continuation to sign in', async () => {
    render(<ResetPasswordPage />)
    await submitPasswordReset()

    fireEvent.click(screen.getByRole('button', { name: 'Continue to sign in' }))
    expect(mocks.navigate).toHaveBeenCalledWith('/login?reset=success', {
      replace: true,
    })
  })
})
