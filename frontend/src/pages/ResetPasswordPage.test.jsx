import { StrictMode } from 'react'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '../api/client'

const mocks = vi.hoisted(() => ({
  logout: vi.fn(),
  navigate: vi.fn(),
  resetPassword: vi.fn(),
  resetPasswordStatus: vi.fn(),
  search: '',
}))

vi.mock('../api/auth', () => ({
  resetPassword: mocks.resetPassword,
  resetPasswordStatus: mocks.resetPasswordStatus,
}))
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

async function renderResetPage(ui = <ResetPasswordPage />) {
  const result = render(ui)
  await act(async () => {})
  return result
}

async function submitPasswordReset(passwords = { newPassword: 'new-password', confirmPassword: 'new-password' }) {
  fireEvent.change(screen.getByLabelText(/^New password/), {
    target: { value: passwords.newPassword },
  })
  fireEvent.change(screen.getByLabelText(/^Confirm new password/), {
    target: { value: passwords.confirmPassword },
  })
  fireEvent.click(screen.getByRole('button', { name: 'Reset password' }))
  await act(async () => {})
}

// Simulates a mobile browser/mail app reusing an already-open tab: opening a
// second, newer email link only changes the URL fragment of the still-
// mounted page (a same-document 'hashchange', never a remount).
function openNewEmailLinkInSameTab(path, token) {
  window.history.pushState({}, '', `${path}#token=${token}`)
  window.dispatchEvent(new Event('hashchange'))
}

describe('ResetPasswordPage success state', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/reset-password#token=reset-token')
    FakeBroadcastChannel.instances = []
    window.BroadcastChannel = FakeBroadcastChannel
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'valid' })
    mocks.resetPassword.mockResolvedValue({ message: 'Password reset successfully' })
    mocks.logout.mockResolvedValue(undefined)
  })

  afterEach(() => {
    vi.useRealTimers()
    delete window.BroadcastChannel
  })

  it('shows only stable success content, removes the token URL, and never redirects', async () => {
    await renderResetPage()
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
    await renderResetPage()
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
    await renderResetPage()
    await submitPasswordReset()

    fireEvent.click(screen.getByRole('button', { name: 'Back to sign in' }))
    await act(async () => {})

    expect(mocks.logout).toHaveBeenCalledOnce()
    expect(mocks.navigate).toHaveBeenLastCalledWith('/login?reset=success', {
      replace: true,
    })
  })

  it('emits only the non-sensitive reset-success event and closes the channel', async () => {
    await renderResetPage()
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

    await renderResetPage()
    await submitPasswordReset()

    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(mocks.navigate).not.toHaveBeenCalled()
  })

  it('supports and immediately scrubs a legacy query-token link', async () => {
    mocks.search = '?token=legacy-reset-token'
    window.history.replaceState({}, '', '/reset-password?token=legacy-reset-token')

    await renderResetPage()
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
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'valid' })
  })

  it('shows the backend message, stays on the form, and lets the user retry with a different password', async () => {
    mocks.resetPassword.mockRejectedValueOnce(
      new ApiError('Choose a password different from your current password.', 400),
    )

    await renderResetPage()
    await submitPasswordReset()

    expect(
      screen.getByText('Choose a password different from your current password.'),
    ).toBeTruthy()
    expect(screen.queryByText('Password reset successfully.')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Back to sign in' })).toBeNull()
    expect(mocks.navigate).not.toHaveBeenCalled()
    expect(screen.getByRole('button', { name: 'Reset password' })).toBeTruthy()
    // A same-password rejection never changes token_version, so the race
    // recheck (triggered by any >=400 rejection) must report the link is
    // still "valid" and must NOT be papered over as a fake success.
    expect(mocks.resetPasswordStatus).toHaveBeenLastCalledWith('reset-token')

    mocks.resetPassword.mockResolvedValueOnce({ message: 'Password reset successfully' })
    await submitPasswordReset({ newPassword: 'a-different-password', confirmPassword: 'a-different-password' })

    expect(mocks.resetPassword).toHaveBeenLastCalledWith({
      token: 'reset-token',
      newPassword: 'a-different-password',
    })
    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
  })
})

describe('ResetPasswordPage link-status classification', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/reset-password#token=reset-token')
  })

  it('status=valid shows the editable password form', async () => {
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'valid' })

    await renderResetPage()

    expect(mocks.resetPasswordStatus).toHaveBeenCalledWith('reset-token')
    expect(screen.getByLabelText(/^New password/)).toBeTruthy()
    expect(screen.getByLabelText(/^Confirm new password/)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Reset password' })).toBeTruthy()
  })

  it('status=used shows the existing success state and no password form', async () => {
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'used' })

    await renderResetPage()

    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Back to sign in' })).toBeTruthy()
    expect(screen.queryByLabelText(/^New password/)).toBeNull()
    expect(screen.queryByLabelText(/^Confirm new password/)).toBeNull()
    expect(screen.queryByRole('button', { name: 'Reset password' })).toBeNull()
    expect(mocks.resetPassword).not.toHaveBeenCalled()
  })

  it('status=invalid shows a safe invalid/expired state with no editable form', async () => {
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'invalid' })

    await renderResetPage()

    expect(screen.getByRole('alert')).toBeTruthy()
    expect(screen.getByText(/invalid or has expired/i)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Request a new link' })).toBeTruthy()
    expect(screen.queryByLabelText(/^New password/)).toBeNull()
    expect(screen.queryByText('Password reset successfully.')).toBeNull()
  })

  it('a failed status check fails closed into the invalid state, never the form', async () => {
    mocks.resetPasswordStatus.mockRejectedValue(new Error('network unavailable'))

    await renderResetPage()

    expect(screen.getByText(/invalid or has expired/i)).toBeTruthy()
    expect(screen.queryByLabelText(/^New password/)).toBeNull()
  })

  it('does not render the password form before the status check resolves', () => {
    let resolveStatus
    mocks.resetPasswordStatus.mockReturnValue(
      new Promise((resolve) => {
        resolveStatus = resolve
      }),
    )

    render(<ResetPasswordPage />)

    expect(screen.queryByLabelText(/^New password/)).toBeNull()
    expect(screen.getByText(/Checking your reset link/)).toBeTruthy()

    resolveStatus({ status: 'valid' })
  })
})

describe('ResetPasswordPage stale-token submit race', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/reset-password#token=reset-token')
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'valid' })
  })

  it('transitions to the success state when a stale-token rejection is confirmed already-used', async () => {
    mocks.resetPassword.mockRejectedValueOnce(
      new ApiError('Invalid or expired password reset token', 400),
    )

    await renderResetPage()
    // The status check ran "valid" on mount; another device consumes the
    // token between that check and this submission.
    mocks.resetPasswordStatus.mockResolvedValueOnce({ status: 'used' })
    await submitPasswordReset()

    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
    expect(screen.queryByLabelText(/^New password/)).toBeNull()
  })

  it('keeps the real error when the recheck still reports valid', async () => {
    mocks.resetPassword.mockRejectedValueOnce(
      new ApiError('Invalid or expired password reset token', 400),
    )

    await renderResetPage()
    mocks.resetPasswordStatus.mockResolvedValueOnce({ status: 'valid' })
    await submitPasswordReset()

    expect(screen.getByText('Invalid or expired password reset token')).toBeTruthy()
    expect(screen.queryByText('Password reset successfully.')).toBeNull()
  })

  it('keeps the real error when the recheck itself fails', async () => {
    mocks.resetPassword.mockRejectedValueOnce(
      new ApiError('Invalid or expired password reset token', 400),
    )

    await renderResetPage()
    mocks.resetPasswordStatus.mockRejectedValueOnce(new Error('network unavailable'))
    await submitPasswordReset()

    expect(screen.getByText('Invalid or expired password reset token')).toBeTruthy()
  })
})

describe('ResetPasswordPage same-mount token replacement', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/reset-password#token=TOKEN_A')
  })

  it('replaces an in-memory token with a newer one from the same mounted page and submits the new one', async () => {
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'valid' })

    await renderResetPage()
    expect(mocks.resetPasswordStatus).toHaveBeenLastCalledWith('TOKEN_A')

    openNewEmailLinkInSameTab('/reset-password', 'TOKEN_B')
    await act(async () => {})

    expect(window.location.hash).toBe('')
    expect(mocks.resetPasswordStatus).toHaveBeenLastCalledWith('TOKEN_B')

    await submitPasswordReset()

    expect(mocks.resetPassword).toHaveBeenCalledWith({
      token: 'TOKEN_B',
      newPassword: 'new-password',
    })
    expect(mocks.resetPassword).not.toHaveBeenCalledWith(
      expect.objectContaining({ token: 'TOKEN_A' }),
    )
  })

  it('clears a stale error from the old token when a new token arrives, and evaluates the new one fresh', async () => {
    mocks.resetPasswordStatus.mockResolvedValueOnce({ status: 'valid' })
    mocks.resetPassword.mockRejectedValueOnce(
      new ApiError('Choose a password different from your current password.', 400),
    )

    await renderResetPage()
    await submitPasswordReset()
    expect(
      screen.getByText('Choose a password different from your current password.'),
    ).toBeTruthy()

    mocks.resetPasswordStatus.mockResolvedValueOnce({ status: 'valid' })
    openNewEmailLinkInSameTab('/reset-password', 'TOKEN_B')
    await act(async () => {})

    expect(
      screen.queryByText('Choose a password different from your current password.'),
    ).toBeNull()
    expect(screen.getByLabelText(/^New password/).value).toBe('')
    expect(mocks.resetPasswordStatus).toHaveBeenLastCalledWith('TOKEN_B')
  })

  it('clears a stale success screen from the old token when a genuinely new token arrives', async () => {
    mocks.resetPasswordStatus.mockResolvedValueOnce({ status: 'valid' })
    mocks.resetPassword.mockResolvedValueOnce({ message: 'Password reset successfully' })

    await renderResetPage()
    await submitPasswordReset()
    expect(screen.getByText('Password reset successfully.')).toBeTruthy()

    mocks.resetPasswordStatus.mockResolvedValueOnce({ status: 'valid' })
    openNewEmailLinkInSameTab('/reset-password', 'TOKEN_B')
    await act(async () => {})

    expect(screen.queryByText('Password reset successfully.')).toBeNull()
    expect(screen.getByLabelText(/^New password/)).toBeTruthy()
  })
})

// A realistic JWT shape (three dot-separated base64url segments, which may
// themselves contain '-' and '_'), not the "reset-token"/"legacy-reset-token"
// literals used elsewhere in this file - guards against a fragment-parsing
// or serialization bug that only a real, dot-and-hyphen-bearing JWT would
// expose (investigated after a live-deployment report of genuine reset
// tokens being rejected as "Invalid or expired").
const REALISTIC_JWT_A =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9' +
  '.eyJzdWIiOiJ1c2VyQGV4YW1wbGUuY29tIiwidHlwZSI6InBhc3N3b3JkX3Jlc2V0IiwidmVyIjozLCJleHAiOjE3ODAwMDAwMDB9' +
  '.xprL2iHW-qv7VM8CLXntk2fAkeKVdbMniStvq8bjRzo'

const REALISTIC_JWT_B =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9' +
  '.eyJzdWIiOiJ1c2VyQGV4YW1wbGUuY29tIiwidHlwZSI6InBhc3N3b3JkX3Jlc2V0IiwidmVyIjo0LCJleHAiOjE3ODAwMDAwOTl9' +
  '.b6DoT8xnjE7fB2NBRAdvT_bTHT7ZobiFvzY7hGz3pfw'

describe('ResetPasswordPage realistic JWT fragment round-trip', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.search = ''
    mocks.resetPasswordStatus.mockResolvedValue({ status: 'valid' })
  })

  it('captures a real JWT-shaped token byte-for-byte, scrubs the URL, and submits the exact same token', async () => {
    window.history.replaceState({}, '', `/reset-password#token=${REALISTIC_JWT_A}`)
    mocks.resetPassword.mockResolvedValue({ message: 'Password reset successfully' })

    await renderResetPage()

    // clearEmailLinkToken runs in a layout effect on mount.
    expect(window.location.pathname).toBe('/reset-password')
    expect(window.location.hash).toBe('')

    await submitPasswordReset()

    expect(mocks.resetPassword).toHaveBeenCalledWith({
      token: REALISTIC_JWT_A,
      newPassword: 'new-password',
    })
    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
  })

  it('retains the exact captured token across StrictMode double-invoked mount effects', async () => {
    window.history.replaceState({}, '', `/reset-password#token=${REALISTIC_JWT_A}`)
    mocks.resetPassword.mockResolvedValue({ message: 'Password reset successfully' })

    await renderResetPage(
      <StrictMode>
        <ResetPasswordPage />
      </StrictMode>,
    )

    expect(window.location.hash).toBe('')

    await submitPasswordReset()

    expect(mocks.resetPassword).toHaveBeenCalledWith({
      token: REALISTIC_JWT_A,
      newPassword: 'new-password',
    })
    expect(screen.getByText('Password reset successfully.')).toBeTruthy()
  })

  it('replaces a realistic JWT with a newer realistic JWT via hashchange, correctly under StrictMode', async () => {
    window.history.replaceState({}, '', `/reset-password#token=${REALISTIC_JWT_A}`)
    mocks.resetPassword.mockResolvedValue({ message: 'Password reset successfully' })

    await renderResetPage(
      <StrictMode>
        <ResetPasswordPage />
      </StrictMode>,
    )
    expect(mocks.resetPasswordStatus).toHaveBeenLastCalledWith(REALISTIC_JWT_A)

    openNewEmailLinkInSameTab('/reset-password', REALISTIC_JWT_B)
    await act(async () => {})

    expect(window.location.hash).toBe('')
    expect(mocks.resetPasswordStatus).toHaveBeenLastCalledWith(REALISTIC_JWT_B)

    await submitPasswordReset()

    expect(mocks.resetPassword).toHaveBeenCalledWith({
      token: REALISTIC_JWT_B,
      newPassword: 'new-password',
    })
    expect(mocks.resetPassword).not.toHaveBeenCalledWith(
      expect.objectContaining({ token: REALISTIC_JWT_A }),
    )
  })
})
