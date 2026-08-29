import { StrictMode } from 'react'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '../api/client'

const mocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  search: '',
  verifyEmail: vi.fn(),
}))

vi.mock('../api/auth', () => ({ verifyEmail: mocks.verifyEmail }))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ search: mocks.search, navigate: mocks.navigate }),
}))

import VerifyEmailPage from './VerifyEmailPage'

// Simulates a mobile browser/mail app reusing an already-open tab: opening a
// second, newer email link only changes the URL fragment of the still-
// mounted page (a same-document 'hashchange', never a remount).
function openNewEmailLinkInSameTab(path, token) {
  window.history.pushState({}, '', `${path}#token=${token}`)
  window.dispatchEvent(new Event('hashchange'))
}

describe('VerifyEmailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.search = ''
    window.history.replaceState({}, '', '/verify-email#token=verification-token')
  })

  it('keeps the email-link browser on a stable success page', async () => {
    mocks.verifyEmail.mockResolvedValue({ message: 'Email verified successfully' })
    render(<VerifyEmailPage />)

    expect(await screen.findByText('Email verified successfully.')).toBeTruthy()
    expect(screen.getByText('You can return to the device where you signed up.')).toBeTruthy()
    expect(mocks.verifyEmail).toHaveBeenCalledWith('verification-token')
    expect(window.location.pathname).toBe('/verify-email')
    expect(window.location.search).toBe('')
    expect(window.location.hash).toBe('')

    fireEvent.click(screen.getByRole('button', { name: 'Sign in on this device' }))
    expect(mocks.navigate).toHaveBeenCalledWith('/login')
    expect(mocks.navigate).not.toHaveBeenCalledWith('/new', expect.anything())
  })

  it('supports and immediately scrubs a legacy query-token link', async () => {
    mocks.search = '?token=legacy-verification-token'
    window.history.replaceState({}, '', '/verify-email?token=legacy-verification-token')
    mocks.verifyEmail.mockResolvedValue({ message: 'Email verified successfully' })

    render(<VerifyEmailPage />)

    expect(await screen.findByText('Email verified successfully.')).toBeTruthy()
    expect(mocks.verifyEmail).toHaveBeenCalledWith('legacy-verification-token')
    expect(window.location.pathname).toBe('/verify-email')
    expect(window.location.search).toBe('')
    expect(window.location.hash).toBe('')
  })

  it('shows a stable controlled state for an already-used verification link, without authenticating this device', async () => {
    mocks.verifyEmail.mockRejectedValue(
      new ApiError('This verification link has already been used.', 400),
    )

    render(<VerifyEmailPage />)

    expect(await screen.findByText('This verification link has already been used.')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Back to login' })).toBeTruthy()
    expect(mocks.navigate).not.toHaveBeenCalled()
  })

  it('never calls verification-session or navigates to /new from this device', async () => {
    mocks.verifyEmail.mockResolvedValue({ message: 'Email verified successfully' })
    render(<VerifyEmailPage />)

    await screen.findByText('Email verified successfully.')

    expect(mocks.navigate).not.toHaveBeenCalledWith('/new', expect.anything())
    expect(mocks.navigate).not.toHaveBeenCalled()
  })

  it('replaces an in-memory token with a newer one from the same mounted page and verifies the new one', async () => {
    let resolveFirst
    mocks.verifyEmail.mockReturnValueOnce(
      new Promise((resolve) => {
        resolveFirst = resolve
      }),
    )

    render(<VerifyEmailPage />)
    expect(mocks.verifyEmail).toHaveBeenLastCalledWith('verification-token')
    expect(screen.getByText('Verifying your email…')).toBeTruthy()

    mocks.verifyEmail.mockResolvedValueOnce({ message: 'Email verified successfully' })
    openNewEmailLinkInSameTab('/verify-email', 'newer-verification-token')
    await act(async () => {})

    expect(window.location.hash).toBe('')
    expect(mocks.verifyEmail).toHaveBeenLastCalledWith('newer-verification-token')

    await screen.findByText('Email verified successfully.')
    // The superseded token's late resolution must not resurrect stale UI.
    resolveFirst({ message: 'Email verified successfully' })
    await act(async () => {})
    expect(screen.getByText('Email verified successfully.')).toBeTruthy()
  })

  it('retains the exact captured token across StrictMode double-invoked mount effects', async () => {
    mocks.verifyEmail.mockResolvedValue({ message: 'Email verified successfully' })

    render(
      <StrictMode>
        <VerifyEmailPage />
      </StrictMode>,
    )

    expect(await screen.findByText('Email verified successfully.')).toBeTruthy()
    expect(mocks.verifyEmail).toHaveBeenCalledWith('verification-token')
    expect(window.location.hash).toBe('')
  })
})
