import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

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
})
