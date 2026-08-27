import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  verifyEmail: vi.fn(),
}))

vi.mock('../api/auth', () => ({ verifyEmail: mocks.verifyEmail }))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ search: '?token=verification-token', navigate: mocks.navigate }),
}))

import VerifyEmailPage from './VerifyEmailPage'

describe('VerifyEmailPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('keeps the email-link browser on a stable success page', async () => {
    mocks.verifyEmail.mockResolvedValue({ message: 'Email verified successfully' })
    render(<VerifyEmailPage />)

    expect(await screen.findByText('Email verified successfully.')).toBeTruthy()
    expect(screen.getByText('You can return to the device where you signed up.')).toBeTruthy()
    expect(mocks.navigate).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('button', { name: 'Sign in on this device' }))
    expect(mocks.navigate).toHaveBeenCalledWith('/login')
    expect(mocks.navigate).not.toHaveBeenCalledWith('/new', expect.anything())
  })
})
