import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError } from '../api/client'

const mocks = vi.hoisted(() => ({
  completeVerificationSession: vi.fn(),
  navigate: vi.fn(),
  refreshSession: vi.fn(),
  register: vi.fn(),
}))

vi.mock('../api/auth', () => ({
  completeVerificationSession: mocks.completeVerificationSession,
}))
vi.mock('../context/useAuth', () => ({
  useAuth: () => ({
    refreshSession: mocks.refreshSession,
    register: mocks.register,
  }),
}))
vi.mock('../router/useRouter', () => ({
  useRouter: () => ({ navigate: mocks.navigate }),
}))

import SignupPage from './SignupPage'

describe('SignupPage verification handoff polling', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.clearAllMocks()
    mocks.refreshSession.mockResolvedValue({ email: 'user@example.com' })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('resumes after reload and enters /new only when handoff authentication succeeds', async () => {
    mocks.completeVerificationSession
      .mockResolvedValueOnce({ status: 'pending' })
      .mockResolvedValueOnce({ status: 'authenticated' })

    render(<SignupPage />)
    await act(async () => {})

    expect(screen.getByText('Check your email')).toBeTruthy()
    expect(mocks.refreshSession).not.toHaveBeenCalled()
    expect(mocks.navigate).not.toHaveBeenCalled()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })

    expect(mocks.completeVerificationSession).toHaveBeenCalledTimes(2)
    expect(mocks.refreshSession).toHaveBeenCalledWith({ throwOnFailure: true })
    expect(mocks.navigate).toHaveBeenCalledWith('/new', { replace: true })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(4000)
    })
    expect(mocks.completeVerificationSession).toHaveBeenCalledTimes(2)
  })

  it('starts bounded polling after a fresh registration succeeds', async () => {
    mocks.completeVerificationSession
      .mockRejectedValueOnce(
        new ApiError('Verification handoff unavailable or expired', 401),
      )
      .mockResolvedValueOnce({ status: 'pending' })
    mocks.register.mockResolvedValue({ email: 'new@example.com' })

    render(<SignupPage />)
    await act(async () => {})

    fireEvent.change(screen.getByLabelText(/^Username/), {
      target: { name: 'username', value: 'new_user' },
    })
    fireEvent.change(screen.getByLabelText(/^Email/), {
      target: { name: 'email', value: 'new@example.com' },
    })
    fireEvent.change(screen.getByLabelText(/^Password/), {
      target: { name: 'password', value: 'new-password' },
    })
    fireEvent.change(screen.getByLabelText(/^Confirm password/), {
      target: { name: 'confirmPassword', value: 'new-password' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Sign up' }))
    await act(async () => {})

    expect(mocks.register).toHaveBeenCalledWith({
      username: 'new_user',
      email: 'new@example.com',
      password: 'new-password',
    })
    expect(screen.getByText('new@example.com')).toBeTruthy()
    expect(screen.queryByDisplayValue('new-password')).toBeNull()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(mocks.completeVerificationSession).toHaveBeenCalledTimes(2)
    expect(mocks.navigate).not.toHaveBeenCalled()
  })

  it('never overlaps polls and stops scheduling after unmount', async () => {
    let resolvePoll
    const pendingPoll = new Promise((resolve) => {
      resolvePoll = resolve
    })
    mocks.completeVerificationSession
      .mockResolvedValueOnce({ status: 'pending' })
      .mockReturnValueOnce(pendingPoll)

    const { unmount } = render(<SignupPage />)
    await act(async () => {})
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(mocks.completeVerificationSession).toHaveBeenCalledTimes(2)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000)
    })
    expect(mocks.completeVerificationSession).toHaveBeenCalledTimes(2)

    unmount()
    await act(async () => {
      resolvePoll({ status: 'pending' })
      await pendingPoll
      await vi.advanceTimersByTimeAsync(4000)
    })
    expect(mocks.completeVerificationSession).toHaveBeenCalledTimes(2)
  })
})
