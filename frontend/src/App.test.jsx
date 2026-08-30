import { act, fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

// App.jsx wires the REAL CriticalOperationProvider and AuthProvider - only
// the leaf hooks/pages below are mocked, so the actual deferred-takeover
// logic in Routes() (App.jsx) runs for real in every test here.
const mocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  refreshSession: vi.fn(),
  getAnalysisHistory: vi.fn(),
  logout: vi.fn(),
}))

const authState = vi.hoisted(() => ({ status: 'authenticated', unavailable: false }))
const routerState = vi.hoisted(() => ({ pathname: '/new' }))

vi.mock('./context/useAuth', () => ({
  useAuth: () => ({
    user: { username: 'alice', email: 'alice@example.com' },
    status: authState.status,
    unavailable: authState.unavailable,
    refreshSession: mocks.refreshSession,
    logout: mocks.logout,
  }),
}))

vi.mock('./router/useRouter', () => ({
  useRouter: () => ({ pathname: routerState.pathname, navigate: mocks.navigate }),
}))

vi.mock('./api/analysis', () => ({ getAnalysisHistory: mocks.getAnalysisHistory }))

vi.mock('./pages/NewInvestigationPage', () => ({
  default: ({ onUploadingChange, onUploaded }) => (
    <div>
      <p>New investigation stub</p>
      <button type="button" onClick={() => onUploadingChange(true)}>
        Simulate upload start
      </button>
      <button type="button" onClick={() => onUploadingChange(false)}>
        Simulate upload end
      </button>
      <button
        type="button"
        onClick={() => {
          onUploaded({ id: 42 })
          onUploadingChange(false)
        }}
      >
        Simulate upload success
      </button>
    </div>
  ),
}))

vi.mock('./pages/AnalysisPage', () => ({
  default: ({ analysisId }) => <div>Analysis page stub {analysisId}</div>,
}))

vi.mock('./pages/ServiceUnavailablePage', () => ({
  default: () => <div>Service unavailable stub</div>,
}))

vi.mock('./pages/LoginPage', () => ({
  default: () => <div>Login page stub</div>,
}))

import App from './App'

async function renderApp() {
  const utils = render(<App />)
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 10))
  })
  return utils
}

function beginUpload() {
  fireEvent.click(screen.getByRole('button', { name: 'Simulate upload start' }))
}

describe('App - critical-operation-locked takeovers', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    authState.status = 'authenticated'
    authState.unavailable = false
    routerState.pathname = '/new'
    window.matchMedia = vi.fn().mockReturnValue({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })
    mocks.getAnalysisHistory.mockResolvedValue({ items: [], next_cursor: null })
    mocks.refreshSession.mockResolvedValue(undefined)
  })

  it('a session expiring mid-upload does not unmount the shell until the upload finishes', async () => {
    const { rerender } = await renderApp()
    beginUpload()

    // Simulates AuthContext's devflo:session-expired handler flipping
    // status - a DIFFERENT in-flight request's 401, unrelated to the
    // upload, discovering the session is gone.
    authState.status = 'unauthenticated'
    await act(async () => {
      rerender(<App />)
    })

    // Still the authenticated shell, still mid-upload - never torn down.
    expect(screen.getByText('New investigation stub')).toBeTruthy()
    expect(screen.queryByText('Login page stub')).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload end' }))
    await act(async () => {
      rerender(<App />)
    })

    // The deferred takeover now resumes immediately.
    expect(screen.getByText('Login page stub')).toBeTruthy()
  })

  it('a 503 discovered mid-upload does not swap to the service-unavailable page until the upload finishes', async () => {
    const { rerender } = await renderApp()
    beginUpload()

    authState.unavailable = true
    await act(async () => {
      rerender(<App />)
    })

    expect(screen.getByText('New investigation stub')).toBeTruthy()
    expect(screen.queryByText('Service unavailable stub')).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload end' }))
    await act(async () => {
      rerender(<App />)
    })

    expect(screen.getByText('Service unavailable stub')).toBeTruthy()
    // Bounded: exactly one revalidation attempt, never a retry storm.
    expect(mocks.refreshSession).toHaveBeenCalledTimes(1)
  })

  it('keeps AppShell mounted while post-upload 503 revalidation is still pending', async () => {
    let resolveRefresh
    mocks.refreshSession.mockReturnValue(
      new Promise((resolve) => {
        resolveRefresh = resolve
      }),
    )

    const { rerender } = await renderApp()
    beginUpload()

    authState.unavailable = true
    await act(async () => {
      rerender(<App />)
    })
    expect(
      screen.getByText('New investigation stub'),
    ).toBeTruthy()

    fireEvent.click(
      screen.getByRole(
        'button',
        { name: 'Simulate upload end' },
      ),
    )
    await act(async () => {
      rerender(<App />)
    })

    // refreshSession has started but has NOT resolved. The stale 503 must
    // not get even one committed render that unmounts AppShell first.
    expect(
      mocks.refreshSession,
    ).toHaveBeenCalledTimes(1)
    expect(
      screen.getByText('New investigation stub'),
    ).toBeTruthy()
    expect(
      screen.queryByText(
        'Service unavailable stub',
      ),
    ).toBeNull()

    authState.unavailable = false
    await act(async () => {
      resolveRefresh()
      await Promise.resolve()
      rerender(<App />)
    })

    expect(
      screen.getByText('New investigation stub'),
    ).toBeTruthy()
    expect(
      screen.queryByText(
        'Service unavailable stub',
      ),
    ).toBeNull()
  })

  it('a stale 503 from during the upload is cleared by ONE revalidation if the backend already recovered', async () => {
    const { rerender } = await renderApp()
    beginUpload()

    authState.unavailable = true
    await act(async () => {
      rerender(<App />)
    })
    expect(screen.getByText('New investigation stub')).toBeTruthy()

    // The backend has actually recovered by the time the upload settles -
    // simulate refreshSession() itself observing that and clearing the
    // stale flag, the same way the real AuthContext.refreshSession does
    // on a successful /auth/me call.
    mocks.refreshSession.mockImplementation(async () => {
      authState.unavailable = false
    })

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload end' }))
    await act(async () => {
      rerender(<App />)
    })

    // Never shown at all - the stale outage state was cleared before this
    // render ever had to decide whether to paint it.
    expect(screen.queryByText('Service unavailable stub')).toBeNull()
    expect(mocks.refreshSession).toHaveBeenCalledTimes(1)
  })

  it('a browser Back/Forward navigation mid-upload does not change what is on screen until the upload finishes', async () => {
    const { rerender } = await renderApp()
    beginUpload()

    // Simulates popstate changing the address bar to a totally different
    // route while the upload is still running.
    routerState.pathname = '/investigation/7'
    await act(async () => {
      rerender(<App />)
    })

    // Still showing the (frozen) /new route's content, not the analysis page.
    expect(screen.getByText('New investigation stub')).toBeTruthy()
    expect(screen.queryByText(/Analysis page stub/)).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload end' }))
    await act(async () => {
      rerender(<App />)
    })

    expect(screen.getByText('Analysis page stub 7')).toBeTruthy()
  })

  it('upload success while a takeover was already pending still gets a real navigate to the new analysis', async () => {
    const { rerender } = await renderApp()
    beginUpload()

    authState.status = 'unauthenticated'
    await act(async () => {
      rerender(<App />)
    })

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload success' }))
    await act(async () => {
      rerender(<App />)
    })

    expect(mocks.navigate).toHaveBeenCalledWith('/investigation/42')
  })
})
