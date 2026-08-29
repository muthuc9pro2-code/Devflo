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
