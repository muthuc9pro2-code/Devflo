import { act, fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  getAnalysisHistory: vi.fn(),
  logout: vi.fn(),
  navigate: vi.fn(),
}))

vi.mock('../api/analysis', () => ({ getAnalysisHistory: mocks.getAnalysisHistory }))
vi.mock('../context/useAuth', () => ({
  useAuth: () => ({
    user: { username: 'alice', email: 'alice@example.com' },
    logout: mocks.logout,
  }),
}))

// A minimal stand-in exposing the exact onUploadingChange/onUploaded
// contract AppShell wires to the real NewInvestigationPage, without
// re-implementing any of its actual upload logic - the test drives the
// upload lifecycle deterministically through these buttons instead.
vi.mock('../pages/NewInvestigationPage', () => ({
  default: ({ onUploadingChange, onUploaded }) => (
    <div>
      <p>New investigation stub</p>
      <button type="button" onClick={() => onUploadingChange(true)}>
        Simulate upload start
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
      <button type="button" onClick={() => onUploadingChange(false)}>
        Simulate upload failure
      </button>
    </div>
  ),
}))

vi.mock('../pages/AnalysisPage', () => ({
  default: ({ analysisId }) => <div>Analysis page stub {analysisId}</div>,
}))

import AppShell from './AppShell'
import { CriticalOperationProvider } from '../context/CriticalOperationContext'

function renderWithProviders(pathname) {
  return render(
    <CriticalOperationProvider>
      <AppShell pathname={pathname} navigate={mocks.navigate} />
    </CriticalOperationProvider>,
  )
}

async function renderShell(pathname = '/new') {
  const utils = renderWithProviders(pathname)
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 10))
  })
  return utils
}

function beginUpload() {
  fireEvent.click(screen.getByRole('button', { name: 'Simulate upload start' }))
}

describe('AppShell upload navigation lock', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.matchMedia = vi.fn().mockReturnValue({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })
    mocks.getAnalysisHistory.mockResolvedValue({
      items: [
        {
          analysis_id: 7,
          status: 'completed',
          original_filename: 'log.txt',
          created_at: '2026-01-01T00:00:00Z',
          updated_at: '2026-01-01T00:00:00Z',
        },
      ],
      next_cursor: null,
    })
    mocks.logout.mockResolvedValue(undefined)
  })

  it('before upload starts: New investigation, History, and Logout all navigate normally', async () => {
    await renderShell()

    fireEvent.click(screen.getByRole('link', { name: /log\.txt/ }))
    expect(mocks.navigate).toHaveBeenLastCalledWith('/investigation/7')

    fireEvent.click(screen.getByRole('button', { name: 'New investigation' }))
    expect(mocks.navigate).toHaveBeenLastCalledWith('/new')

    fireEvent.click(screen.getByRole('button', { name: 'Log out' }))
    await act(async () => {})
    expect(mocks.logout).toHaveBeenCalledOnce()
    expect(mocks.navigate).toHaveBeenLastCalledWith('/login', { replace: true })
  })

  it('once upload begins: controls are disabled/aria-disabled and cannot navigate', async () => {
    await renderShell()
    beginUpload()

    const newInvestigationButton = screen.getByRole('button', { name: 'New investigation' })
    const logoutButton = screen.getByRole('button', { name: 'Log out' })
    const brandLink = screen.getByRole('link', { name: 'Devflo' })
    const historyLink = screen.getByRole('link', { name: /log\.txt/ })

    expect(newInvestigationButton.disabled).toBe(true)
    expect(logoutButton.disabled).toBe(true)
    expect(brandLink.getAttribute('aria-disabled')).toBe('true')
    expect(historyLink.getAttribute('aria-disabled')).toBe('true')

    fireEvent.click(newInvestigationButton)
    fireEvent.click(brandLink)
    fireEvent.click(historyLink)
    fireEvent.click(logoutButton)
    await act(async () => {})

    expect(mocks.navigate).not.toHaveBeenCalled()
    expect(mocks.logout).not.toHaveBeenCalled()
    expect(screen.getByText('New investigation stub')).toBeTruthy()
  })

  it('clicking New investigation while locked does not remount the current NewInvestigationPage', async () => {
    await renderShell()
    beginUpload()

    const stub = screen.getByText('New investigation stub')
    fireEvent.click(screen.getByRole('button', { name: 'New investigation' }))
    await act(async () => {})

    expect(screen.getByText('New investigation stub')).toBe(stub)
  })

  it('upload success: navigates to the real analysis and releases the lock', async () => {
    const { rerender } = renderWithProviders('/new')
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10))
    })
    beginUpload()

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload success' }))
    await act(async () => {})

    expect(mocks.navigate).toHaveBeenCalledWith('/investigation/42')

    // The real app re-renders AppShell with the new pathname once its own
    // router state updates from that navigate() call - simulate that here
    // on the SAME instance and confirm the lock was actually released
    // (not still true, which would leave these controls disabled).
    rerender(
      <CriticalOperationProvider>
        <AppShell pathname="/investigation/42" navigate={mocks.navigate} />
      </CriticalOperationProvider>,
    )
    fireEvent.click(screen.getByRole('button', { name: 'New investigation' }))
    expect(mocks.navigate).toHaveBeenLastCalledWith('/new')
  })

  it('upload failure: stays on the page, lock releases, navigation works again', async () => {
    await renderShell()
    beginUpload()

    fireEvent.click(screen.getByRole('button', { name: 'Simulate upload failure' }))
    await act(async () => {})

    expect(screen.getByText('New investigation stub')).toBeTruthy()
    expect(mocks.navigate).not.toHaveBeenCalled()

    const newInvestigationButton = screen.getByRole('button', { name: 'New investigation' })
    expect(newInvestigationButton.disabled).toBe(false)

    fireEvent.click(newInvestigationButton)
    expect(mocks.navigate).toHaveBeenLastCalledWith('/new')

    fireEvent.click(screen.getByRole('button', { name: 'Log out' }))
    await act(async () => {})
    expect(mocks.logout).toHaveBeenCalledOnce()
  })

  it('mobile drawer: locked controls remain non-navigable, but closing the drawer still works', async () => {
    window.matchMedia = vi.fn().mockReturnValue({
      matches: true,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })
    await renderShell()
    beginUpload()

    fireEvent.click(screen.getByRole('button', { name: 'Open navigation' }))

    const newInvestigationButton = screen.getByRole('button', { name: 'New investigation' })
    const logoutButton = screen.getByRole('button', { name: 'Log out' })
    expect(newInvestigationButton.disabled).toBe(true)
    expect(logoutButton.disabled).toBe(true)

    fireEvent.click(newInvestigationButton)
    fireEvent.click(logoutButton)
    await act(async () => {})
    expect(mocks.navigate).not.toHaveBeenCalled()
    expect(mocks.logout).not.toHaveBeenCalled()

    // Closing the drawer is harmless and stays available (two controls
    // share this label: the sidebar's own close button and the scrim).
    fireEvent.click(screen.getAllByRole('button', { name: 'Close navigation' })[0])
  })
})
