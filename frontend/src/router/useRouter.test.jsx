import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useRouter } from './useRouter'

function fireBrowserBack(toPath) {
  // Simulates what a real browser does on Back/Forward: it changes
  // window.location FIRST, then fires popstate - pushState itself does
  // NOT fire popstate, so this manual two-step is how a real back
  // navigation is exercised in jsdom.
  window.history.pushState({}, '', toPath)
  window.dispatchEvent(new PopStateEvent('popstate'))
}

describe('useRouter', () => {
  beforeEach(() => {
    window.history.pushState({}, '', '/new')
  })

  afterEach(() => {
    window.history.pushState({}, '', '/new')
  })

  it('updates location normally on popstate when not blocked', () => {
    const { result } = renderHook(() => useRouter(false))

    act(() => {
      fireBrowserBack('/login')
    })

    expect(result.current.pathname).toBe('/login')
    expect(window.location.pathname).toBe('/login')
  })

  it('reverts the address bar immediately when a real Back/Forward fires while blocked', () => {
    const { result } = renderHook(() => useRouter(true))
    expect(result.current.pathname).toBe('/new')

    act(() => {
      fireBrowserBack('/login')
    })

    // The address bar itself never actually ends up on /login - pushed
    // straight back to /new by the blocked-popstate handler.
    expect(window.location.pathname).toBe('/new')
    expect(result.current.pathname).toBe('/new')
  })

  it('does not loop: pushState during the revert never re-triggers popstate handling', () => {
    const pushStateSpy = vi.spyOn(window.history, 'pushState')
    renderHook(() => useRouter(true))

    act(() => {
      fireBrowserBack('/login')
    })

    // Two total pushState calls: the test helper's own simulated browser
    // navigation to /login, then exactly ONE corrective pushState back to
    // /new from the blocked-popstate handler - never more, which is what
    // a real infinite loop would produce (this call count would keep
    // growing, or the test would hang).
    expect(pushStateSpy).toHaveBeenCalledTimes(2)
    expect(pushStateSpy).toHaveBeenLastCalledWith({}, '', '/new')
    pushStateSpy.mockRestore()
  })

  it('does not block our OWN navigate() calls while locked (the upload-success handoff)', () => {
    const { result } = renderHook(() => useRouter(true))

    act(() => {
      result.current.navigate('/investigation/42')
    })

    expect(result.current.pathname).toBe('/investigation/42')
    expect(window.location.pathname).toBe('/investigation/42')
  })

  it('resumes normal Back/Forward once no longer blocked', () => {
    const { result, rerender } = renderHook(({ locked }) => useRouter(locked), {
      initialProps: { locked: true },
    })

    act(() => {
      fireBrowserBack('/login')
    })
    expect(result.current.pathname).toBe('/new')

    rerender({ locked: false })

    act(() => {
      fireBrowserBack('/login')
    })
    expect(result.current.pathname).toBe('/login')
    expect(window.location.pathname).toBe('/login')
  })
})
