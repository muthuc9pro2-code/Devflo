import { act, renderHook } from '@testing-library/react'
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest'
import { useRouter } from './useRouter'

const HISTORY_INDEX_KEY = '__devfloHistoryIndex'

function simulatePopState(toPath, index) {
  // popstate fires after the browser has already traversed to its target.
  // jsdom does not perform history.go() traversal itself, so install the
  // target explicitly and assert the corrective history.go() call.
  const state = {
    [HISTORY_INDEX_KEY]: index,
  }
  window.history.replaceState(
    state,
    '',
    toPath,
  )
  window.dispatchEvent(
    new PopStateEvent(
      'popstate',
      { state },
    ),
  )
}

describe('useRouter', () => {
  beforeEach(() => {
    window.history.replaceState(
      {},
      '',
      '/new',
    )
  })

  afterEach(() => {
    window.history.replaceState(
      {},
      '',
      '/new',
    )
    vi.restoreAllMocks()
  })

  it('updates location normally on popstate when not blocked', () => {
    const { result } = renderHook(
      () => useRouter(false),
    )

    act(() => {
      simulatePopState('/login', 1)
    })

    expect(result.current.pathname).toBe('/login')
    expect(window.location.pathname).toBe('/login')
  })

  it('blocks Back by traversing forward to the locked entry without inserting history', () => {
    const pushStateSpy = vi.spyOn(
      window.history,
      'pushState',
    )
    const replaceStateSpy = vi.spyOn(
      window.history,
      'replaceState',
    )
    const goSpy = vi
      .spyOn(window.history, 'go')
      .mockImplementation(() => {})

    const { result, rerender } = renderHook(
      ({ locked }) => useRouter(locked),
      {
        initialProps: {
          locked: false,
        },
      },
    )

    act(() => {
      result.current.navigate('/investigation/7')
      result.current.navigate('/new')
    })

    expect(
      window.history.state[HISTORY_INDEX_KEY],
    ).toBe(2)

    rerender({
      locked: true,
    })

    const pushCountBeforePop =
      pushStateSpy.mock.calls.length
    const replaceCountBeforePop =
      replaceStateSpy.mock.calls.length

    act(() => {
      simulatePopState(
        '/investigation/7',
        1,
      )
    })

    // React never adopts the traversed route while the upload is locked.
    expect(result.current.pathname).toBe('/new')
    // Browser moved Back by one entry, so traverse Forward by one entry
    // to return to the EXISTING /new entry.
    expect(goSpy).toHaveBeenCalledWith(1)
    // The blocker itself inserted/replaced NOTHING.
    expect(
      pushStateSpy.mock.calls.length,
    ).toBe(pushCountBeforePop)
    // simulatePopState itself contributed exactly one replaceState.
    expect(
      replaceStateSpy.mock.calls.length,
    ).toBe(replaceCountBeforePop + 1)
  })

  it('blocks Forward by traversing backward to the locked entry without inserting history', () => {
    const pushStateSpy = vi.spyOn(
      window.history,
      'pushState',
    )
    const goSpy = vi
      .spyOn(window.history, 'go')
      .mockImplementation(() => {})

    const { result, rerender } = renderHook(
      ({ locked }) => useRouter(locked),
      {
        initialProps: {
          locked: false,
        },
      },
    )

    act(() => {
      result.current.navigate('/investigation/7')
    })

    rerender({
      locked: true,
    })

    const pushCountBeforePop =
      pushStateSpy.mock.calls.length

    act(() => {
      simulatePopState(
        '/investigation/8',
        2,
      )
    })

    expect(
      result.current.pathname,
    ).toBe('/investigation/7')
    // Browser moved Forward by one, so go Back by one.
    expect(goSpy).toHaveBeenCalledWith(-1)
    expect(
      pushStateSpy.mock.calls.length,
    ).toBe(pushCountBeforePop)
  })

  it('does not block our OWN navigate() while locked and protects the upload-success handoff entry', () => {
    const goSpy = vi
      .spyOn(window.history, 'go')
      .mockImplementation(() => {})

    const { result } = renderHook(
      () => useRouter(true),
    )

    act(() => {
      result.current.navigate(
        '/investigation/42',
      )
    })

    expect(
      result.current.pathname,
    ).toBe('/investigation/42')
    expect(
      window.location.pathname,
    ).toBe('/investigation/42')
    expect(
      window.history.state[HISTORY_INDEX_KEY],
    ).toBe(1)

    // While the lock is still momentarily held, even Back from the newly
    // created handoff entry returns to that handoff rather than /new.
    act(() => {
      simulatePopState('/new', 0)
    })

    expect(
      result.current.pathname,
    ).toBe('/investigation/42')
    expect(goSpy).toHaveBeenCalledWith(1)
  })

  it('queues an upload-success navigation until an in-flight Back correction reaches the locked entry', () => {
    const pushStateSpy = vi.spyOn(
      window.history,
      'pushState',
    )
    const goSpy = vi
      .spyOn(window.history, 'go')
      .mockImplementation(() => {})

    const { result, rerender } = renderHook(
      ({ locked }) => useRouter(locked),
      {
        initialProps: {
          locked: false,
        },
      },
    )

    /*
     * Build:
     *
     * index 0 = initial
     * index 1 = /investigation/7
     * index 2 = /new
     *
     * Upload will lock at index 2.
     */
    act(() => {
      result.current.navigate(
        '/investigation/7',
      )
      result.current.navigate('/new')
    })

    expect(
      window.history.state[HISTORY_INDEX_KEY],
    ).toBe(2)

    rerender({
      locked: true,
    })

    /*
     * User presses Back.
     *
     * The real browser has already moved to index 1 before popstate fires.
     * Router schedules history.go(+1), but our mock deliberately does NOT
     * complete that traversal yet.
     */
    act(() => {
      simulatePopState(
        '/investigation/7',
        1,
      )
    })

    expect(goSpy).toHaveBeenCalledWith(1)
    // React still exposes the protected route.
    expect(result.current.pathname).toBe(
      '/new',
    )

    const pushesBeforeHandoff =
      pushStateSpy.mock.calls.length

    /*
     * THIS is the race Item 11 previously missed.
     *
     * Upload succeeds before history.go(+1) has completed.
     */
    act(() => {
      result.current.navigate(
        '/investigation/42',
      )
    })

    /*
     * /investigation/42 must NOT have been pushState()'d from temporary
     * history index 1. It must still be queued.
     */
    expect(
      pushStateSpy.mock.calls.length,
    ).toBe(pushesBeforeHandoff)
    expect(result.current.pathname).toBe(
      '/new',
    )

    /*
     * Upload releases its critical-operation lock before the corrective
     * traversal has completed.
     *
     * Pending correction ownership must survive this.
     */
    rerender({
      locked: false,
    })

    /*
     * Now model the popstate fired when the asynchronous history.go(+1)
     * finally returns the browser to the ORIGINAL locked entry:
     *
     * index 2 = /new
     */
    act(() => {
      simulatePopState(
        '/new',
        2,
      )
    })

    /*
     * Only NOW may the queued upload-success handoff push index 3.
     */
    expect(
      pushStateSpy.mock.calls.length,
    ).toBe(pushesBeforeHandoff + 1)
    expect(
      window.location.pathname,
    ).toBe('/investigation/42')
    expect(
      result.current.pathname,
    ).toBe('/investigation/42')
    expect(
      window.history.state[HISTORY_INDEX_KEY],
    ).toBe(3)
  })

  it('resumes normal Back/Forward state updates once no longer blocked', () => {
    const goSpy = vi
      .spyOn(window.history, 'go')
      .mockImplementation(() => {})

    const { result, rerender } = renderHook(
      ({ locked }) => useRouter(locked),
      {
        initialProps: {
          locked: false,
        },
      },
    )

    act(() => {
      result.current.navigate('/login')
      result.current.navigate('/new')
    })

    rerender({
      locked: true,
    })

    act(() => {
      simulatePopState('/login', 1)
    })

    expect(result.current.pathname).toBe('/new')
    expect(goSpy).toHaveBeenCalledWith(1)

    // history.go() is asynchronous. Model the corrective popstate that returns
    // the browser to the locked /new entry before checking normal unlocked
    // Back/Forward behavior.
    act(() => {
      simulatePopState('/new', 2)
    })

    rerender({
      locked: false,
    })

    goSpy.mockClear()

    act(() => {
      simulatePopState('/login', 1)
    })

    expect(result.current.pathname).toBe('/login')
    expect(goSpy).not.toHaveBeenCalled()
  })
})
