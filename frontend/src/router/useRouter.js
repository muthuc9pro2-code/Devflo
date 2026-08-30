import { useCallback, useEffect, useRef, useState } from 'react'

const NAVIGATION_EVENT = 'devflo:navigation'
const HISTORY_INDEX_KEY = '__devfloHistoryIndex'

function readLocation() {
  return {
    pathname: window.location.pathname,
    search: window.location.search,
  }
}

function stateObject(state) {
  return state && typeof state === 'object' ? state : {}
}

function readHistoryIndex(state = window.history.state) {
  const value = stateObject(state)[HISTORY_INDEX_KEY]
  return Number.isSafeInteger(value) ? value : null
}

function ensureCurrentHistoryIndex() {
  const existing = readHistoryIndex()
  if (existing !== null) {
    return existing
  }
  const index = 0
  // No URL argument: only attach Devflo metadata to the CURRENT entry.
  // This deliberately preserves pathname, search AND hash. The latter
  // matters for verification/reset bearer-token fragments.
  window.history.replaceState(
    {
      ...stateObject(window.history.state),
      [HISTORY_INDEX_KEY]: index,
    },
    '',
  )
  return index
}

// Minimal history-API router. The app only has a handful of screens, so a
// full routing library would be more machinery than this needs.
//
// While `blockPopState` is true, browser Back/Forward is bounced back to the
// history entry where the lock began. The important detail is HOW: we use
// history.go() to traverse back to the locked entry instead of pushState() or
// replaceState(). Traversal preserves the browser's existing back/forward
// topology; inserting a corrective entry would truncate/duplicate history and
// make navigation behave differently after the upload settles.
//
// Every Devflo-created history entry carries a local ordered index. That lets
// popstate determine whether the browser moved backward or forward and
// calculate the exact traversal needed to return. Our own navigate() calls
// remain allowed while locked (the upload-success handoff), and when they do
// navigate they also become the new locked entry.
export function useRouter(blockPopState = false) {
  const [location, setLocation] = useState(() => {
    ensureCurrentHistoryIndex()
    return readLocation()
  })
  const blockPopStateRef = useRef(blockPopState)
  const lockedIndexRef = useRef(null)

  useEffect(() => {
    blockPopStateRef.current = blockPopState
    if (blockPopState) {
      lockedIndexRef.current = ensureCurrentHistoryIndex()
    } else {
      lockedIndexRef.current = null
    }
  }, [blockPopState])

  useEffect(() => {
    const onPopState = (event) => {
      if (
        blockPopStateRef.current
        && lockedIndexRef.current !== null
      ) {
        const targetIndex = readHistoryIndex(event.state)
        if (
          targetIndex !== null
          && targetIndex !== lockedIndexRef.current
        ) {
          window.history.go(
            lockedIndexRef.current - targetIndex,
          )
        }
        // Never expose the traversed target as React router state while
        // blocked. For indexed Devflo entries, history.go() above returns
        // the browser to the exact locked entry without creating or
        // replacing any history entry.
        return
      }
      setLocation(readLocation())
    }
    const onInternalNavigation = () => {
      setLocation(readLocation())
    }

    window.addEventListener('popstate', onPopState)
    window.addEventListener(
      NAVIGATION_EVENT,
      onInternalNavigation,
    )
    return () => {
      window.removeEventListener('popstate', onPopState)
      window.removeEventListener(
        NAVIGATION_EVENT,
        onInternalNavigation,
      )
    }
  }, [])

  const navigate = useCallback(
    (to, { replace = false } = {}) => {
      if (
        `${window.location.pathname}${window.location.search}`
        === to
      ) {
        return
      }

      const currentIndex = ensureCurrentHistoryIndex()
      const nextIndex = replace
        ? currentIndex
        : currentIndex + 1
      const nextState = {
        ...stateObject(window.history.state),
        [HISTORY_INDEX_KEY]: nextIndex,
      }
      const method = replace
        ? 'replaceState'
        : 'pushState'
      window.history[method](
        nextState,
        '',
        to,
      )

      // Internal navigation while locked is intentional, specifically the
      // successful upload -> /investigation/:id handoff. That destination
      // becomes the new browser-history entry protected until unlock.
      if (blockPopStateRef.current) {
        lockedIndexRef.current = nextIndex
      }

      window.dispatchEvent(
        new Event(NAVIGATION_EVENT),
      )
    },
    [],
  )

  return {
    ...location,
    navigate,
  }
}
