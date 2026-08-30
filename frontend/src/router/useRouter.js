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
  return Number.isSafeInteger(value)
    ? value
    : null
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
// history.go() is asynchronous. If our own navigate() is requested while a
// corrective traversal is still returning to the locked entry, that navigation
// is queued and committed only after the corrective popstate reaches the exact
// locked history index. This prevents a successful upload handoff from calling
// pushState() while the browser is temporarily sitting on an older Back/Forward
// entry and accidentally truncating the forward branch.
//
// Every Devflo-created history entry carries a local ordered index. That lets
// popstate determine whether the browser moved backward or forward and
// calculate the exact traversal needed to return.
export function useRouter(blockPopState = false) {
  const [location, setLocation] = useState(() => {
    ensureCurrentHistoryIndex()
    return readLocation()
  })
  const blockPopStateRef = useRef(blockPopState)
  const lockedIndexRef = useRef(null)
  // A browser Back/Forward traversal has already happened, and history.go()
  // is asynchronously returning us to the protected entry.
  const correctionPendingRef = useRef(false)
  const correctionTargetIndexRef = useRef(null)
  // Internal navigation requested while that correction is still pending.
  // In the upload flow this is the successful /investigation/:id handoff.
  const queuedNavigationRef = useRef(null)

  useEffect(() => {
    blockPopStateRef.current = blockPopState
    if (blockPopState) {
      // If a corrective traversal is already in flight, its original target
      // stays authoritative. The browser may currently be sitting briefly on
      // the Back/Forward entry that triggered the correction, so do not
      // redefine the lock from that temporary location.
      if (!correctionPendingRef.current) {
        lockedIndexRef.current =
          ensureCurrentHistoryIndex()
      }
    } else if (!correctionPendingRef.current) {
      lockedIndexRef.current = null
    }
  }, [blockPopState])

  const commitNavigation = useCallback(
    (to, { replace = false } = {}) => {
      if (
        `${window.location.pathname}${window.location.search}`
        === to
      ) {
        return
      }

      const currentIndex =
        ensureCurrentHistoryIndex()
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
      // successful upload -> /investigation/:id handoff. Once committed from
      // the correct history entry, that destination becomes the new lock.
      if (blockPopStateRef.current) {
        lockedIndexRef.current = nextIndex
      }

      window.dispatchEvent(
        new Event(NAVIGATION_EVENT),
      )
    },
    [],
  )

  useEffect(() => {
    const onPopState = (event) => {
      const targetIndex =
        readHistoryIndex(event.state)

      /*
       * A previous blocked Back/Forward already initiated history.go().
       *
       * IMPORTANT:
       * This state remains authoritative even if blockPopState became false
       * in the meantime. Otherwise upload completion could release the lock
       * while the browser is still temporarily on an older entry and an
       * internal navigate() could push from there.
       */
      if (correctionPendingRef.current) {
        const correctionTargetIndex =
          correctionTargetIndexRef.current
        if (
          correctionTargetIndex !== null
          && targetIndex === correctionTargetIndex
        ) {
          // The browser has physically returned to the exact protected
          // history entry. It is now safe to perform a queued push/replace.
          correctionPendingRef.current = false
          correctionTargetIndexRef.current = null
          const queuedNavigation =
            queuedNavigationRef.current
          queuedNavigationRef.current = null
          if (queuedNavigation) {
            commitNavigation(
              queuedNavigation.to,
              {
                replace:
                  queuedNavigation.replace,
              },
            )
          } else if (!blockPopStateRef.current) {
            // No internal navigation was waiting and the critical operation
            // already ended. The browser and React state may now resume
            // ordinary unlocked synchronization.
            lockedIndexRef.current = null
            setLocation(readLocation())
          }
          return
        }
        /*
         * Another browser gesture can occur while the first corrective
         * traversal is still in flight. Keep steering toward the SAME
         * protected entry. Never adopt or mutate history from an intermediate
         * position.
         */
        if (
          correctionTargetIndex !== null
          && targetIndex !== null
          && targetIndex !== correctionTargetIndex
        ) {
          window.history.go(
            correctionTargetIndex - targetIndex,
          )
        }
        return
      }

      if (
        blockPopStateRef.current
        && lockedIndexRef.current !== null
      ) {
        if (
          targetIndex !== null
          && targetIndex !== lockedIndexRef.current
        ) {
          correctionPendingRef.current = true
          correctionTargetIndexRef.current =
            lockedIndexRef.current
          window.history.go(
            lockedIndexRef.current - targetIndex,
          )
        }
        // Never expose the temporary Back/Forward destination as React
        // router state while the critical operation is locked.
        return
      }

      setLocation(readLocation())
    }
    const onInternalNavigation = () => {
      setLocation(readLocation())
    }

    window.addEventListener(
      'popstate',
      onPopState,
    )
    window.addEventListener(
      NAVIGATION_EVENT,
      onInternalNavigation,
    )
    return () => {
      window.removeEventListener(
        'popstate',
        onPopState,
      )
      window.removeEventListener(
        NAVIGATION_EVENT,
        onInternalNavigation,
      )
    }
  }, [commitNavigation])

  const navigate = useCallback(
    (to, { replace = false } = {}) => {
      /*
       * history.go() has not finished returning the browser to the locked
       * entry yet.
       *
       * Do NOT pushState/replaceState from the temporary browser position.
       * Queue the destination and commit it from the protected entry when
       * the corrective popstate arrives.
       */
      if (correctionPendingRef.current) {
        // Last internal destination wins. During the upload lifecycle this
        // is normally exactly one /investigation/:id handoff.
        queuedNavigationRef.current = {
          to,
          replace,
        }
        return
      }
      commitNavigation(
        to,
        { replace },
      )
    },
    [commitNavigation],
  )

  return {
    ...location,
    navigate,
  }
}
