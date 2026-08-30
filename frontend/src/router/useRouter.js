import { useCallback, useEffect, useRef, useState } from 'react'

const NAVIGATION_EVENT = 'devflo:navigation'

function readLocation() {
  return { pathname: window.location.pathname, search: window.location.search }
}

// Minimal history-API router. The app only has a handful of screens, so a
// full routing library would be more machinery than this needs.
//
// `blockPopState`, when true, makes browser Back/Forward (and any other
// popstate source - a mobile OS history-back gesture included) a no-op
// against the address bar itself: the browser has already changed
// window.location by the time popstate fires, so this immediately pushes
// the locked path back. window.history.pushState() never itself fires a
// popstate event, so this can never loop. It never blocks our OWN
// navigate() calls (dispatched via NAVIGATION_EVENT, a separate listener
// below) - those stay unconditional, which is exactly what lets an
// upload's own success handoff still navigate to the new analysis while
// the lock is still momentarily held (see NewInvestigationPage).
export function useRouter(blockPopState = false) {
  const [location, setLocation] = useState(readLocation)
  const blockPopStateRef = useRef(blockPopState)
  const lockedPathRef = useRef(null)

  useEffect(() => {
    blockPopStateRef.current = blockPopState
    lockedPathRef.current = blockPopState
      ? `${window.location.pathname}${window.location.search}`
      : null
  }, [blockPopState])

  useEffect(() => {
    const onPopState = () => {
      if (blockPopStateRef.current && lockedPathRef.current !== null) {
        const current = `${window.location.pathname}${window.location.search}`
        if (current !== lockedPathRef.current) {
          window.history.pushState({}, '', lockedPathRef.current)
        }
        return
      }
      setLocation(readLocation())
    }
    const onInternalNavigation = () => setLocation(readLocation())

    window.addEventListener('popstate', onPopState)
    window.addEventListener(NAVIGATION_EVENT, onInternalNavigation)
    return () => {
      window.removeEventListener('popstate', onPopState)
      window.removeEventListener(NAVIGATION_EVENT, onInternalNavigation)
    }
  }, [])

  const navigate = useCallback((to, { replace = false } = {}) => {
    if (`${window.location.pathname}${window.location.search}` === to) return

    const method = replace ? 'replaceState' : 'pushState'
    window.history[method]({}, '', to)
    window.dispatchEvent(new Event(NAVIGATION_EVENT))
  }, [])

  return { ...location, navigate }
}
