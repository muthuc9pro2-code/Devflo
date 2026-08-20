import { useCallback, useEffect, useState } from 'react'

const NAVIGATION_EVENT = 'devflo:navigation'

function readLocation() {
  return { pathname: window.location.pathname, search: window.location.search }
}

// Minimal history-API router. The app only has a handful of screens, so a
// full routing library would be more machinery than this needs.
export function useRouter() {
  const [location, setLocation] = useState(readLocation)

  useEffect(() => {
    const onPopState = () => setLocation(readLocation())
    window.addEventListener('popstate', onPopState)
    window.addEventListener(NAVIGATION_EVENT, onPopState)
    return () => {
      window.removeEventListener('popstate', onPopState)
      window.removeEventListener(NAVIGATION_EVENT, onPopState)
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
