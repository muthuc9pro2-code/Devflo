import { useCallback, useEffect, useState } from 'react'

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
    return () => window.removeEventListener('popstate', onPopState)
  }, [])

  const navigate = useCallback((to) => {
    window.history.pushState({}, '', to)
    setLocation(readLocation())
  }, [])

  return { ...location, navigate }
}
