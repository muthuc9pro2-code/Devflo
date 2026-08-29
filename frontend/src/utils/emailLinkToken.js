import { useLayoutEffect, useState } from 'react'

export function readEmailLinkToken(search) {
  const fragment = new URLSearchParams(window.location.hash.slice(1)).get('token')
  return fragment || new URLSearchParams(search).get('token')
}

export function clearEmailLinkToken(cleanPath) {
  window.history.replaceState(window.history.state, '', cleanPath)
}

// Captures a one-time #token=... email-link credential into React state and
// immediately scrubs it from the URL. A mobile browser or mail app may reuse
// an already-open tab: opening a second, newer email link for the same page
// only changes the URL fragment of the still-mounted page, which the
// browser treats as a same-document navigation (a 'hashchange' event, not a
// fresh page load/remount) - without listening for it, the stale first
// token would silently keep being submitted instead of the newer one.
// history.replaceState() below never itself fires 'hashchange', so scrubbing
// the URL can't create a loop with this listener.
export function useEmailLinkToken(cleanPath, search) {
  const [token, setToken] = useState(() => readEmailLinkToken(search))

  useLayoutEffect(() => {
    if (token) clearEmailLinkToken(cleanPath)
  }, [cleanPath, token])

  useLayoutEffect(() => {
    const onHashChange = () => {
      const next = readEmailLinkToken(search)
      if (!next) return
      setToken((current) => (next === current ? current : next))
    }
    window.addEventListener('hashchange', onHashChange)
    return () => window.removeEventListener('hashchange', onHashChange)
  }, [search])

  return token
}
