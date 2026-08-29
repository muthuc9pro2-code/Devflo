import { useCallback, useMemo, useState } from 'react'
import { CriticalOperationContext } from './critical-operation-context'

// Coordinates one narrow fact across the whole app: is a critical,
// browser->server operation (currently: the diagnostic upload POST,
// before the durable Analysis exists) in flight right now. Deliberately
// tiny and in-memory only (no localStorage) - this is a live, per-tab
// request-lifetime flag, never something meant to survive a reload.
//
// AppShell/NewInvestigationPage already guard in-app navigation (sidebar,
// brand, logout, New investigation) against this flag locally. What only
// this shared context can do is let App.jsx's OWN top-level takeovers -
// a session expiring, or a backend 503 - defer unmounting the whole
// authenticated shell (and therefore abandoning the in-flight request)
// until the operation finishes, instead of tearing it down mid-request.
export function CriticalOperationProvider({ children }) {
  const [locked, setLockedState] = useState(false)

  const setLocked = useCallback((value) => {
    setLockedState(Boolean(value))
  }, [])

  const value = useMemo(() => ({ locked, setLocked }), [locked, setLocked])

  return (
    <CriticalOperationContext.Provider value={value}>
      {children}
    </CriticalOperationContext.Provider>
  )
}
