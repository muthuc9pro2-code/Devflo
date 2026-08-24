import { useCallback, useEffect, useRef, useState } from 'react'
import { getMe, login as apiLogin, logout as apiLogout, register as apiRegister } from '../api/auth'
import { ApiError } from '../api/client'
import { AuthContext } from './auth-context'

// A verified access-token cookie is the only way `authenticated` happens:
// /auth/login rejects unverified accounts (403), so reaching this state
// already implies the account is email-verified.
export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [status, setStatus] = useState('loading') // loading | authenticated | unauthenticated
  // A controlled backend 503 (core DB/service unavailable) is NOT a logout:
  // /auth/me failing that way must not be reinterpreted as "session
  // invalid" (see the catch blocks below and reportServiceUnavailable in
  // api/client.js). `status` is deliberately left untouched whenever this
  // is set - App.jsx renders the global unavailable screen ahead of any
  // status-based routing, and once service returns, status resolves the
  // normal way (authenticated or genuinely unauthenticated).
  const [unavailable, setUnavailable] = useState(false)
  const sessionRequestRef = useRef(0)

  const refreshSession = useCallback(async ({ throwOnFailure = false } = {}) => {
    const requestId = ++sessionRequestRef.current
    try {
      const me = await getMe()
      if (requestId !== sessionRequestRef.current) {
        if (throwOnFailure) throw new Error('Session changed before authentication completed.')
        return null
      }
      setUser(me)
      setStatus('authenticated')
      setUnavailable(false)
      return me
    } catch (error) {
      if (requestId !== sessionRequestRef.current) {
        if (throwOnFailure) throw error
        return null
      }
      if (error instanceof ApiError && error.status === 503) {
        setUnavailable(true)
        if (throwOnFailure) throw error
        return null
      }
      setUser(null)
      setStatus('unauthenticated')
      setUnavailable(false)
      if (throwOnFailure) throw error
      return null
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    const requestId = ++sessionRequestRef.current

    getMe()
      .then((me) => {
        if (cancelled || requestId !== sessionRequestRef.current) return
        setUser(me)
        setStatus('authenticated')
        setUnavailable(false)
      })
      .catch((error) => {
        if (cancelled || requestId !== sessionRequestRef.current) return
        if (error instanceof ApiError && error.status === 503) {
          setUnavailable(true)
          return
        }
        setUser(null)
        setStatus('unauthenticated')
      })

    return () => {
      cancelled = true
      if (requestId === sessionRequestRef.current) sessionRequestRef.current += 1
    }
  }, [])

  useEffect(() => {
    const onSessionExpired = () => {
      sessionRequestRef.current += 1
      setUser(null)
      setStatus('unauthenticated')
    }
    window.addEventListener('devflo:session-expired', onSessionExpired)
    return () => window.removeEventListener('devflo:session-expired', onSessionExpired)
  }, [])

  // Centralized 503 signal (api/client.js) from ANY request, not just
  // /auth/me - a DB outage discovered while e.g. loading History or
  // polling an analysis must reach this same global state.
  useEffect(() => {
    const onServiceUnavailable = () => setUnavailable(true)
    window.addEventListener('devflo:service-unavailable', onServiceUnavailable)
    return () => window.removeEventListener('devflo:service-unavailable', onServiceUnavailable)
  }, [])

  const login = useCallback(
    async (credentials) => {
      sessionRequestRef.current += 1
      await apiLogin(credentials)
      return refreshSession({ throwOnFailure: true })
    },
    [refreshSession],
  )

  const register = useCallback((details) => apiRegister(details), [])

  const logout = useCallback(async () => {
    sessionRequestRef.current += 1
    try {
      await apiLogout()
    } finally {
      setUser(null)
      setStatus('unauthenticated')
    }
  }, [])

  return (
    <AuthContext.Provider value={{ user, status, unavailable, login, register, logout, refreshSession }}>
      {children}
    </AuthContext.Provider>
  )
}
