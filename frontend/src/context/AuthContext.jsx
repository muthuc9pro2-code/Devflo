import { useCallback, useEffect, useRef, useState } from 'react'
import { getMe, login as apiLogin, logout as apiLogout, register as apiRegister } from '../api/auth'
import { AuthContext } from './auth-context'

// A verified access-token cookie is the only way `authenticated` happens:
// /auth/login rejects unverified accounts (403), so reaching this state
// already implies the account is email-verified.
export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [status, setStatus] = useState('loading') // loading | authenticated | unauthenticated
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
      return me
    } catch (error) {
      if (requestId !== sessionRequestRef.current) {
        if (throwOnFailure) throw error
        return null
      }
      setUser(null)
      setStatus('unauthenticated')
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
      })
      .catch(() => {
        if (cancelled || requestId !== sessionRequestRef.current) return
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
    <AuthContext.Provider value={{ user, status, login, register, logout, refreshSession }}>
      {children}
    </AuthContext.Provider>
  )
}
