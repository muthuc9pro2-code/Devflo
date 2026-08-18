import { useCallback, useEffect, useState } from 'react'
import { getMe, login as apiLogin, logout as apiLogout, register as apiRegister } from '../api/auth'
import { AuthContext } from './auth-context'

// A verified access-token cookie is the only way `authenticated` happens:
// /auth/login rejects unverified accounts (403), so reaching this state
// already implies the account is email-verified.
export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [status, setStatus] = useState('loading') // loading | authenticated | unauthenticated

  const refreshSession = useCallback(async () => {
    try {
      const me = await getMe()
      setUser(me)
      setStatus('authenticated')
      return me
    } catch {
      setUser(null)
      setStatus('unauthenticated')
      return null
    }
  }, [])

  useEffect(() => {
    let cancelled = false

    getMe()
      .then((me) => {
        if (cancelled) return
        setUser(me)
        setStatus('authenticated')
      })
      .catch(() => {
        if (cancelled) return
        setUser(null)
        setStatus('unauthenticated')
      })

    return () => {
      cancelled = true
    }
  }, [])

  const login = useCallback(
    async (credentials) => {
      await apiLogin(credentials)
      return refreshSession()
    },
    [refreshSession],
  )

  const register = useCallback((details) => apiRegister(details), [])

  const logout = useCallback(async () => {
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
