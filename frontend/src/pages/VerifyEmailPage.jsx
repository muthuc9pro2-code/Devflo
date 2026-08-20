import { useEffect, useState } from 'react'
import { verifyEmail } from '../api/auth'
import { useRouter } from '../router/useRouter'
import { useAuth } from '../context/useAuth'
import { ApiError } from '../api/client'

export default function VerifyEmailPage() {
  const { search, navigate } = useRouter()
  const { refreshSession } = useAuth()
  const token = new URLSearchParams(search).get('token')
  const [state, setState] = useState(token ? 'verifying' : 'error') // verifying | success | error
  const [message, setMessage] = useState(token ? '' : 'This verification link is missing its token.')

  useEffect(() => {
    if (!token) return

    let cancelled = false

    verifyEmail(token)
      .then(async (result) => {
        if (cancelled) return
        // /auth/verify-email already set the same access/refresh cookies
        // /auth/login does - refreshSession() just picks up that cookie
        // state so the app enters as authenticated, with no re-entered
        // credentials and no token carried in the URL.
        await refreshSession({ throwOnFailure: true })
        if (cancelled) return
        setState('success')
        setMessage(result.message || 'Email verified successfully.')
        navigate('/new', { replace: true })
      })
      .catch((err) => {
        if (cancelled) return
        setState('error')
        setMessage(
          err instanceof ApiError
            ? err.message
            : 'Verification failed. The link may have expired.',
        )
      })

    return () => {
      cancelled = true
    }
  }, [token, refreshSession, navigate])

  return (
    <div className="auth-shell">
      <div className="auth-card">
        <h1 className="brand">Devflo</h1>

        {state === 'verifying' && <p className="tagline" role="status">Verifying your email…</p>}

        {state === 'success' && <p className="status-ok" role="status">{message}</p>}

        {state === 'error' && (
          <>
            <p className="status-error" role="alert">{message}</p>
            <button className="btn-secondary" type="button" onClick={() => navigate('/login')}>
              Back to login
            </button>
          </>
        )}
      </div>
    </div>
  )
}
