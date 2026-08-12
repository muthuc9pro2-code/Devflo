import { useEffect, useState } from 'react'
import { verifyEmail } from '../api/auth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

export default function VerifyEmailPage() {
  const { search, navigate } = useRouter()
  const token = new URLSearchParams(search).get('token')
  const [state, setState] = useState(token ? 'verifying' : 'error') // verifying | success | error
  const [message, setMessage] = useState(token ? '' : 'This verification link is missing its token.')

  useEffect(() => {
    if (!token) return

    let cancelled = false

    verifyEmail(token)
      .then((result) => {
        if (cancelled) return
        setState('success')
        setMessage(result.message || 'Email verified successfully.')
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
  }, [token])

  return (
    <div className="auth-shell">
      <div className="auth-card">
        <h1 className="brand">Devflo</h1>

        {state === 'verifying' && <p className="tagline">Verifying your email…</p>}

        {state === 'success' && (
          <>
            <p className="status-ok">{message}</p>
            <button className="btn-primary" onClick={() => navigate('/login')}>
              Go to login
            </button>
          </>
        )}

        {state === 'error' && (
          <>
            <p className="status-error">{message}</p>
            <button className="btn-secondary" onClick={() => navigate('/login')}>
              Back to login
            </button>
          </>
        )}
      </div>
    </div>
  )
}
