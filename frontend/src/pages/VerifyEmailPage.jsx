import { useEffect, useLayoutEffect, useState } from 'react'
import { verifyEmail } from '../api/auth'
import { useRouter } from '../router/useRouter'
import { clearEmailLinkToken, readEmailLinkToken } from '../utils/emailLinkToken'
import { ApiError } from '../api/client'

export default function VerifyEmailPage() {
  const { search, navigate } = useRouter()
  const [token] = useState(() => readEmailLinkToken(search))
  const [state, setState] = useState(token ? 'verifying' : 'error') // verifying | success | error
  const [message, setMessage] = useState(token ? '' : 'This verification link is missing its token.')

  useLayoutEffect(() => {
    if (token) clearEmailLinkToken('/verify-email')
  }, [token])

  useEffect(() => {
    if (!token) return

    let cancelled = false

    verifyEmail(token)
      .then(() => {
        if (cancelled) return
        setState('success')
        setMessage('Email verified successfully.')
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
  }, [navigate, token])

  return (
    <div className="auth-shell">
      <div className="auth-card">
        <h1 className="brand">Devflo</h1>

        {state === 'verifying' && <p className="tagline" role="status">Verifying your email…</p>}

        {state === 'success' && (
          <>
            <p className="status-ok" role="status">{message}</p>
            <p className="tagline">You can return to the device where you signed up.</p>
            <button className="btn-primary" type="button" onClick={() => navigate('/login')}>
              Sign in on this device
            </button>
          </>
        )}

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
