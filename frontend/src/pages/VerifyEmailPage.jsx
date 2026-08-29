import { useEffect, useState } from 'react'
import { verifyEmail } from '../api/auth'
import { useRouter } from '../router/useRouter'
import { useEmailLinkToken } from '../utils/emailLinkToken'
import { ApiError } from '../api/client'

export default function VerifyEmailPage() {
  const { search, navigate } = useRouter()
  const token = useEmailLinkToken('/verify-email', search)
  const [checkedToken, setCheckedToken] = useState(null)
  const [state, setState] = useState(() => (token ? 'verifying' : 'error')) // verifying | success | error
  const [message, setMessage] = useState(() => (token ? '' : 'This verification link is missing its token.'))

  // A newly-arrived token (same mounted page, different email link) must be
  // verified fresh rather than leaving a previous token's success/error
  // state on screen. Resetting it here, during render, is React's
  // documented way to reset state when a prop-like value changes without
  // forcing a remount - the effect below only does the actual async call.
  if (token && token !== checkedToken) {
    setCheckedToken(token)
    setState('verifying')
    setMessage('')
  }

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
  }, [token])

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
