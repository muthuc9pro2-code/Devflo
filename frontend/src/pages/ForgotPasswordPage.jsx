import { useEffect, useRef, useState } from 'react'
import { forgotPassword } from '../api/auth'
import { useAuth } from '../context/useAuth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

const DEFAULT_MESSAGE = 'If an account exists for this email, a password reset link has been sent.'
const AUTH_EVENTS_CHANNEL = 'devflo-auth-events'

export default function ForgotPasswordPage() {
  const { navigate } = useRouter()
  const { logout } = useAuth()
  const [email, setEmail] = useState('')
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [message, setMessage] = useState('')
  const [passwordChanged, setPasswordChanged] = useState(false)
  const awaitingReset = useRef(false)

  useEffect(() => {
    if (typeof window.BroadcastChannel !== 'function') return undefined

    let channel
    try {
      channel = new window.BroadcastChannel(AUTH_EVENTS_CHANNEL)
      channel.onmessage = (event) => {
        if (awaitingReset.current && event.data?.type === 'password-reset-success') {
          setPasswordChanged(true)
        }
      }
    } catch {
      return undefined
    }

    return () => {
      channel.onmessage = null
      channel.close()
    }
  }, [])

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')
    setSubmitting(true)
    try {
      const result = await forgotPassword({ email })
      awaitingReset.current = true
      setMessage(result?.message || DEFAULT_MESSAGE)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Something went wrong. Please try again.')
    } finally {
      setSubmitting(false)
    }
  }

  const handleBackToSignIn = async () => {
    try {
      await logout()
    } catch {
      // AuthContext still clears in-memory auth; stale cookies are version-invalid.
    } finally {
      navigate('/login?reset=success', { replace: true })
    }
  }

  if (message) {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <h1 className="brand">Devflo</h1>
          <p className="status-ok" role="status">
            {passwordChanged ? 'Password reset successfully.' : 'Check your email'}
          </p>
          {!passwordChanged && <p className="tagline">{message}</p>}
          <button
            className="btn-primary"
            type="button"
            onClick={passwordChanged ? handleBackToSignIn : () => navigate('/login')}
          >
            {passwordChanged ? 'Back to sign in' : 'Back to login'}
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="auth-shell">
      <form className="auth-card" onSubmit={handleSubmit}>
        <h1 className="brand">Devflo</h1>
        <p className="tagline">Enter your email and we&apos;ll send you a password reset link.</p>

        {error && <div className="alert-error" role="alert">{error}</div>}

        <label className="field">
          <span>Email</span>
          <input
            name="email"
            type="email"
            required
            autoComplete="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
          />
        </label>

        <button className="btn-primary" type="submit" disabled={submitting}>
          {submitting ? 'Sending…' : 'Send reset link'}
        </button>

        <p className="switch-link">
          Remembered your password?{' '}
          <a
            href="/login"
            onClick={(event) => {
              event.preventDefault()
              navigate('/login')
            }}
          >
            Sign in
          </a>
        </p>
      </form>
    </div>
  )
}
