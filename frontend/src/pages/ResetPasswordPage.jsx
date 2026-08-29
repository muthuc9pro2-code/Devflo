import { useEffect, useState } from 'react'
import { resetPassword, resetPasswordStatus } from '../api/auth'
import { useAuth } from '../context/useAuth'
import { useRouter } from '../router/useRouter'
import { useEmailLinkToken } from '../utils/emailLinkToken'
import { ApiError } from '../api/client'

const AUTH_EVENTS_CHANNEL = 'devflo-auth-events'

function publishPasswordResetSuccess() {
  if (typeof window.BroadcastChannel !== 'function') return

  try {
    const channel = new window.BroadcastChannel(AUTH_EVENTS_CHANNEL)
    try {
      channel.postMessage({ type: 'password-reset-success' })
    } finally {
      channel.close()
    }
  } catch {
    // Same-browser notification is optional UX; reset success still stands.
  }
}

export default function ResetPasswordPage() {
  const { search, navigate } = useRouter()
  const { logout } = useAuth()
  const token = useEmailLinkToken('/reset-password', search)
  const [checkedToken, setCheckedToken] = useState(null)
  const [linkStatus, setLinkStatus] = useState(() => (token ? 'checking' : 'idle'))
  const [form, setForm] = useState({ newPassword: '', confirmPassword: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [success, setSuccess] = useState(false)

  // A newly-arrived token (same mounted page, different email link) must be
  // evaluated fresh - any stale form/error/success state from a previous
  // token must not linger on screen while this one is being classified.
  // Resetting it here, during render, is React's documented way to reset
  // state when a prop-like value changes without forcing a remount (see
  // https://react.dev/learn/you-might-not-need-an-effect) - the effect
  // below is left to do only the actual async status fetch.
  if (token && token !== checkedToken) {
    setCheckedToken(token)
    setLinkStatus('checking')
    setError('')
    setForm({ newPassword: '', confirmPassword: '' })
    setSuccess(false)
    setSubmitting(false)
  }

  useEffect(() => {
    if (!token) return

    let cancelled = false

    resetPasswordStatus(token)
      .then((result) => {
        if (cancelled) return
        setLinkStatus(result.status)
      })
      .catch(() => {
        if (cancelled) return
        // Fail closed: never fall open into showing the password form when
        // the link's status genuinely could not be determined.
        setLinkStatus('invalid')
      })

    return () => {
      cancelled = true
    }
  }, [token])

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')

    if (form.newPassword.length < 8) {
      setError('Password must be at least 8 characters.')
      return
    }
    if (form.newPassword !== form.confirmPassword) {
      setError('Passwords do not match.')
      return
    }

    setSubmitting(true)
    try {
      await resetPassword({ token, newPassword: form.newPassword })
      setForm({ newPassword: '', confirmPassword: '' })
      setSuccess(true)
      publishPasswordResetSuccess()
    } catch (err) {
      // The token may have been consumed by another device between the
      // last status check and this submission. Re-check once so that
      // specific race lands on the success screen instead of a confusing
      // error - any other rejection (including the same-current-password
      // one, which never changes token state) keeps its real message.
      if (err instanceof ApiError && err.status >= 400) {
        try {
          const recheck = await resetPasswordStatus(token)
          if (recheck.status === 'used') {
            setLinkStatus('used')
            return
          }
        } catch {
          // Best-effort UX only; fall through to the real error below.
        }
      }
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

  if (!token) {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <h1 className="brand">Devflo</h1>
          <p className="status-error" role="alert">This password reset link is missing its token.</p>
          <button className="btn-secondary" type="button" onClick={() => navigate('/forgot-password')}>
            Request a new link
          </button>
        </div>
      </div>
    )
  }

  if (success || linkStatus === 'used') {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <h1 className="brand">Devflo</h1>
          <p className="status-ok" role="status">Password reset successfully.</p>
          <button className="btn-primary" type="button" onClick={handleBackToSignIn}>
            Back to sign in
          </button>
        </div>
      </div>
    )
  }

  if (linkStatus === 'invalid') {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <h1 className="brand">Devflo</h1>
          <p className="status-error" role="alert">This password reset link is invalid or has expired.</p>
          <button className="btn-secondary" type="button" onClick={() => navigate('/forgot-password')}>
            Request a new link
          </button>
        </div>
      </div>
    )
  }

  if (linkStatus === 'checking') {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <h1 className="brand">Devflo</h1>
          <p className="tagline" role="status">Checking your reset link…</p>
        </div>
      </div>
    )
  }

  return (
    <div className="auth-shell">
      <form className="auth-card" onSubmit={handleSubmit}>
        <h1 className="brand">Devflo</h1>
        <p className="tagline">Choose a new password for your account.</p>

        {error && <div className="alert-error" role="alert">{error}</div>}

        <label className="field">
          <span>New password</span>
          <input
            name="newPassword"
            type="password"
            required
            minLength={8}
            maxLength={128}
            autoComplete="new-password"
            value={form.newPassword}
            onChange={handleChange}
          />
          <small className="field-hint">At least 8 characters.</small>
        </label>

        <label className="field">
          <span>Confirm new password</span>
          <input
            name="confirmPassword"
            type="password"
            required
            minLength={8}
            maxLength={128}
            autoComplete="new-password"
            value={form.confirmPassword}
            onChange={handleChange}
          />
        </label>

        <button className="btn-primary" type="submit" disabled={submitting}>
          {submitting ? 'Resetting…' : 'Reset password'}
        </button>
      </form>
    </div>
  )
}
