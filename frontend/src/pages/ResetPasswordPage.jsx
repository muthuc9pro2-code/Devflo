import { useState } from 'react'
import { resetPassword } from '../api/auth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

export default function ResetPasswordPage() {
  const { search, navigate } = useRouter()
  const token = new URLSearchParams(search).get('token')
  const [form, setForm] = useState({ newPassword: '', confirmPassword: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

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
      // Reset-password never authenticates the caller (no cookies are set) -
      // send the user back to the existing login form to sign in normally,
      // reusing the same search-param convention VerifyEmailPage's ?token=
      // already establishes for passing page-local state through the URL.
      navigate('/login?reset=success', { replace: true })
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Something went wrong. Please try again.')
    } finally {
      setSubmitting(false)
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
