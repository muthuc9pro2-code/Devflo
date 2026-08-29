import { useState } from 'react'
import { useAuth } from '../context/useAuth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

export default function LoginPage() {
  const { login } = useAuth()
  const { search, navigate } = useRouter()
  const [form, setForm] = useState({ email: '', password: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  // Set only after the user's explicit reset-success action. Email bearer
  // credentials use fragments instead and are scrubbed before API submission.
  const [resetSuccess, setResetSuccess] = useState(
    () => new URLSearchParams(search).get('reset') === 'success',
  )

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')
    setResetSuccess(false)
    setSubmitting(true)
    try {
      await login(form)
      navigate('/new', { replace: true })
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Something went wrong. Please try again.')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="auth-shell">
      <form className="auth-card" onSubmit={handleSubmit}>
        <h1 className="brand">Devflo</h1>
        <p className="tagline">Sign in to start an investigation</p>

        {resetSuccess && !error && (
          <div className="alert-success" role="status">
            Password reset successfully. Sign in with your new password.
          </div>
        )}
        {error && <div className="alert-error" role="alert">{error}</div>}

        <label className="field">
          <span>Email</span>
          <input
            name="email"
            type="email"
            required
            autoComplete="email"
            value={form.email}
            onChange={handleChange}
          />
        </label>

        <label className="field">
          <span>Password</span>
          <input
            name="password"
            type="password"
            required
            maxLength={128}
            autoComplete="current-password"
            value={form.password}
            onChange={handleChange}
          />
        </label>

        <p className="forgot-password-link">
          <a
            href="/forgot-password"
            onClick={(event) => {
              event.preventDefault()
              navigate('/forgot-password')
            }}
          >
            Forgot password?
          </a>
        </p>

        <button className="btn-primary" type="submit" disabled={submitting}>
          {submitting ? 'Signing in…' : 'Sign in'}
        </button>

        <p className="switch-link">
          Don&apos;t have an account?{' '}
          <a
            href="/signup"
            onClick={(event) => {
              event.preventDefault()
              navigate('/signup')
            }}
          >
            Sign up
          </a>
        </p>
      </form>
    </div>
  )
}
