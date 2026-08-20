import { useState } from 'react'
import { useAuth } from '../context/useAuth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

export default function LoginPage() {
  const { login } = useAuth()
  const { navigate } = useRouter()
  const [form, setForm] = useState({ email: '', password: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')
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
            autoComplete="current-password"
            value={form.password}
            onChange={handleChange}
          />
        </label>

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
