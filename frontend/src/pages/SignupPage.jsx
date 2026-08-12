import { useState } from 'react'
import { useAuth } from '../context/useAuth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

export default function SignupPage() {
  const { register } = useAuth()
  const { navigate } = useRouter()
  const [form, setForm] = useState({ username: '', email: '', password: '' })
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [registeredEmail, setRegisteredEmail] = useState(null)

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')
    setSubmitting(true)
    try {
      const result = await register(form)
      setRegisteredEmail(result.email)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Something went wrong. Please try again.')
    } finally {
      setSubmitting(false)
    }
  }

  if (registeredEmail) {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <h1 className="brand">Devflo</h1>
          <p className="status-ok">Check your email</p>
          <p className="tagline">
            We sent a verification link to <strong>{registeredEmail}</strong>. Open it to activate
            your account, then come back here and sign in.
          </p>
          <button className="btn-primary" onClick={() => navigate('/login')}>
            Back to login
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="auth-shell">
      <form className="auth-card" onSubmit={handleSubmit}>
        <h1 className="brand">Devflo</h1>
        <p className="tagline">Create your account</p>

        {error && <div className="alert-error">{error}</div>}

        <label className="field">
          <span>Username</span>
          <input
            name="username"
            type="text"
            required
            autoComplete="username"
            value={form.username}
            onChange={handleChange}
          />
        </label>

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
            autoComplete="new-password"
            value={form.password}
            onChange={handleChange}
          />
        </label>

        <button className="btn-primary" type="submit" disabled={submitting}>
          {submitting ? 'Creating account…' : 'Sign up'}
        </button>

        <p className="switch-link">
          Already have an account?{' '}
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
