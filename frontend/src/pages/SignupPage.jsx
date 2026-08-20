import { useState } from 'react'
import { useAuth } from '../context/useAuth'
import { useRouter } from '../router/useRouter'
import { ApiError } from '../api/client'

const USERNAME_PATTERN = /^[A-Za-z0-9_]+$/
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/

function validateSignup(form) {
  const validation = {
    username: '',
    email: '',
    password: '',
    confirmPassword: '',
  }

  if (form.username.length < 3) {
    validation.username = 'Username must be at least 3 characters.'
  } else if (form.username.length > 30) {
    validation.username = 'Username must be 30 characters or fewer.'
  } else if (!USERNAME_PATTERN.test(form.username)) {
    validation.username = 'Use only letters, numbers and underscore.'
  }

  if (!EMAIL_PATTERN.test(form.email)) {
    validation.email = 'Enter a valid email address.'
  }

  if (form.password.length < 8) {
    validation.password = 'Password must be at least 8 characters.'
  } else if (form.password.length > 128) {
    validation.password = 'Password must be 128 characters or fewer.'
  }

  if (!form.confirmPassword) {
    validation.confirmPassword = 'Confirm your password.'
  } else if (form.confirmPassword !== form.password) {
    validation.confirmPassword = 'Passwords do not match.'
  }

  return validation
}

export default function SignupPage() {
  const { register } = useAuth()
  const { navigate } = useRouter()
  const [form, setForm] = useState({
    username: '',
    email: '',
    password: '',
    confirmPassword: '',
  })
  const [touched, setTouched] = useState({})
  const [submitted, setSubmitted] = useState(false)
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [registeredEmail, setRegisteredEmail] = useState(null)
  const validation = validateSignup(form)

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => ({ ...prev, [name]: value }))
    setError('')
  }

  const handleBlur = (event) => {
    setTouched((prev) => ({ ...prev, [event.target.name]: true }))
  }

  const visibleError = (name) => {
    if (!submitted && !touched[name]) return ''
    return validation[name]
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')
    setSubmitted(true)

    if (Object.values(validation).some(Boolean)) return

    setSubmitting(true)
    try {
      const result = await register({
        username: form.username,
        email: form.email,
        password: form.password,
      })
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
          <p className="status-ok" role="status">Check your email</p>
          <p className="tagline">
            We sent a verification link to <strong>{registeredEmail}</strong>. Open it to activate
            your account and sign in to Devflo automatically.
          </p>
          <button className="btn-primary" type="button" onClick={() => navigate('/login')}>
            Back to login
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="auth-shell">
      <form className="auth-card" onSubmit={handleSubmit} noValidate>
        <h1 className="brand">Devflo</h1>
        <p className="tagline">Create your account</p>

        {error && <div className="alert-error" role="alert">{error}</div>}

        <label className="field">
          <span>Username</span>
          <input
            name="username"
            type="text"
            required
            minLength={3}
            maxLength={30}
            pattern="[A-Za-z0-9_]+"
            autoComplete="username"
            autoCapitalize="none"
            spellCheck={false}
            value={form.username}
            onChange={handleChange}
            onBlur={handleBlur}
            aria-invalid={Boolean(visibleError('username'))}
            aria-describedby={`username-requirements${visibleError('username') ? ' username-error' : ''}`}
          />
          <small className="field-hint" id="username-requirements">
            3–30 characters. Letters, numbers and underscore only.
          </small>
          {visibleError('username') && (
            <small className="auth-field-error" id="username-error">
              {visibleError('username')}
            </small>
          )}
        </label>

        <label className="field">
          <span>Email</span>
          <input
            name="email"
            type="email"
            required
            autoComplete="email"
            autoCapitalize="none"
            spellCheck={false}
            value={form.email}
            onChange={handleChange}
            onBlur={handleBlur}
            aria-invalid={Boolean(visibleError('email'))}
            aria-describedby={visibleError('email') ? 'email-error' : undefined}
          />
          {visibleError('email') && (
            <small className="auth-field-error" id="email-error">
              {visibleError('email')}
            </small>
          )}
        </label>

        <label className="field">
          <span>Password</span>
          <input
            name="password"
            type="password"
            required
            minLength={8}
            maxLength={128}
            autoComplete="new-password"
            value={form.password}
            onChange={handleChange}
            onBlur={handleBlur}
            aria-invalid={Boolean(visibleError('password'))}
            aria-describedby={`password-requirements${visibleError('password') ? ' password-error' : ''}`}
          />
          <small className="field-hint" id="password-requirements">At least 8 characters.</small>
          {visibleError('password') && (
            <small className="auth-field-error" id="password-error">
              {visibleError('password')}
            </small>
          )}
        </label>

        <label className="field">
          <span>Confirm password</span>
          <input
            name="confirmPassword"
            type="password"
            required
            minLength={8}
            maxLength={128}
            autoComplete="new-password"
            value={form.confirmPassword}
            onChange={handleChange}
            onBlur={handleBlur}
            aria-invalid={Boolean(visibleError('confirmPassword'))}
            aria-describedby={visibleError('confirmPassword') ? 'confirm-password-error' : undefined}
          />
          {visibleError('confirmPassword') && (
            <small className="auth-field-error" id="confirm-password-error">
              {visibleError('confirmPassword')}
            </small>
          )}
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
