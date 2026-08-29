import { useState } from 'react'
import { AuthProvider } from './context/AuthContext'
import { useAuth } from './context/useAuth'
import { CriticalOperationProvider } from './context/CriticalOperationContext'
import { useCriticalOperation } from './context/useCriticalOperation'
import { useRouter } from './router/useRouter'
import LoginPage from './pages/LoginPage'
import SignupPage from './pages/SignupPage'
import VerifyEmailPage from './pages/VerifyEmailPage'
import ForgotPasswordPage from './pages/ForgotPasswordPage'
import ResetPasswordPage from './pages/ResetPasswordPage'
import ServiceUnavailablePage from './pages/ServiceUnavailablePage'
import AppShell from './components/AppShell'

function Routes() {
  const { pathname, navigate } = useRouter()
  const { status, unavailable, refreshSession } = useAuth()
  const { locked } = useCriticalOperation()

  // Remembers the pathname as of the last UNLOCKED render, so a browser
  // Back/Forward navigation mid-upload changes the address bar but not
  // what is actually on screen - never a ref mutated during render (the
  // render phase must stay free of that); this is React's own sanctioned
  // "adjust state while rendering" shape, which re-renders once more
  // before committing rather than causing an effect-driven extra frame.
  const [frozenPathname, setFrozenPathname] = useState(pathname)
  if (!locked && frozenPathname !== pathname) {
    setFrozenPathname(pathname)
  }
  const effectivePathname = locked ? frozenPathname : pathname

  // `locked` can only ever be true once AppShell (and the
  // NewInvestigationPage inside it) is already mounted and has called
  // setLocked(true) itself - so its being true is already proof enough
  // that this is the view to keep showing. A critical operation (the
  // diagnostic upload) in flight must never be torn down by an App-level
  // takeover - a session expiring, or a backend 503 discovered by some
  // OTHER request - or by the address bar changing underneath it.
  if (locked) {
    return <AppShell pathname={effectivePathname} navigate={navigate} />
  }

  // A confirmed backend 503 (core DB/service unavailable) overrides every
  // other route - History, current Analysis state, and auth verification
  // are all unusable without the DB, so nothing else here can be trusted.
  if (unavailable) {
    return <ServiceUnavailablePage onRetry={refreshSession} />
  }

  if (pathname === '/verify-email') {
    return <VerifyEmailPage />
  }

  // Reachable regardless of auth status, same as /verify-email above - a
  // reset link is a token-scoped action, not something that should depend
  // on whether this browser also happens to have an existing session.
  if (pathname === '/reset-password') {
    return <ResetPasswordPage />
  }

  if (status === 'loading') {
    return (
      <div className="full-loader">
        <span className="brand">Devflo</span>
      </div>
    )
  }

  if (status === 'authenticated') {
    return <AppShell pathname={pathname} navigate={navigate} />
  }

  if (pathname === '/signup') {
    return <SignupPage />
  }

  if (pathname === '/forgot-password') {
    return <ForgotPasswordPage />
  }

  return <LoginPage />
}

function App() {
  return (
    <CriticalOperationProvider>
      <AuthProvider>
        <Routes />
      </AuthProvider>
    </CriticalOperationProvider>
  )
}

export default App
