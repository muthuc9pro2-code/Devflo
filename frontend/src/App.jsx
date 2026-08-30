import { useEffect, useState } from 'react'
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
  const { locked } = useCriticalOperation()
  // blockPopState=locked: a real browser Back/Forward (or a mobile OS
  // history-back gesture) must not even change the address bar while a
  // critical operation is in flight - see useRouter's own docstring.
  const { pathname, navigate } = useRouter(locked)
  const { status, unavailable, refreshSession } = useAuth()

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

  // A 503 discovered by some OTHER request while an upload held the lock
  // is exactly the kind of stale outage state that must not immediately
  // paint ServiceUnavailablePage the instant the lock releases - the
  // backend may already have recovered by then. On the specific
  // locked->unlocked transition, while `unavailable` is (still) true, do
  // ONE bounded revalidation instead of trusting that stale flag: neither
  // a retry loop nor a busy-poll, just a single refreshSession() call
  // gated by this one transition, which itself can only ever fire once
  // per upload (locked can only go true->false once per upload cycle).
  // Keep a React-state snapshot of the previous lock value. This uses the
  // same guarded render-time state-adjustment pattern already used above for
  // frozenPathname: when locked changes, React immediately rerenders this
  // component before committing, so the first unlocked render can never
  // expose a stale 503 takeover.
  const [previousLocked, setPreviousLocked] = useState(locked)
  const [
    postUploadRevalidationPending,
    setPostUploadRevalidationPending,
  ] = useState(false)
  if (previousLocked !== locked) {
    if (previousLocked && !locked && unavailable) {
      setPostUploadRevalidationPending(true)
    }
    setPreviousLocked(locked)
  }
  useEffect(() => {
    if (!postUploadRevalidationPending) {
      return
    }
    refreshSession().finally(() => {
      setPostUploadRevalidationPending(false)
    })
  }, [postUploadRevalidationPending, refreshSession])

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

  // A locked -> unlocked transition with a deferred 503 first enters this
  // state during render itself, before React commits anything. AppShell
  // therefore remains mounted while refreshSession() determines whether the
  // outage signal is still real.
  if (postUploadRevalidationPending) {
    return <AppShell pathname={pathname} navigate={navigate} />
  }

  // A confirmed backend 503 (core DB/service unavailable) overrides every
  // other route - History, current Analysis state, and auth verification
  // are all unusable without the DB, so nothing else here can be trusted.
  // Deferred while the one bounded post-upload revalidation above is
  // still in flight, so a stale outage flag from during the upload cannot
  // paint this over a backend that has already recovered.
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
