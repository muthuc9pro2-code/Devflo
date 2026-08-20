import { AuthProvider } from './context/AuthContext'
import { useAuth } from './context/useAuth'
import { useRouter } from './router/useRouter'
import LoginPage from './pages/LoginPage'
import SignupPage from './pages/SignupPage'
import VerifyEmailPage from './pages/VerifyEmailPage'
import AppShell from './components/AppShell'

function Routes() {
  const { pathname, navigate } = useRouter()
  const { status } = useAuth()

  if (pathname === '/verify-email') {
    return <VerifyEmailPage />
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

  return pathname === '/signup' ? <SignupPage /> : <LoginPage />
}

function App() {
  return (
    <AuthProvider>
      <Routes />
    </AuthProvider>
  )
}

export default App
