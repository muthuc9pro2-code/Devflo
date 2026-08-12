import { AuthProvider } from './context/AuthContext'
import { useAuth } from './context/useAuth'
import { useRouter } from './router/useRouter'
import LoginPage from './pages/LoginPage'
import SignupPage from './pages/SignupPage'
import VerifyEmailPage from './pages/VerifyEmailPage'
import DashboardPage from './pages/DashboardPage'

function Routes() {
  const { pathname } = useRouter()
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
    return <DashboardPage />
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


