import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { getAnalysisHistory } from '../api/analysis'
import { ApiError } from '../api/client'
import { useAuth } from '../context/useAuth'
import AnalysisPage from '../pages/AnalysisPage'
import NewInvestigationPage from '../pages/NewInvestigationPage'
import HistorySidebar from './HistorySidebar'

function MenuIcon() {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true">
      <path d="M3 5h14M3 10h14M3 15h14" />
    </svg>
  )
}

function reconcileHistoryItem(incoming, existing) {
  if (!existing) return incoming
  if (existing.status === 'completed' || existing.status === 'failed') {
    return incoming.status === existing.status ? incoming : existing
  }
  if (existing.status === 'processing' && incoming.status === 'pending') {
    return { ...incoming, status: 'processing' }
  }
  return incoming
}

export default function AppShell({ pathname, navigate }) {
  const { user, logout } = useAuth()
  const [history, setHistory] = useState([])
  const [nextCursor, setNextCursor] = useState(null)
  const [historyLoading, setHistoryLoading] = useState(true)
  const [historyLoadingMore, setHistoryLoadingMore] = useState(false)
  const [historyError, setHistoryError] = useState('')
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [newInvestigationKey, setNewInvestigationKey] = useState(0)
  const [isMobile, setIsMobile] = useState(() => window.matchMedia('(max-width: 900px)').matches)
  const historyRequestRef = useRef(0)
  const liveStatusRef = useRef(new Map())
  const drawerRef = useRef(null)
  const drawerCloseRef = useRef(null)
  const menuButtonRef = useRef(null)

  const loadHistory = useCallback(async ({ cursor = null, append = false } = {}) => {
    const requestId = ++historyRequestRef.current
    if (append) {
      setHistoryLoadingMore(true)
    } else {
      setHistoryLoading(true)
      setHistoryLoadingMore(false)
    }
    setHistoryError('')

    try {
      const page = await getAnalysisHistory({ cursor, limit: 20 })
      if (requestId !== historyRequestRef.current) return
      setHistory((current) => {
        if (!append) {
          const currentById = new Map(current.map((item) => [Number(item.analysis_id), item]))
          return page.items.map((item) => {
            const analysisId = Number(item.analysis_id)
            const reconciled = reconcileHistoryItem(item, currentById.get(analysisId))
            const liveStatus = liveStatusRef.current.get(analysisId)
            return liveStatus
              ? reconcileHistoryItem(reconciled, { ...reconciled, status: liveStatus })
              : reconciled
          })
        }
        const known = new Set(current.map((item) => item.analysis_id))
        return [...current, ...page.items.filter((item) => !known.has(item.analysis_id))]
      })
      setNextCursor(page.next_cursor)
    } catch (error) {
      if (requestId !== historyRequestRef.current) return
      setHistoryError(
        error instanceof ApiError && error.status === 0
          ? 'History is temporarily unavailable.'
          : 'Could not load history.',
      )
    } finally {
      if (requestId === historyRequestRef.current) {
        setHistoryLoading(false)
        setHistoryLoadingMore(false)
      }
    }
  }, [])

  useEffect(() => {
    const timer = window.setTimeout(() => loadHistory(), 0)
    return () => window.clearTimeout(timer)
  }, [loadHistory])

  useEffect(() => {
    const refresh = () => loadHistory()
    window.addEventListener('devflo:history-refresh', refresh)
    return () => window.removeEventListener('devflo:history-refresh', refresh)
  }, [loadHistory])

  useEffect(() => {
    const media = window.matchMedia('(max-width: 900px)')
    const update = () => {
      setIsMobile(media.matches)
      if (!media.matches) setDrawerOpen(false)
    }
    media.addEventListener('change', update)
    return () => media.removeEventListener('change', update)
  }, [])

  useEffect(() => {
    if (!isMobile || !drawerOpen) return undefined

    const previouslyFocused = document.activeElement
    const menuButton = menuButtonRef.current
    const focusTimer = window.setTimeout(() => drawerCloseRef.current?.focus(), 0)
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        setDrawerOpen(false)
        return
      }
      if (event.key !== 'Tab') return

      const focusable = [...(drawerRef.current?.querySelectorAll(
        'a[href], button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ) || [])].filter((element) => !element.hasAttribute('inert'))
      if (focusable.length === 0) {
        event.preventDefault()
        return
      }

      const first = focusable[0]
      const last = focusable[focusable.length - 1]
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault()
        last.focus()
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault()
        first.focus()
      }
    }

    document.addEventListener('keydown', onKeyDown)
    return () => {
      window.clearTimeout(focusTimer)
      document.removeEventListener('keydown', onKeyDown)
      if (previouslyFocused instanceof HTMLElement && document.contains(previouslyFocused)) {
        previouslyFocused.focus()
      } else {
        menuButton?.focus()
      }
    }
  }, [drawerOpen, isMobile])

  const analysisMatch = pathname.match(/^\/investigation\/(\d+)\/?$/)
  const analysisId = analysisMatch?.[1] || null
  const historyItem = useMemo(
    () => history.find((item) => Number(item.analysis_id) === Number(analysisId)) || null,
    [analysisId, history],
  )
  const isNewRoute = pathname === '/new' || pathname === '/new/'
  const isKnownRoute = isNewRoute || analysisId !== null

  useEffect(() => {
    if (!isKnownRoute) navigate('/new', { replace: true })
  }, [isKnownRoute, navigate])

  const handleNavigate = useCallback((to) => {
    setDrawerOpen(false)
    if (to === '/new') setNewInvestigationKey((value) => value + 1)
    navigate(to)
  }, [navigate])

  const handleUploaded = useCallback((analysis) => {
    navigate(`/investigation/${analysis.id}`)
  }, [navigate])

  const handleSettled = useCallback(() => {
    loadHistory()
  }, [loadHistory])

  const handleStatusChange = useCallback((changedAnalysisId, status) => {
    liveStatusRef.current.set(Number(changedAnalysisId), status)
    setHistory((current) => current.map((item) => {
      if (Number(item.analysis_id) !== Number(changedAnalysisId)) return item
      if (item.status === 'completed' || item.status === 'failed') return item
      if (item.status === 'processing' && status === 'pending') return item
      return { ...item, status }
    }))
  }, [])

  const handleLogout = useCallback(async () => {
    try {
      await logout()
    } catch {
      // AuthContext clears local session state even if the server is unavailable.
    } finally {
      navigate('/login', { replace: true })
    }
  }, [logout, navigate])

  return (
    <div className="authenticated-shell">
      <div
        id="devflo-sidebar"
        ref={drawerRef}
        className={`sidebar-drawer${drawerOpen ? ' open' : ''}`}
        aria-hidden={isMobile && !drawerOpen ? 'true' : undefined}
        inert={isMobile && !drawerOpen}
      >
        <HistorySidebar
          items={history}
          activeAnalysisId={analysisId}
          loading={historyLoading}
          loadingMore={historyLoadingMore}
          error={historyError}
          nextCursor={nextCursor}
          user={user}
          onNavigate={handleNavigate}
          onLoadMore={() => loadHistory({ cursor: nextCursor, append: true })}
          onRetry={() => loadHistory()}
          onLogout={handleLogout}
          onClose={() => setDrawerOpen(false)}
          closeButtonRef={drawerCloseRef}
        />
      </div>
      {drawerOpen && (
        <button
          type="button"
          className="drawer-scrim"
          aria-label="Close navigation"
          onClick={() => setDrawerOpen(false)}
        />
      )}

      <div
        className="workspace"
        aria-hidden={isMobile && drawerOpen ? 'true' : undefined}
        inert={isMobile && drawerOpen}
      >
        <header className="mobile-header">
          <button
            type="button"
            className="mobile-menu-button"
            ref={menuButtonRef}
            onClick={() => setDrawerOpen(true)}
            aria-label="Open navigation"
            aria-controls="devflo-sidebar"
            aria-expanded={drawerOpen}
          >
            <MenuIcon />
          </button>
          <span className="brand">Devflo</span>
          <span className="mobile-header-spacer" />
        </header>

        <main className={`workspace-main${analysisId ? ' analysis-workspace' : ''}`}>
          {isNewRoute && (
            <NewInvestigationPage key={newInvestigationKey} onUploaded={handleUploaded} />
          )}
          {analysisId && (
            <AnalysisPage
              key={analysisId}
              analysisId={analysisId}
              historyItem={historyItem}
              onSettled={handleSettled}
              onStatusChange={handleStatusChange}
            />
          )}
        </main>
      </div>
    </div>
  )
}
