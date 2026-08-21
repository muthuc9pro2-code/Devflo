function shortenTitle(title, maxWords = 6) {
  const words = title.trim().split(/\s+/)
  if (words.length <= maxWords) return title.trim()
  return `${words.slice(0, maxWords).join(' ')}…`
}

function historyTitle(item) {
  if (item.title) return shortenTitle(item.title)
  if (item.status === 'processing') return 'Investigation in progress'
  if (item.status === 'pending') return 'Investigation queued'
  if (item.status === 'failed') return 'Investigation failed'
  return shortenTitle(item.original_filename || 'Investigation')
}

function statusLabel(status) {
  if (status === 'pending') return 'Queued'
  if (status === 'processing') return 'Analyzing'
  if (status === 'failed') return 'Failed'
  return ''
}

function NewIcon() {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true">
      <path d="M10 4v12M4 10h12" />
    </svg>
  )
}

export default function HistorySidebar({
  items,
  activeAnalysisId,
  loading,
  loadingMore,
  error,
  nextCursor,
  user,
  onNavigate,
  onLoadMore,
  onRetry,
  onLogout,
  onClose,
  closeButtonRef,
}) {
  return (
    <aside className="sidebar" aria-label="Devflo navigation">
      <div className="sidebar-top">
        <div className="sidebar-brand-row">
          <a
            className="sidebar-brand"
            href="/new"
            onClick={(event) => {
              event.preventDefault()
              onNavigate('/new')
            }}
          >
            <span className="brand-mark" aria-hidden="true">D</span>
            <span className="brand">Devflo</span>
          </a>
          <button
            ref={closeButtonRef}
            type="button"
            className="sidebar-mobile-close"
            onClick={onClose}
            aria-label="Close navigation"
          >
            <span aria-hidden="true">×</span>
          </button>
        </div>
        <button className="new-investigation-button" type="button" onClick={() => onNavigate('/new')}>
          <NewIcon />
          <span>New investigation</span>
        </button>
      </div>

      <nav className="history-nav" aria-label="Investigation history">
        <div className="history-heading">History</div>

        {loading && items.length === 0 && (
          <div className="history-loading" role="status">
            <span className="spinner" aria-hidden="true" />
            Loading history
          </div>
        )}

        {error && items.length === 0 && (
          <div className="history-empty">
            <span>{error}</span>
            <button type="button" className="text-button" onClick={onRetry}>Try again</button>
          </div>
        )}

        {!loading && !error && items.length === 0 && (
          <div className="history-empty">Your investigations will appear here.</div>
        )}

        <ul className="history-list">
          {items.map((item) => {
            const isActive = Number(activeAnalysisId) === Number(item.analysis_id)
            const label = statusLabel(item.status)
            const title = historyTitle(item)

            return (
              <li key={item.analysis_id}>
                <a
                  href={`/investigation/${item.analysis_id}`}
                  className={`history-item${isActive ? ' active' : ''}`}
                  aria-current={isActive ? 'page' : undefined}
                  title={title}
                  onClick={(event) => {
                    event.preventDefault()
                    onNavigate(`/investigation/${item.analysis_id}`)
                  }}
                >
                  <span className="history-item-title">{title}</span>
                  {label && (
                    <span className={`history-status history-status-${item.status}`}>
                      <span className="status-dot" aria-hidden="true" />
                      {label}
                    </span>
                  )}
                </a>
              </li>
            )
          })}
        </ul>

        {error && items.length > 0 && (
          <div className="history-inline-error" role="alert">
            <span>{error}</span>
            <button type="button" className="text-button" onClick={onRetry}>Try again</button>
          </div>
        )}

        {nextCursor && (
          <button
            className="history-load-more"
            type="button"
            onClick={onLoadMore}
            disabled={loading || loadingMore}
          >
            {loadingMore ? 'Loading…' : 'Load more'}
          </button>
        )}
      </nav>

      <div className="sidebar-user">
        <div className="user-avatar" aria-hidden="true">
          {(user?.username || user?.email || 'D').slice(0, 1).toUpperCase()}
        </div>
        <div className="sidebar-user-copy">
          <strong>{user?.username || 'Devflo user'}</strong>
          <span title={user?.email}>{user?.email}</span>
        </div>
        <button type="button" className="sidebar-logout" onClick={onLogout} aria-label="Log out">
          <svg viewBox="0 0 20 20" aria-hidden="true">
            <path d="M8 4H5.5A1.5 1.5 0 0 0 4 5.5v9A1.5 1.5 0 0 0 5.5 16H8m4-3 3-3-3-3m3 3H8" />
          </svg>
        </button>
      </div>
    </aside>
  )
}
