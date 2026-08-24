import { useState } from 'react'

// Rendered whenever AuthContext's `unavailable` flag is set - a controlled
// backend 503 (core DB/service unavailable), never a generic 4xx/5xx or a
// feature-specific failure (Gemini/source/etc. stay on their own existing
// UX). Reuses the same design language as AnalysisPage's failed/error
// terminal screens (analysis-state-page/state-icon/state-actions).
export default function ServiceUnavailablePage({ onRetry }) {
  const [retrying, setRetrying] = useState(false)

  const handleRetry = async () => {
    setRetrying(true)
    try {
      await onRetry?.()
    } finally {
      setRetrying(false)
    }
  }

  return (
    <div className="analysis-state-page view-enter">
      <div className="state-icon state-icon-error" aria-hidden="true">!</div>
      <p className="eyebrow">Service unavailable</p>
      <h1>Devflo is temporarily unavailable</h1>
      <p>Something went wrong on our side. Please try again in a moment.</p>
      <div className="state-actions">
        <button type="button" className="btn-primary" onClick={handleRetry} disabled={retrying}>
          {retrying ? 'Trying again…' : 'Try again'}
        </button>
      </div>
    </div>
  )
}
