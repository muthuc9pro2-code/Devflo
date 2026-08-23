import { useEffect, useMemo, useRef, useState } from 'react'
import {
  cancelAnalysis,
  getAnalysisDetail,
  subscribeToAnalysisEvents,
} from '../api/analysis'
import { ApiError } from '../api/client'
import InvestigationResult from '../components/InvestigationResult'
import ProcessingView from '../components/ProcessingView'
import { useRouter } from '../router/useRouter'

const DURABLE_CHECK_INTERVAL_MS = 15_000
const RECONNECT_DELAY_MS = 1_200
// How long the brief "Analysis cancelled" notice stays up (direct
// navigation / reconnect / live-SSE-observed cancellation) before this
// page moves on by itself - a button-click cancel (Part N) skips this
// entirely and navigates away immediately, since the user already knows
// what they just did.
const CANCELLED_NOTICE_MS = 1_800

function readUploadManifest(analysisId) {
  try {
    const value = JSON.parse(sessionStorage.getItem(`devflo:analysis:${analysisId}`))
    return Array.isArray(value?.files) ? value.files.filter((item) => typeof item === 'string') : []
  } catch {
    return []
  }
}

function mergeArtifacts(current, incoming, replace = false) {
  const byId = new Map()
  if (!replace) {
    current.forEach((artifact) => byId.set(artifact.artifact_id, artifact))
  }
  incoming.forEach((artifact) => {
    if (artifact?.artifact_id === null || artifact?.artifact_id === undefined) return
    const previous = byId.get(artifact.artifact_id) || {}
    byId.set(artifact.artifact_id, { ...previous, ...artifact })
  })
  return [...byId.values()]
}

const INITIAL_STATE = {
  mode: 'loading',
  status: null,
  progress: 0,
  stage: '',
  message: '',
  connectionState: 'connecting',
  artifacts: [],
  result: null,
  error: '',
}

export default function AnalysisPage({ analysisId, historyItem, onSettled, onStatusChange, onCancelled }) {
  const { navigate } = useRouter()
  const [view, setView] = useState(INITIAL_STATE)
  const [retryKey, setRetryKey] = useState(0)
  const [cancelling, setCancelling] = useState(false)
  const [cancelError, setCancelError] = useState('')
  const settledCallbackRef = useRef(onSettled)
  const statusCallbackRef = useRef(onStatusChange)
  const cancelledCallbackRef = useRef(onCancelled)
  // Bridges the cancel button (rendered outside this effect) to the
  // request-cancellation closure defined inside it, which is the only
  // place with access to this mount's terminal/timer/stream state -
  // mirrors the opposite-direction settledCallbackRef/statusCallbackRef
  // bridges above.
  const requestCancelRef = useRef(() => {})

  useEffect(() => {
    settledCallbackRef.current = onSettled
  }, [onSettled])

  useEffect(() => {
    statusCallbackRef.current = onStatusChange
  }, [onStatusChange])

  useEffect(() => {
    cancelledCallbackRef.current = onCancelled
  }, [onCancelled])

  const manifestFiles = useMemo(() => readUploadManifest(analysisId), [analysisId])
  const filenames = manifestFiles.length > 0
    ? manifestFiles
    : (historyItem?.original_filename ? [historyItem.original_filename] : [])

  useEffect(() => {
    let disposed = false
    let terminal = false
    let closeEventSource = null
    let reconnectTimer = null
    let durableTimer = null
    let cancelledNoticeTimer = null
    let requestInFlight = false
    let highWaterProgress = 0
    let hasReportedSettled = false
    let reportedStatus = null

    const clearTimers = () => {
      if (reconnectTimer) window.clearTimeout(reconnectTimer)
      if (durableTimer) window.clearTimeout(durableTimer)
      if (cancelledNoticeTimer) window.clearTimeout(cancelledNoticeTimer)
      reconnectTimer = null
      durableTimer = null
      cancelledNoticeTimer = null
    }

    const closeStream = () => {
      closeEventSource?.()
      closeEventSource = null
    }

    const reportSettled = () => {
      if (hasReportedSettled) return
      hasReportedSettled = true
      settledCallbackRef.current?.()
    }

    const reportStatus = (status) => {
      if (!status || status === reportedStatus) return
      reportedStatus = status
      statusCallbackRef.current?.(analysisId, status)
    }

    const finishWithResult = (result) => {
      if (disposed || terminal || !result) return
      terminal = true
      reportStatus('completed')
      clearTimers()
      closeStream()
      setView((current) => ({
        ...current,
        mode: 'completed',
        status: 'completed',
        progress: 99,
        connectionState: 'closed',
        artifacts: Array.isArray(result.artifacts)
          ? mergeArtifacts([], result.artifacts, true)
          : current.artifacts,
        result,
        error: '',
      }))
      reportSettled()
    }

    const finishWithFailure = () => {
      if (disposed || terminal) return
      terminal = true
      reportStatus('failed')
      clearTimers()
      closeStream()
      setView((current) => ({
        ...current,
        mode: 'failed',
        status: 'failed',
        connectionState: 'closed',
        error: '',
      }))
      reportSettled()
    }

    // Shared by both cancellation-observation paths (durable state /
    // live SSE "cancelled" event, and the button-click success path
    // below): stop everything durably, tell AppShell so the item is
    // removed from local History immediately (never just relabeled -
    // see cancelledCallbackRef/onCancelled), and resync History for
    // good measure. Deliberately does NOT go through reportStatus -
    // AppShell's onStatusChange path only relabels an item in place,
    // which would leave a cancelled analysis visible in History.
    const markCancelledLocally = () => {
      terminal = true
      clearTimers()
      closeStream()
      cancelledCallbackRef.current?.(analysisId)
      reportSettled()
    }

    // Used when cancellation is *observed* rather than just performed by
    // this tab (direct navigation to an already-cancelled id, a durable
    // reconnect poll, or a live SSE "cancelled" event arriving while this
    // page is open) - briefly explains what happened, then moves on.
    const finishWithCancelled = () => {
      if (disposed || terminal) return
      markCancelledLocally()
      setView((current) => ({
        ...current,
        mode: 'cancelled',
        status: 'cancelled',
        connectionState: 'closed',
        error: '',
      }))
      cancelledNoticeTimer = window.setTimeout(() => {
        if (!disposed) navigate('/new')
      }, CANCELLED_NOTICE_MS)
    }

    const applyDurableState = (state) => {
      if (disposed || terminal || !state) return true
      if (state.status === 'completed' && state.investigation_result) {
        finishWithResult(state.investigation_result)
        return true
      }
      if (state.status === 'failed') {
        finishWithFailure()
        return true
      }
      if (state.status === 'cancelled') {
        finishWithCancelled()
        return true
      }
      if (state.status !== 'pending' && state.status !== 'processing') return false

      if (Number.isFinite(Number(state.progress))) {
        highWaterProgress = Math.max(highWaterProgress, Math.min(99, Number(state.progress)))
      }
      const nextStatus = reportedStatus === 'processing' && state.status === 'pending'
        ? 'processing'
        : state.status
      reportStatus(nextStatus)
      setView((current) => ({
        ...current,
        mode: 'live',
        status: nextStatus,
        progress: highWaterProgress,
        artifacts: Array.isArray(state.artifacts)
          ? mergeArtifacts(current.artifacts, state.artifacts)
          : current.artifacts,
        error: '',
      }))
      return false
    }

    const scheduleDurableCheck = () => {
      if (disposed || terminal) return
      if (durableTimer) window.clearTimeout(durableTimer)
      durableTimer = window.setTimeout(async () => {
        if (disposed || terminal) return
        if (requestInFlight) {
          scheduleDurableCheck()
          return
        }
        requestInFlight = true
        try {
          const state = await getAnalysisDetail(analysisId)
          if (!disposed) applyDurableState(state)
        } catch {
          // A transient detail failure does not change the analysis outcome.
        } finally {
          requestInFlight = false
          scheduleDurableCheck()
        }
      }, DURABLE_CHECK_INTERVAL_MS)
    }

    const openStream = () => {
      if (disposed || terminal || closeEventSource) return

      closeEventSource = subscribeToAnalysisEvents(analysisId, {
        onOpen: () => {
          if (disposed || terminal) return
          setView((current) => ({ ...current, connectionState: 'connected' }))
        },
        onState: (state) => {
          if (applyDurableState(state)) closeStream()
        },
        onProgress: (event) => {
          if (disposed || terminal) return
          if (Number.isFinite(Number(event.progress))) {
            highWaterProgress = Math.max(highWaterProgress, Math.min(99, Number(event.progress)))
          }
          reportStatus('processing')
          setView((current) => ({
            ...current,
            mode: 'live',
            status: current.status === 'pending' ? 'processing' : (current.status || 'processing'),
            progress: highWaterProgress,
            stage: event.stage || current.stage,
            message: event.message || current.message,
          }))
        },
        onArtifactOutcome: (artifact) => {
          if (disposed || terminal) return
          setView((current) => ({
            ...current,
            artifacts: mergeArtifacts(current.artifacts, [artifact]),
          }))
        },
        onResult: finishWithResult,
        onCancelled: finishWithCancelled,
        onError: () => {
          if (disposed || terminal) return
          closeStream()
          setView((current) => ({ ...current, connectionState: 'reconnecting' }))
          if (reconnectTimer) window.clearTimeout(reconnectTimer)

          const attemptReconnect = async () => {
            if (disposed || terminal) return
            if (requestInFlight) {
              reconnectTimer = window.setTimeout(attemptReconnect, RECONNECT_DELAY_MS)
              return
            }
            requestInFlight = true
            try {
              const state = await getAnalysisDetail(analysisId)
              const isTerminal = applyDurableState(state)
              if (!isTerminal && !disposed) openStream()
            } catch {
              if (!disposed && !terminal) {
                reconnectTimer = window.setTimeout(attemptReconnect, RECONNECT_DELAY_MS * 2)
              }
            } finally {
              requestInFlight = false
            }
          }

          reconnectTimer = window.setTimeout(attemptReconnect, RECONNECT_DELAY_MS)
        },
      })
    }

    const loadInitialState = async () => {
      requestInFlight = true
      try {
        const state = await getAnalysisDetail(analysisId)
        const isTerminal = applyDurableState(state)
        if (!isTerminal && !disposed) {
          openStream()
          scheduleDurableCheck()
        }
      } catch (error) {
        if (disposed) return
        if (error instanceof ApiError && error.status === 404) {
          terminal = true
          setView({ ...INITIAL_STATE, mode: 'not-found', error: 'This investigation could not be found.' })
          return
        }
        setView({
          ...INITIAL_STATE,
          mode: 'error',
          error: error instanceof ApiError ? error.message : 'Could not load this investigation.',
        })
      } finally {
        requestInFlight = false
      }
    }

    // Plain closure-local flag, not the `cancelling` React state - this
    // callback is created once per effect run (mirrors terminal/disposed
    // above) and reading component state here would only ever see the
    // value from mount time. The disabled button is the real double-click
    // guard; this just avoids two in-flight POSTs from this closure.
    let cancelRequestInFlight = false

    const requestCancel = async () => {
      if (disposed || terminal || cancelRequestInFlight) return
      cancelRequestInFlight = true
      setCancelling(true)
      setCancelError('')
      try {
        await cancelAnalysis(analysisId)
        if (disposed || terminal) return
        // Success is intentional and already understood by the user (they
        // just clicked the button) - no intermediate notice, straight to
        // /new, unlike the "observed elsewhere" finishWithCancelled path.
        markCancelledLocally()
        navigate('/new')
      } catch (error) {
        cancelRequestInFlight = false
        if (disposed) return
        setCancelling(false)
        setCancelError(
          error instanceof ApiError ? error.message : 'Could not cancel this investigation.',
        )
      }
    }
    requestCancelRef.current = requestCancel

    loadInitialState()

    return () => {
      disposed = true
      clearTimers()
      closeStream()
    }
  }, [analysisId, retryKey, navigate])

  if (view.mode === 'loading') {
    return (
      <div className="analysis-state-page" role="status">
        <span className="spinner spinner-large" aria-hidden="true" />
        <p>Loading investigation…</p>
      </div>
    )
  }

  if (view.mode === 'not-found' || view.mode === 'error') {
    return (
      <div className="analysis-state-page view-enter">
        <div className="state-icon state-icon-error" aria-hidden="true">!</div>
        <h1>{view.mode === 'not-found' ? 'Investigation not found' : 'Unable to load investigation'}</h1>
        <p>{view.error}</p>
        <div className="state-actions">
          {view.mode === 'error' && (
            <button
              type="button"
              className="btn-primary"
              onClick={() => {
                setView(INITIAL_STATE)
                setRetryKey((value) => value + 1)
              }}
            >
              Try again
            </button>
          )}
          <button type="button" className="btn-secondary" onClick={() => navigate('/new')}>
            New investigation
          </button>
        </div>
      </div>
    )
  }

  if (view.mode === 'failed') {
    return (
      <div className="analysis-state-page view-enter">
        <div className="state-icon state-icon-error" aria-hidden="true">!</div>
        <p className="eyebrow">Analysis failed</p>
        <h1>Devflo could not complete this investigation</h1>
        <p>The analysis stopped before a result was produced. Your history entry remains available.</p>
        <button type="button" className="btn-primary" onClick={() => navigate('/new')}>
          Start a new investigation
        </button>
      </div>
    )
  }

  if (view.mode === 'cancelled') {
    return (
      <div className="analysis-state-page view-enter">
        <div className="state-icon state-icon-warning" aria-hidden="true">–</div>
        <p className="eyebrow">Analysis cancelled</p>
        <h1>The investigation was cancelled and its generated analysis data was discarded.</h1>
        <button type="button" className="btn-primary" onClick={() => navigate('/new')}>
          New investigation
        </button>
      </div>
    )
  }

  if (view.mode === 'completed') {
    return <InvestigationResult result={view.result} />
  }

  return (
    <div className="analysis-live-page">
      <ProcessingView
        status={view.status}
        progress={view.progress}
        stage={view.stage}
        message={view.message}
        connectionState={view.connectionState}
        filenames={filenames}
        artifacts={view.artifacts}
        artifactCount={historyItem?.artifact_count || manifestFiles.length || view.artifacts.length}
        onCancel={() => requestCancelRef.current?.()}
        cancelling={cancelling}
        cancelError={cancelError}
      />
    </div>
  )
}
