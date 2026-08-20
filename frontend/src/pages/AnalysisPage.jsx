import { useEffect, useMemo, useRef, useState } from 'react'
import {
  getAnalysisDetail,
  subscribeToAnalysisEvents,
} from '../api/analysis'
import { ApiError } from '../api/client'
import InvestigationResult from '../components/InvestigationResult'
import ProcessingView from '../components/ProcessingView'
import { useRouter } from '../router/useRouter'

const DURABLE_CHECK_INTERVAL_MS = 15_000
const RECONNECT_DELAY_MS = 1_200

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

export default function AnalysisPage({ analysisId, historyItem, onSettled, onStatusChange }) {
  const { navigate } = useRouter()
  const [view, setView] = useState(INITIAL_STATE)
  const [retryKey, setRetryKey] = useState(0)
  const settledCallbackRef = useRef(onSettled)
  const statusCallbackRef = useRef(onStatusChange)

  useEffect(() => {
    settledCallbackRef.current = onSettled
  }, [onSettled])

  useEffect(() => {
    statusCallbackRef.current = onStatusChange
  }, [onStatusChange])

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
    let requestInFlight = false
    let highWaterProgress = 0
    let hasReportedSettled = false
    let reportedStatus = null

    const clearTimers = () => {
      if (reconnectTimer) window.clearTimeout(reconnectTimer)
      if (durableTimer) window.clearTimeout(durableTimer)
      reconnectTimer = null
      durableTimer = null
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

    loadInitialState()

    return () => {
      disposed = true
      clearTimers()
      closeStream()
    }
  }, [analysisId, retryKey])

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
      />
    </div>
  )
}
