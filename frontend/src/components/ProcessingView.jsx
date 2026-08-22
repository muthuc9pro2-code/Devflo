import { useEffect, useMemo, useRef, useState } from 'react'
import { humanize } from '../utils/format'

function usePrefersReducedMotion() {
  const [reduced, setReduced] = useState(() => window.matchMedia('(prefers-reduced-motion: reduce)').matches)

  useEffect(() => {
    const query = window.matchMedia('(prefers-reduced-motion: reduce)')
    const onChange = () => setReduced(query.matches)
    query.addEventListener('change', onChange)
    return () => query.removeEventListener('change', onChange)
  }, [])

  return reduced
}

function useAnimatedProgress(target) {
  const reducedMotion = usePrefersReducedMotion()
  const [displayed, setDisplayed] = useState(target)
  const currentRef = useRef(target)

  useEffect(() => {
    const destination = Math.max(0, Math.min(99, Number(target) || 0))
    const start = currentRef.current
    if (reducedMotion || destination === start) {
      currentRef.current = destination
      setDisplayed(destination)
      return undefined
    }

    const duration = Math.min(520, 180 + Math.abs(destination - start) * 14)
    const startedAt = performance.now()
    let frame

    const tick = (now) => {
      const ratio = Math.min(1, (now - startedAt) / duration)
      const eased = 1 - (1 - ratio) ** 3
      const next = start + (destination - start) * eased
      currentRef.current = next
      setDisplayed(next)
      if (ratio < 1) frame = requestAnimationFrame(tick)
    }

    frame = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(frame)
  }, [reducedMotion, target])

  return displayed
}

function artifactLabel(artifact) {
  if (artifact.status === 'duplicate') return 'Duplicate'
  if (artifact.status === 'unsupported') return 'Unsupported'
  if (artifact.status === 'resource_limited') return 'Resource limit exceeded'
  if (artifact.status === 'processing_error') return 'Could not analyze'
  if (artifact.relationship_status) return humanize(artifact.relationship_status)
  if (artifact.status === 'processed' || artifact.status === 'completed') return 'Processed'
  return humanize(artifact.status)
}

function basename(path) {
  return path.split('/').pop()
}

function buildFileRows(filenames, artifacts) {
  const outcomes = artifacts.filter((artifact) => artifact?.source_file)
  const usedOutcomeIds = new Set()
  const basenameCounts = filenames.reduce((counts, filename) => {
    const name = basename(filename)
    counts.set(name, (counts.get(name) || 0) + 1)
    return counts
  }, new Map())
  const rows = filenames.map((filename, index) => {
    const name = basename(filename)
    const outcome = basenameCounts.get(name) === 1
      ? outcomes.find((candidate) => (
        !usedOutcomeIds.has(candidate.artifact_id)
        && candidate.source_file === name
      ))
      : null
    if (outcome) usedOutcomeIds.add(outcome.artifact_id)
    return { key: `selected:${index}:${filename}`, filename, outcome }
  })

  const ambiguousOutcomes = []
  outcomes.forEach((outcome) => {
    if (!usedOutcomeIds.has(outcome.artifact_id)) {
      if ((basenameCounts.get(outcome.source_file) || 0) > 1) {
        ambiguousOutcomes.push(outcome)
        return
      }
      rows.push({
        key: `outcome:${outcome.artifact_id}`,
        filename: outcome.source_file,
        outcome,
      })
    }
  })

  return { rows, ambiguousOutcomes }
}

export default function ProcessingView({
  status,
  progress,
  message,
  stage,
  connectionState,
  filenames,
  artifacts,
  artifactCount,
}) {
  const displayedProgress = useAnimatedProgress(progress)
  const { rows, ambiguousOutcomes } = useMemo(
    () => buildFileRows(filenames, artifacts),
    [artifacts, filenames],
  )
  const failureNotes = useMemo(
    () => rows.filter((row) => (
      row.outcome
      && ['resource_limited', 'processing_error'].includes(row.outcome.status)
      && row.outcome.message
    )),
    [rows],
  )
  const isPending = status === 'pending'
  const primaryMessage = isPending ? 'Queued for analysis' : (message || 'Analyzing diagnostics')
  const unknownCount = Math.max(0, (artifactCount || 0) - rows.length)
  const clampedProgress = Math.max(0, Math.min(99, displayedProgress))

  return (
    <section className="processing-view" aria-labelledby="processing-heading">
      <div className="processing-center">
        <div
          className="progress-plain"
          role="progressbar"
          aria-valuemin="0"
          aria-valuemax="99"
          aria-valuenow={progress}
          aria-label="Investigation progress"
        >
          <strong>{Math.round(displayedProgress)}</strong>
          <span>%</span>
        </div>

        <div className="processing-copy">
          <p className="eyebrow">{isPending ? 'Waiting for a worker' : (humanize(stage) || 'Investigation')}</p>
          <h1 id="processing-heading">{primaryMessage}</h1>
          <div className="progress-line" aria-hidden="true">
            <div className="progress-line-fill" style={{ width: `${clampedProgress}%` }} />
          </div>
          {connectionState === 'reconnecting' && (
            <p className="connection-status" role="status">
              <span className="status-dot" aria-hidden="true" />
              Reconnecting…
            </p>
          )}
        </div>
      </div>

      {(rows.length > 0 || unknownCount > 0) && (
        <div className="processing-files" aria-label="Uploaded diagnostics">
          <div className="processing-files-heading">
            <span>Diagnostics</span>
            {artifactCount > 0 && <span>{artifactCount} total</span>}
          </div>
          <div className="processing-files-scroll">
            <ol>
              {rows.map((row, index) => (
                <li key={row.key}>
                  <span className="processing-file-index">{String(index + 1).padStart(2, '0')}</span>
                  <span className="processing-file-name" title={row.filename}>{row.filename}</span>
                  <span className={`processing-file-status${row.outcome ? ' resolved' : ''}`}>
                    {row.outcome ? artifactLabel(row.outcome) : (isPending ? 'Included' : 'Analyzing')}
                  </span>
                </li>
              ))}
            </ol>
            {failureNotes.map((row) => (
              <p className="processing-files-note" key={`reason:${row.key}`}>
                {row.filename}: {row.outcome.message}
              </p>
            ))}
            {unknownCount > 0 && (
              <p className="processing-files-note">
                {unknownCount} additional diagnostic {unknownCount === 1 ? 'file is' : 'files are'} included in this investigation.
              </p>
            )}
            {ambiguousOutcomes.map((outcome) => (
              <p className="processing-files-note" key={`ambiguous:${outcome.artifact_id}`}>
                Backend outcome for {outcome.source_file}: {artifactLabel(outcome)}. The matching folder path is not exposed while processing.
              </p>
            ))}
          </div>
        </div>
      )}
    </section>
  )
}
