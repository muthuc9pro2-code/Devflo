import { useState } from 'react'
import { formatTimestamp, humanize, isPresent } from '../utils/format'

function SourceMatch({ match }) {
  return (
    <div className="source-match">
      <div className="source-finding-location">
        <code>{match.relative_path}</code>
        {isPresent(match.line_number) && <span>line {match.line_number}</span>}
        {match.function && <span>{match.function}()</span>}
        {match.confidence && <span>{humanize(match.confidence)} confidence</span>}
      </div>
      {match.snippet && <pre><code>{match.snippet}</code></pre>}
    </div>
  )
}

function EvidenceRecord({ evidence }) {
  const headline = evidence.service || evidence.module || evidence.event_type || evidence.source_file || 'Evidence record'
  const timestamp = formatTimestamp(evidence.first_seen)
  const sourceMatches = Array.isArray(evidence.source_matches) ? evidence.source_matches : []

  return (
    <article className="evidence-record">
      <div className="evidence-record-heading">
        <strong>{headline}</strong>
        {evidence.severity && <span className={`severity severity-${String(evidence.severity).toLowerCase()}`}>{evidence.severity}</span>}
      </div>
      <div className="evidence-meta">
        {evidence.event_type && <span>{humanize(evidence.event_type)}</span>}
        {evidence.source_file && (
          <span>{evidence.source_file}{isPresent(evidence.first_line_number) ? `:${evidence.first_line_number}` : ''}</span>
        )}
        {timestamp && <span>{timestamp}</span>}
        {isPresent(evidence.occurrence_count) && <span>{evidence.occurrence_count} occurrence{evidence.occurrence_count === 1 ? '' : 's'}</span>}
      </div>
      {evidence.representative_line && <pre className="diagnostic-text"><code>{evidence.representative_line}</code></pre>}
      {sourceMatches.map((match, index) => <SourceMatch key={`${match.relative_path}:${match.line_number}:${index}`} match={match} />)}
    </article>
  )
}

export default function DiagnosticDetails({ evidence, fallbackContext, supplementalContext }) {
  const [open, setOpen] = useState(false)
  const records = Array.isArray(evidence) ? evidence : []
  const fallback = Array.isArray(fallbackContext) ? fallbackContext : []
  const supplemental = Array.isArray(supplementalContext) ? supplementalContext : []
  const count = records.length + fallback.length + supplemental.length

  if (count === 0) return null

  return (
    <details className="diagnostic-details result-panel" onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>
        <span>Evidence and diagnostic context</span>
        <span className="summary-count">{count}</span>
      </summary>
      {open && (
        <div className="evidence-list">
          {records.map((record, index) => <EvidenceRecord key={record.id ?? index} evidence={record} />)}
          {fallback.map((context, index) => (
            <article className="evidence-record" key={`fallback:${context.artifact_id}:${index}`}>
              <div className="evidence-record-heading">
                <strong>{context.source_file || 'Diagnostic context'}</strong>
                {context.kind && <span>{humanize(context.kind)}</span>}
              </div>
              <pre className="diagnostic-text"><code>{context.text}</code></pre>
            </article>
          ))}
          {supplemental.length > 0 && (
            <div className="supplemental-context">
              <p>Additional low-structure context is shown separately and is not treated as linked incident evidence.</p>
              {supplemental.map((context, index) => (
                <article className="evidence-record" key={`supplemental:${context.artifact_id}:${index}`}>
                  <strong>{context.source_file || 'Additional context'}</strong>
                  <pre className="diagnostic-text"><code>{context.text}</code></pre>
                </article>
              ))}
            </div>
          )}
        </div>
      )}
    </details>
  )
}
