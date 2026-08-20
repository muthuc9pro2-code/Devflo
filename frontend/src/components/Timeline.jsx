import { formatRelativeMs, formatTimestamp, humanize } from '../utils/format'

export default function Timeline({ entries }) {
  if (!Array.isArray(entries) || entries.length === 0) return null

  return (
    <section className="timeline-section result-section" aria-labelledby="timeline-heading">
      <div className="section-heading-row">
        <div>
          <p className="eyebrow">Deterministic sequence</p>
          <h2 id="timeline-heading">Incident timeline</h2>
        </div>
        <span>{entries.length} events</span>
      </div>
      <ol className="timeline-track">
        {entries.map((entry, index) => {
          const relative = formatRelativeMs(entry.relative_ms)
          return (
            <li className={`timeline-item role-${entry.role || 'uncorrelated'}`} key={`${entry.node_id}:${index}`}>
              <div className="timeline-time" title={formatTimestamp(entry.timestamp) || undefined}>
                {relative || 'Time unavailable'}
              </div>
              <div className="timeline-marker" aria-hidden="true" />
              <div className="timeline-copy">
                <span>{humanize(entry.role || 'uncorrelated')}</span>
                <strong>{entry.service || 'Diagnostic event'}</strong>
              </div>
            </li>
          )
        })}
      </ol>
    </section>
  )
}
