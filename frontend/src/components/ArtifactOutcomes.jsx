import { humanize } from '../utils/format'

function artifactPresentation(artifact) {
  if (artifact.status === 'unsupported') return { label: 'Unsupported', tone: 'danger', symbol: '×' }
  if (artifact.status === 'duplicate') return { label: 'Duplicate', tone: 'muted', symbol: '↻' }

  const relationship = artifact.relationship_status
  if (relationship === 'linked') return { label: 'Linked', tone: 'success', symbol: '✓' }
  if (relationship === 'partially_linked') return { label: 'Partially linked', tone: 'warning', symbol: '△' }
  if (relationship === 'not_linked') return { label: 'Not linked', tone: 'muted', symbol: '○' }
  if (relationship === 'low_structure') return { label: 'Low structure', tone: 'warning', symbol: '△' }
  if (relationship === 'no_evidence') return { label: 'No evidence', tone: 'muted', symbol: '○' }
  if (artifact.status === 'processed') return { label: 'Analyzed', tone: 'success', symbol: '✓' }

  return { label: humanize(artifact.status || relationship || 'Recorded'), tone: 'muted', symbol: '○' }
}

export default function ArtifactOutcomes({ artifacts, defaultOpen = false }) {
  if (!Array.isArray(artifacts) || artifacts.length === 0) return null

  return (
    <details className="artifact-panel result-panel" open={defaultOpen}>
      <summary>
        <span>Diagnostic artifacts</span>
        <span className="summary-count">{artifacts.length}</span>
      </summary>
      <div className="artifact-list">
        {artifacts.map((artifact, index) => {
          const presentation = artifactPresentation(artifact)
          const detailMessage = artifact.message || (
            artifact.status === 'duplicate' && artifact.duplicate_of_source_file
              ? `Duplicate of ${artifact.duplicate_of_source_file}; not processed independently.`
              : null
          )
          return (
            <article className="artifact-row" key={artifact.artifact_id ?? `${artifact.source_file}:${index}`}>
              <span className={`artifact-symbol tone-${presentation.tone}`} aria-hidden="true">
                {presentation.symbol}
              </span>
              <div className="artifact-copy">
                <div className="artifact-title-row">
                  <strong title={artifact.source_file}>{artifact.source_file || 'Diagnostic artifact'}</strong>
                  <span className={`artifact-status tone-${presentation.tone}`}>{presentation.label}</span>
                </div>
                {detailMessage && <p>{detailMessage}</p>}
                <div className="artifact-meta">
                  {artifact.source_format && <span>{humanize(artifact.source_format)}</span>}
                  {Number.isFinite(Number(artifact.evidence_count)) && Number(artifact.evidence_count) > 0 && (
                    <span>{artifact.evidence_count} evidence {Number(artifact.evidence_count) === 1 ? 'record' : 'records'}</span>
                  )}
                </div>
              </div>
            </article>
          )
        })}
      </div>
    </details>
  )
}
