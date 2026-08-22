import AnalysisNarrative from './AnalysisNarrative'
import ArtifactOutcomes from './ArtifactOutcomes'
import CorrelationGraph from './CorrelationGraph'
import DiagnosticDetails from './DiagnosticDetails'
import Timeline from './Timeline'

function resultTitle(result) {
  if (result.ai_analysis?.title) return result.ai_analysis.title
  if (result.investigation_path === 'zero_evidence') return 'No meaningful diagnostic evidence found'
  if (result.investigation_path === 'correlated') return 'Correlated incident investigation'
  return 'Diagnostic investigation'
}

function pathLabel(path) {
  if (path === 'correlated') return 'Correlated investigation'
  if (path === 'zero_evidence') return 'Evidence review'
  return 'Diagnostic analysis'
}

function ResultHeader({ result }) {
  const artifactCount = Array.isArray(result.artifacts) ? result.artifacts.length : 0
  const evidenceCount = Number.isFinite(Number(result.evidence_count)) ? Number(result.evidence_count) : null

  return (
    <header className="result-header">
      <p className="eyebrow">Investigation result</p>
      <h1>{resultTitle(result)}</h1>
      {result.investigation_path !== 'correlated' && result.ai_analysis?.summary && (
        <p className="result-summary">{result.ai_analysis.summary}</p>
      )}
      <div className="result-meta">
        <span>{pathLabel(result.investigation_path)}</span>
        {artifactCount > 0 && <><i aria-hidden="true" /> <span>{artifactCount} artifact{artifactCount === 1 ? '' : 's'}</span></>}
        {evidenceCount !== null && <><i aria-hidden="true" /> <span>{evidenceCount} evidence {evidenceCount === 1 ? 'record' : 'records'}</span></>}
      </div>
    </header>
  )
}

function SourceOutcome({ source }) {
  if (!source) return null
  const isReady = source.status === 'ready'

  return (
    <section className="zero-evidence-card result-panel">
      <div className="zero-evidence-mark" aria-hidden="true">{isReady ? '✓' : '○'}</div>
      <div>
        <h2>Source code</h2>
        {isReady ? (
          <p>Available for source correlation.</p>
        ) : (
          <>
            <p>{source.failure_reason || 'Source code could not be prepared.'}</p>
            <p className="muted-copy">Investigation completed without source correlation.</p>
          </>
        )}
      </div>
    </section>
  )
}

function SimpleResult({ result }) {
  return (
    <>
      <AnalysisNarrative analysis={result.ai_analysis} />
      <ArtifactOutcomes artifacts={result.artifacts} defaultOpen={(result.artifacts?.length || 0) <= 4} />
      {result.evidence_truncated && (
        <p className="bounded-result-note">
          Showing {result.evidence_count_returned} of {result.evidence_count} retained evidence records in this result view.
        </p>
      )}
      <DiagnosticDetails
        evidence={result.evidence}
        fallbackContext={result.fallback_context}
        supplementalContext={result.supplemental_low_structure_context}
      />
    </>
  )
}

function ZeroEvidenceResult({ result }) {
  return (
    <>
      <section className="zero-evidence-card result-panel">
        <div className="zero-evidence-mark" aria-hidden="true">○</div>
        <div>
          <h2>Analysis finished without a diagnosis</h2>
          <p>{result.message || 'Devflo did not find meaningful diagnostic evidence in the supplied material.'}</p>
          <p className="muted-copy">No root cause or incident relationship has been inferred.</p>
        </div>
      </section>
      <ArtifactOutcomes artifacts={result.artifacts} defaultOpen />
    </>
  )
}

function CorrelatedResult({ result }) {
  const component = result.components?.[0]

  return (
    <>
      {component ? (
        <>
          <CorrelationGraph component={component} />
          <Timeline entries={component.timeline} />
        </>
      ) : (
        <section className="empty-graph result-panel">
          <h2>Primary incident graph unavailable</h2>
          <p>This stored correlated result does not include a primary component to visualize.</p>
        </section>
      )}
      {result.evidence_truncated && (
        <p className="bounded-result-note">
          The primary graph uses {result.evidence_count_returned} of {result.evidence_count} retained evidence records selected by the backend&apos;s bounded result contract.
        </p>
      )}
      <ArtifactOutcomes artifacts={result.artifacts} />
      <AnalysisNarrative analysis={result.ai_analysis} showSummary />
    </>
  )
}

export default function InvestigationResult({ result }) {
  if (!result) return null

  return (
    <article className={`result-page result-${result.investigation_path || 'unknown'} view-enter`}>
      <ResultHeader result={result} />
      <SourceOutcome source={result.source} />
      {result.investigation_path === 'correlated' && <CorrelatedResult result={result} />}
      {result.investigation_path === 'simple' && <SimpleResult result={result} />}
      {result.investigation_path === 'zero_evidence' && <ZeroEvidenceResult result={result} />}
      {!['correlated', 'simple', 'zero_evidence'].includes(result.investigation_path) && (
        <section className="result-panel">
          <h2>Result unavailable</h2>
          <p>This stored investigation uses a result format this frontend does not recognize.</p>
        </section>
      )}
    </article>
  )
}
