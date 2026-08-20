function NarrativeSection({ title, children, className = '' }) {
  return (
    <section className={`narrative-section${className ? ` ${className}` : ''}`}>
      <h3>{title}</h3>
      {children}
    </section>
  )
}

export default function AnalysisNarrative({ analysis, showSummary = false }) {
  if (!analysis) {
    return (
      <section className="narrative-unavailable result-section">
        <p>
          <strong>Devflo AI explanation is temporarily unavailable.</strong>{' '}
          Your deterministic investigation is complete.
        </p>
      </section>
    )
  }

  const whatHappened = Array.isArray(analysis.what_happened) ? analysis.what_happened : []
  const causes = Array.isArray(analysis.probable_root_causes) ? analysis.probable_root_causes : []
  const actions = Array.isArray(analysis.recommended_actions) ? analysis.recommended_actions : []
  const findings = Array.isArray(analysis.source_code_findings) ? analysis.source_code_findings : []
  const uncertainties = Array.isArray(analysis.uncertainties) ? analysis.uncertainties : []
  const hasContent = (showSummary && analysis.summary)
    || whatHappened.length
    || causes.length
    || actions.length
    || findings.length
    || uncertainties.length

  if (!hasContent) return null

  return (
    <section className="analysis-narrative result-section" aria-labelledby="devflo-analysis-heading">
      <div className="section-heading-row">
        <div>
          <p className="eyebrow">Evidence-grounded explanation</p>
          <h2 id="devflo-analysis-heading">Devflo analysis</h2>
        </div>
      </div>

      {showSummary && analysis.summary && <p className="narrative-summary">{analysis.summary}</p>}

      <div className="narrative-grid">
        {whatHappened.length > 0 && (
          <NarrativeSection title="What happened" className="narrative-wide">
            <ol className="explanation-list">
              {whatHappened.map((item, index) => <li key={`${index}:${item}`}>{item}</li>)}
            </ol>
          </NarrativeSection>
        )}

        {causes.length > 0 && (
          <NarrativeSection title={causes.length === 1 ? 'Probable root cause' : 'Probable root causes'}>
            <div className="cause-list">
              {causes.map((cause, index) => (
                <article key={`${index}:${cause.title}`}>
                  <strong>{cause.title}</strong>
                  {cause.explanation && <p>{cause.explanation}</p>}
                  {cause.evidence_ids?.length > 0 && (
                    <span className="supporting-evidence">
                      {cause.evidence_ids.length} supporting evidence {cause.evidence_ids.length === 1 ? 'record' : 'records'}
                    </span>
                  )}
                </article>
              ))}
            </div>
          </NarrativeSection>
        )}

        {actions.length > 0 && (
          <NarrativeSection title="What to do next">
            <ol className="action-list">
              {actions.map((action, index) => (
                <li key={`${index}:${action}`}>
                  <span aria-hidden="true">{String(index + 1).padStart(2, '0')}</span>
                  <p>{action}</p>
                </li>
              ))}
            </ol>
          </NarrativeSection>
        )}

        {findings.length > 0 && (
          <NarrativeSection title="Source findings" className="narrative-wide">
            <div className="source-findings">
              {findings.map((finding, index) => (
                <article key={`${finding.file}:${finding.line_number ?? 'none'}:${index}`}>
                  <div className="source-finding-location">
                    <code>{finding.file}</code>
                    {finding.line_number !== null && finding.line_number !== undefined && (
                      <span>line {finding.line_number}</span>
                    )}
                    {finding.function && <span>{finding.function}()</span>}
                  </div>
                  <p>{finding.explanation}</p>
                </article>
              ))}
            </div>
          </NarrativeSection>
        )}

        {uncertainties.length > 0 && (
          <NarrativeSection title="Uncertainty" className="narrative-wide uncertainty-section">
            <ul>
              {uncertainties.map((uncertainty, index) => <li key={`${index}:${uncertainty}`}>{uncertainty}</li>)}
            </ul>
          </NarrativeSection>
        )}
      </div>
    </section>
  )
}
