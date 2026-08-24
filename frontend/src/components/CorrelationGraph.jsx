import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import {
  BaseEdge,
  Background,
  BackgroundVariant,
  ControlButton,
  Controls,
  EdgeLabelRenderer,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  getSmoothStepPath,
  getStraightPath,
  useNodesState,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { formatRelativeMs, formatTimestamp, humanize, isPresent } from '../utils/format'

const ROLE_ORDER = { root: 0, propagation: 1, victim: 2, uncorrelated: 3 }
const NODE_WIDTH = 264
const NODE_HEIGHT_ESTIMATE = 132
const COLUMN_STEP = 360
const ROW_STEP = 172
const UNCORRELATED_GAP = 118
const EDGE_STEP_OFFSET = 18
const DIRECTED_EDGE_LANES = [0.34, 0.5, 0.66]
const FOCUSABLE_SELECTOR = [
  'button:not([disabled])',
  '[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

function nodeHeadline(node) {
  return node.service || node.module || node.event_type || node.source_file || 'Diagnostic event'
}

function nodeSubtitle(node) {
  const headline = nodeHeadline(node)
  return [node.event_type, node.module, node.source_file].find((value) => value && value !== headline) || null
}

function IncidentNode({ data, selected }) {
  const { node, relativeMs, isAssociationEndpoint } = data
  const relative = formatRelativeMs(relativeMs)
  const role = node.role || 'uncorrelated'
  const source = node.source_file || node.source_format
  const identity = node.request_id || node.trace_id || node.resolved_identity

  return (
    <div className={`incident-node role-${role}${selected ? ' selected' : ''}${isAssociationEndpoint ? ' association-endpoint' : ''}`}>
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <div className="incident-node-topline">
        <span className="node-role">{humanize(role)}</span>
        {node.severity && <span className="node-severity">{node.severity}</span>}
      </div>
      <strong title={nodeHeadline(node)}>{nodeHeadline(node)}</strong>
      {nodeSubtitle(node) && <p className="node-subtitle" title={nodeSubtitle(node)}>{nodeSubtitle(node)}</p>}
      <div className="incident-node-context">
        {source && <span title={source}>{source}</span>}
        {relative && <span>{relative}</span>}
      </div>
      <div className="incident-node-meta">
        {identity && <span title={identity}>{identity}</span>}
        {isPresent(node.http_status) && <span>HTTP {node.http_status}</span>}
        {Number(node.occurrence_count) > 1 && <span>×{node.occurrence_count}</span>}
      </div>
      <Handle type="source" position={Position.Right} isConnectable={false} />
    </div>
  )
}

const NODE_TYPES = { incident: IncidentNode }

function TimingEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerStart,
  markerEnd,
  interactionWidth,
  pathOptions,
  style: edgeStyle,
  data,
}) {
  const isStraight = data?.pathType === 'straight'
  const pathArguments = { sourceX, sourceY, targetX, targetY }

  const pathResult = isStraight
    ? getStraightPath(pathArguments)
    : getSmoothStepPath({
        ...pathArguments,
        sourcePosition,
        targetPosition,
        borderRadius: pathOptions?.borderRadius,
        offset: pathOptions?.offset,
        stepPosition: pathOptions?.stepPosition,
      })

  const [edgePath, labelX, labelY] = pathResult
  const showTimingLabel = !isStraight
    && data?.showTimingLabel
    && isPresent(data?.deltaMs)

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerStart={markerStart}
        markerEnd={markerEnd}
        interactionWidth={interactionWidth}
        style={edgeStyle}
      />
      {showTimingLabel && (
        <EdgeLabelRenderer>
          <div
            className="edge-timing-chip"
            style={{
              transform: `translate(-50%, -50%) translate(${labelX + (data?.labelOffsetX || 0)}px, ${labelY + (data?.labelOffsetY || 0)}px)`,
            }}
          >
            {formatRelativeMs(data.deltaMs)}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

const EDGE_TYPES = { timing: TimingEdge }

function FitViewIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M8 3H3v5M16 3h5v5M8 21H3v-5M16 21h5v-5" />
      <path d="m8 8 3 3m5-3-3 3m-5 5 3-3m5 3-3-3" />
    </svg>
  )
}

function FullscreenIcon({ active }) {
  return active ? (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M8 3v5H3M16 3v5h5M8 21v-5H3M16 21v-5h5" />
    </svg>
  ) : (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M8 3H3v5M16 3h5v5M8 21H3v-5M16 21h5v-5" />
    </svg>
  )
}

function AssociationToggle({ count, active, onToggle }) {
  if (count === 0) return null

  return (
    <button
      type="button"
      className={`association-toggle${active ? ' active' : ''}`}
      aria-pressed={active}
      onClick={onToggle}
    >
      <span className="association-line" aria-hidden="true" />
      {active ? 'Hide associations' : `Show associations (${count})`}
    </button>
  )
}

// Slim vertical scrollbar that mirrors/drives the React Flow viewport's Y
// position - lets the user move up/down through a zoomed-in graph, which
// horizontal trackpad pan and drag alone don't make obvious. contentBounds
// is in flow-space (unscaled) coordinates; paneHeight is the real rendered
// pixel height of the React Flow pane.
function GraphVScrollbar({ viewport, paneHeight, contentBounds, onScrollTo }) {
  const dragStartRef = useRef(null)

  if (!contentBounds || paneHeight <= 0) return null

  const contentHeightPx = (contentBounds.bottom - contentBounds.top) * viewport.zoom
  if (contentHeightPx - paneHeight <= 4) return null

  const thumbFraction = Math.min(1, Math.max(paneHeight / contentHeightPx, 0.06))
  const thumbHeight = thumbFraction * paneHeight
  const trackTravel = Math.max(0, paneHeight - thumbHeight)

  const yAtTop = -contentBounds.top * viewport.zoom
  const yAtBottom = paneHeight - contentBounds.bottom * viewport.zoom
  const scrollRange = yAtTop - yAtBottom
  const scrollFraction = scrollRange > 0
    ? Math.min(1, Math.max(0, (yAtTop - viewport.y) / scrollRange))
    : 0
  const thumbTop = scrollFraction * trackTravel

  const handlePointerDown = (event) => {
    if (event.button != null && event.button !== 0) return
    event.preventDefault()
    event.stopPropagation()
    event.currentTarget.setPointerCapture(event.pointerId)
    dragStartRef.current = { clientY: event.clientY, fraction: scrollFraction }
  }

  const handlePointerMove = (event) => {
    const start = dragStartRef.current
    if (!start || scrollRange <= 0) return
    const deltaFraction = trackTravel > 0 ? (event.clientY - start.clientY) / trackTravel : 0
    const nextFraction = Math.min(1, Math.max(0, start.fraction + deltaFraction))
    onScrollTo(yAtTop - nextFraction * scrollRange)
  }

  const handlePointerUp = (event) => {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId)
    }
    dragStartRef.current = null
  }

  return (
    <div className="graph-vscroll" aria-hidden="true">
      <div
        className="graph-vscroll-thumb"
        style={{ height: `${thumbHeight}px`, transform: `translateY(${thumbTop}px)` }}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
      />
    </div>
  )
}

function buildFlowNodes(component) {
  const timelineById = new Map(
    (component.timeline || []).map((entry) => [String(entry.node_id), entry]),
  )
  const nodes = [...(component.nodes || [])]
  const directed = component.edges || []
  const validIds = new Set(nodes.map((node) => String(node.id)))

  // Compute a deterministic left-to-right depth from the backend's directed DAG.
  // Role is still authoritative and visible; depth only prevents dense graphs from
  // collapsing every propagation node into one giant visual column.
  const incoming = new Map(nodes.map((node) => [String(node.id), []]))
  directed.forEach((edge) => {
    const source = String(edge.source_id ?? edge.source)
    const target = String(edge.target_id ?? edge.target)
    if (validIds.has(source) && validIds.has(target)) incoming.get(target).push(source)
  })
  const depthMemo = new Map()
  const depthOf = (id, visiting = new Set()) => {
    if (depthMemo.has(id)) return depthMemo.get(id)
    if (visiting.has(id)) return 0
    const parents = incoming.get(id) || []
    if (parents.length === 0) { depthMemo.set(id, 0); return 0 }
    const nextVisiting = new Set(visiting); nextVisiting.add(id)
    const depth = Math.min(12, 1 + Math.max(...parents.map((parent) => depthOf(parent, nextVisiting))))
    depthMemo.set(id, depth)
    return depth
  }

  const sorted = nodes.sort((left, right) => {
    const leftId = String(left.id)
    const rightId = String(right.id)
    const depthDifference = depthOf(leftId) - depthOf(rightId)
    if (depthDifference !== 0) return depthDifference
    const roleDifference = (ROLE_ORDER[left.role] ?? 3) - (ROLE_ORDER[right.role] ?? 3)
    if (roleDifference !== 0) return roleDifference
    const leftTime = timelineById.get(leftId)?.relative_ms
    const rightTime = timelineById.get(rightId)?.relative_ms
    if (isPresent(leftTime) && isPresent(rightTime) && Number(leftTime) !== Number(rightTime)) return Number(leftTime) - Number(rightTime)
    if (isPresent(leftTime) !== isPresent(rightTime)) return isPresent(leftTime) ? -1 : 1
    return leftId.localeCompare(rightId)
  })

  const rowsByDepth = new Map()
  const uncorrelated = []
  sorted.forEach((node) => {
    const id = String(node.id)
    if ((node.role || 'uncorrelated') === 'uncorrelated') {
      uncorrelated.push(node)
      return
    }
    const depth = depthOf(id)
    if (!rowsByDepth.has(depth)) rowsByDepth.set(depth, [])
    rowsByDepth.get(depth).push(node)
  })
  const maxPrimaryRows = Math.max(1, ...[...rowsByDepth.values()].map((items) => items.length))

  return sorted.map((node, revealIndex) => {
    const id = String(node.id)
    const timelineEntry = timelineById.get(id)
    const role = node.role || 'uncorrelated'
    let x
    let y
    if (role === 'uncorrelated') {
      const index = uncorrelated.findIndex((item) => String(item.id) === id)
      x = index * (NODE_WIDTH + 54)
      y = 58 + maxPrimaryRows * ROW_STEP + UNCORRELATED_GAP
    } else {
      const depth = depthOf(id)
      const column = rowsByDepth.get(depth) || []
      const row = column.findIndex((item) => String(item.id) === id)
      x = depth * COLUMN_STEP
      y = 58 + row * ROW_STEP
    }

    return {
      id,
      type: 'incident',
      position: { x, y },
      data: { node, relativeMs: timelineEntry?.relative_ms },
      className: 'graph-reveal-node',
      style: { '--reveal-delay': `${Math.min(420, 50 + revealIndex * 26)}ms` },
    }
  })
}

function buildFlowEdges(component, showAssociations, nodeIndex) {
  const validNodeIds = new Set((component.nodes || []).map((node) => String(node.id)))
  const directedEdges = []
  const associationEdges = []

  ;(component.edges || []).forEach((edge, index) => {
    const source = String(edge.source_id ?? edge.source)
    const target = String(edge.target_id ?? edge.target)
    if (!validNodeIds.has(source) || !validNodeIds.has(target)) return
    const revealAt = Math.max(nodeIndex.get(source) || 0, nodeIndex.get(target) || 0)
    // Backend now generates "explicit_parent_child" for an exact parent-span
    // match; legacy persisted result_snapshot JSON from before that rename
    // may still contain "causal" for the exact same relationship - both get
    // the same strongest-directed-edge visual treatment.
    const isExplicitParentChild = edge.relationship_type === 'explicit_parent_child'
      || edge.relationship_type === 'causal'
    const lane = ((nodeIndex.get(source) || 0) + (nodeIndex.get(target) || 0)) % DIRECTED_EDGE_LANES.length
    directedEdges.push({
      id: `directed:${index}:${source}:${target}`,
      source,
      target,
      type: 'timing',
      className: `graph-reveal-edge edge-${edge.relationship_type || 'inferred'}`,
      markerEnd: { type: MarkerType.ArrowClosed, color: isExplicitParentChild ? '#62d8ca' : '#91a0b8' },
      style: {
        stroke: isExplicitParentChild ? '#62d8ca' : '#91a0b8',
        strokeWidth: isExplicitParentChild ? 1.8 : 1.45,
        '--reveal-delay': `${Math.min(680, 180 + revealAt * 44)}ms`,
      },
      pathOptions: {
        offset: EDGE_STEP_OFFSET,
        borderRadius: 5,
        stepPosition: DIRECTED_EDGE_LANES[lane],
      },
      data: {
        kind: 'edge',
        original: edge,
        pathType: 'smoothstep',
        deltaMs: edge.delta_ms,
        showTimingLabel: false,
        labelOffsetX: (lane - 1) * 10,
        labelOffsetY: (lane - 1) * 18,
      },
    })
  })

  if (showAssociations) {
    ;(component.associations || []).forEach((association, index) => {
      const source = String(association.node_a)
      const target = String(association.node_b)
      if (!validNodeIds.has(source) || !validNodeIds.has(target)) return
      associationEdges.push({
        id: `association:${index}:${source}:${target}`,
        source,
        target,
        type: 'timing',
        className: 'association-edge',
        style: { stroke: '#7f8da5', strokeWidth: 1.55, strokeDasharray: '7 6' },
        interactionWidth: 18,
        data: {
          kind: 'association',
          original: association,
          pathType: 'straight',
        },
      })
    })
  }

  // Associations first (earlier in DOM/paint order) so directed
  // propagation edges always render on top of them - see z-order note
  // above .association-edge in index.css.
  return [...associationEdges, ...directedEdges]
}

function CopyableValue({ label, value }) {
  const [copied, setCopied] = useState(false)
  if (!isPresent(value)) return null

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(String(value))
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1200)
    } catch {
      setCopied(false)
    }
  }

  return (
    <div className="detail-field detail-field-copyable">
      <span>{label}</span>
      <div>
        <code>{String(value)}</code>
        <button type="button" onClick={copy} aria-label={`Copy ${label}`}>{copied ? 'Copied' : 'Copy'}</button>
      </div>
    </div>
  )
}

function DetailField({ label, value, transform }) {
  if (!isPresent(value)) return null
  return (
    <div className="detail-field">
      <span>{label}</span>
      <strong>{transform ? transform(value) : String(value)}</strong>
    </div>
  )
}

function collectSourceMatches(node) {
  const candidates = [
    ...(Array.isArray(node.source_matches) ? node.source_matches : []),
    ...(node.evidence || []).flatMap((record) => Array.isArray(record.source_matches) ? record.source_matches : []),
  ]
  const seen = new Set()
  return candidates.filter((match) => {
    const key = `${match.relative_path}:${match.line_number}:${match.function}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function NodeDetails({ node, timelineEntry, rootCandidate }) {
  const sourceMatches = collectSourceMatches(node)
  const attributes = node.diagnostic_attributes && typeof node.diagnostic_attributes === 'object'
    ? Object.entries(node.diagnostic_attributes).filter(([, value]) => isPresent(value))
    : []

  return (
    <>
      <p className="detail-kicker">{humanize(node.role || 'uncorrelated')} node</p>
      <h3 id="graph-detail-heading">{nodeHeadline(node)}</h3>
      <div className="detail-grid">
        <DetailField label="Role" value={node.role} transform={humanize} />
        <DetailField label="Node ID" value={node.id} />
        <DetailField label="Artifact ID" value={node.artifact_id} />
        <DetailField label="Fingerprint" value={node.fingerprint} />
        <DetailField label="Service" value={node.service} />
        <DetailField label="Module" value={node.module} />
        <DetailField label="Severity" value={node.severity} />
        <DetailField label="Event type" value={node.event_type} transform={humanize} />
        <DetailField label="Host" value={node.host} />
        <DetailField label="Container" value={node.container} />
        <DetailField label="Pod" value={node.pod} />
        <DetailField label="Endpoint" value={node.endpoint} />
        <DetailField label="HTTP status" value={node.http_status} />
        <DetailField label="First seen" value={node.first_seen} transform={formatTimestamp} />
        <DetailField label="Last seen" value={node.last_seen} transform={formatTimestamp} />
        <DetailField label="Timeline timestamp" value={timelineEntry?.timestamp} transform={formatTimestamp} />
        <DetailField label="Relative time" value={timelineEntry?.relative_ms} transform={formatRelativeMs} />
        <DetailField label="Timeline service" value={timelineEntry?.service} />
        <DetailField label="Timeline role" value={timelineEntry?.role} transform={humanize} />
        <DetailField label="Occurrences" value={node.occurrence_count} />
        <DetailField label="Source artifact" value={node.source_file} />
        <DetailField label="Source format" value={node.source_format} transform={humanize} />
        <DetailField label="Identity match" value={node.identity_match_type} transform={humanize} />
        <DetailField label="Identity strength" value={node.identity_strength} />
        <DetailField
          label="Root score"
          value={node.role === 'root' || Number(node.root_cause_strength) > 0 ? node.root_cause_strength : null}
        />
      </div>


      {rootCandidate?.graph_stats && (
        <div className="detail-block">
          <h4>Root-cause graph statistics</h4>
          <div className="detail-grid">
            <DetailField label="Incoming edges" value={rootCandidate.graph_stats.incoming_count} />
            <DetailField label="Outgoing edges" value={rootCandidate.graph_stats.outgoing_count} />
            <DetailField label="Downstream nodes" value={rootCandidate.graph_stats.downstream_count} />
            <DetailField label="Incoming strength" value={rootCandidate.graph_stats.incoming_strength} />
            <DetailField label="Outgoing strength" value={rootCandidate.graph_stats.outgoing_strength} />
            <DetailField label="Ranked root strength" value={rootCandidate.root_cause_strength} />
          </div>
        </div>
      )}
      <div className="copyable-identifiers">
        <CopyableValue label="Trace ID" value={node.trace_id} />
        <CopyableValue label="Request ID" value={node.request_id} />
        <CopyableValue label="Span ID" value={node.span_id} />
        <CopyableValue label="Parent span ID" value={node.parent_span_id} />
        <CopyableValue label="Resolved identity" value={node.resolved_identity} />
      </div>

      {node.representative_line && (
        <div className="detail-block">
          <h4>Representative evidence</h4>
          <pre><code>{node.representative_line}</code></pre>
        </div>
      )}

      {sourceMatches.length > 0 && (
        <div className="detail-block">
          <h4>Source code matches</h4>
          {sourceMatches.map((match, index) => (
            <article className="detail-source-match" key={`${match.relative_path}:${match.line_number}:${index}`}>
              <div><code>{match.relative_path}</code>{isPresent(match.line_number) && <span>line {match.line_number}</span>}</div>
              {match.function && <p>Function: <code>{match.function}</code></p>}
              {match.confidence && <p>{humanize(match.confidence)} confidence</p>}
              {match.snippet && <pre><code>{match.snippet}</code></pre>}
            </article>
          ))}
        </div>
      )}

      {Array.isArray(node.evidence) && node.evidence.length > 0 && (
        <div className="detail-block">
          <h4>Evidence records</h4>
          {node.evidence.map((record, index) => (
            <article className="detail-evidence" key={record.id ?? index}>
              <div>
                <strong>{record.service || record.event_type || record.source_file || 'Evidence'}</strong>
                {record.severity && <span>{record.severity}</span>}
              </div>
              <div className="evidence-field-grid">
                <DetailField label="Evidence ID" value={record.id} />
                <DetailField label="Artifact ID" value={record.artifact_id} />
                <DetailField label="Event type" value={record.event_type} transform={humanize} />
                <DetailField label="Module" value={record.module} />
                <DetailField label="Host" value={record.host} />
                <DetailField label="Container" value={record.container} />
                <DetailField label="Pod" value={record.pod} />
                <DetailField label="Endpoint" value={record.endpoint} />
                <DetailField label="HTTP status" value={record.http_status} />
                <DetailField label="Source format" value={record.source_format} transform={humanize} />
                <DetailField label="Source file" value={record.source_file} />
                <DetailField label="First line" value={record.first_line_number} />
                <DetailField label="Last line" value={record.last_line_number} />
                <DetailField label="Occurrences" value={record.occurrence_count} />
                <DetailField label="First seen" value={record.first_seen} transform={formatTimestamp} />
                <DetailField label="Last seen" value={record.last_seen} transform={formatTimestamp} />
                <DetailField label="Identity match" value={record.identity_match_type} transform={humanize} />
                <DetailField label="Identity strength" value={record.identity_strength} />
                <DetailField label="OCR confidence" value={record.ocr_confidence} />
              </div>
              <div className="copyable-identifiers evidence-identifiers">
                <CopyableValue label="Trace ID" value={record.trace_id} />
                <CopyableValue label="Request ID" value={record.request_id} />
                <CopyableValue label="Span ID" value={record.span_id} />
                <CopyableValue label="Parent span ID" value={record.parent_span_id} />
                <CopyableValue label="Resolved identity" value={record.resolved_identity} />
              </div>
              {record.representative_line && <pre><code>{record.representative_line}</code></pre>}
              {record.diagnostic_attributes && Object.keys(record.diagnostic_attributes).length > 0 && (
                <pre><code>{JSON.stringify(record.diagnostic_attributes, null, 2)}</code></pre>
              )}
            </article>
          ))}
        </div>
      )}

      {attributes.length > 0 && (
        <div className="detail-block">
          <h4>Diagnostic attributes</h4>
          <dl className="attribute-list">
            {attributes.map(([key, value]) => (
              <div key={key}><dt>{humanize(key)}</dt><dd><code>{typeof value === 'string' ? value : JSON.stringify(value)}</code></dd></div>
            ))}
          </dl>
        </div>
      )}
    </>
  )
}

// "explicit_parent_child" (current) and legacy persisted "causal" (old
// analyses, never generated anymore - see buildFlowEdges) both name the
// same exact-parent-span relationship: proven DIRECTION, not proven
// physical causation, so both get this same truthful label rather than
// generic humanize() (which would otherwise render legacy data as bare
// "Causal").
function relationshipTypeLabel(relationshipType) {
  if (relationshipType === 'explicit_parent_child' || relationshipType === 'causal') {
    return 'Explicit parent-child'
  }
  return humanize(relationshipType)
}

function RelationshipDetails({ kind, relationship }) {
  const directed = kind === 'edge'
  return (
    <>
      <p className="detail-kicker">{directed ? 'Directed relationship' : 'Non-directional association'}</p>
      <h3 id="graph-detail-heading">
        {directed ? relationshipTypeLabel(relationship.relationship_type) : 'Associated evidence'}
      </h3>
      {!directed && <p className="detail-note">This relationship links evidence from the same incident without implying causation or direction.</p>}
      <div className="detail-grid">
        <DetailField label="Time delta" value={relationship.delta_ms} transform={formatRelativeMs} />
        <DetailField label="Correlation strength" value={relationship.correlation_strength} />
        {directed && <DetailField label="Direction confidence" value={relationship.direction_confidence} />}
      </div>
      {Array.isArray(relationship.signals) && relationship.signals.length > 0 && (
        <div className="detail-block">
          <h4>Signals</h4>
          <div className="signal-list">
            {relationship.signals.map((signal) => <span key={signal}>{humanize(signal)}</span>)}
          </div>
        </div>
      )}
    </>
  )
}

function DetailPanel({ selection, timelineById, rootCandidateById, onClose, panelRef }) {
  if (!selection) return null
  return (
    <aside
      ref={panelRef}
      className="graph-detail-panel"
      role="dialog"
      aria-modal="false"
      aria-labelledby="graph-detail-heading"
      tabIndex={-1}
      onKeyDown={(event) => {
        if (event.key === 'Escape') {
          event.preventDefault()
          onClose()
        }
      }}
    >
      <button type="button" className="detail-close" onClick={onClose} aria-label="Close details">×</button>
      {selection.kind === 'node' ? (
        <NodeDetails
          node={selection.value}
          timelineEntry={timelineById.get(String(selection.value.id))}
          rootCandidate={rootCandidateById.get(String(selection.value.id))}
        />
      ) : (
        <RelationshipDetails kind={selection.kind} relationship={selection.value} />
      )}
    </aside>
  )
}

export default function CorrelationGraph({ component }) {
  const [showAssociations, setShowAssociations] = useState(false)
  const [selection, setSelection] = useState(null)
  const [hoveredAssociationId, setHoveredAssociationId] = useState(null)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const detailPanelRef = useRef(null)
  const detailTriggerRef = useRef(null)
  const flowInstanceRef = useRef(null)
  const graphFrameRef = useRef(null)
  const graphFlowRef = useRef(null)
  const fullscreenCloseRef = useRef(null)
  const fullscreenTriggerRef = useRef(null)
  const initialNodes = useMemo(() => buildFlowNodes(component), [component])
  const [nodes, , onNodesChange] = useNodesState(initialNodes)
  const nodeIndex = useMemo(() => new Map(nodes.map((node, index) => [node.id, index])), [nodes])
  const baseEdges = useMemo(
    () => buildFlowEdges(component, showAssociations, nodeIndex),
    [component, nodeIndex, showAssociations],
  )
  const selectedNodeId = selection?.kind === 'node' ? String(selection.value.id) : null
  const activeAssociationId = hoveredAssociationId
    ?? (selection?.kind === 'association' ? selection.edgeId : null)
  const selectedDirectedEdgeId = selection?.kind === 'edge' ? selection.edgeId : null
  const hasDirectedFocus = selectedDirectedEdgeId != null || selectedNodeId != null

  // Focus is contextual instead of permanent clutter:
  // - node selected: connected directed edges brighten and expose delta_ms
  // - directed edge selected: that one edge exposes delta_ms
  // - association hover/select: that dashed association brightens
  const edges = useMemo(() => baseEdges.map((edge) => {
    if (edge.data?.kind === 'association') {
      if (!showAssociations || (!activeAssociationId && !selectedNodeId)) return edge
      const isActive = edge.id === activeAssociationId
      const touchesSelectedNode = selectedNodeId != null
        && (edge.source === selectedNodeId || edge.target === selectedNodeId)
      const focusClass = isActive || touchesSelectedNode
        ? 'association-active'
        : 'association-dim'
      return { ...edge, className: `${edge.className} ${focusClass}` }
    }

    if (edge.data?.kind !== 'edge' || !hasDirectedFocus) {
      return {
        ...edge,
        data: { ...edge.data, showTimingLabel: false },
      }
    }

    const touchesSelectedNode = selectedNodeId != null
      && (edge.source === selectedNodeId || edge.target === selectedNodeId)
    const isSelectedEdge = selectedDirectedEdgeId === edge.id
    const isFocused = isSelectedEdge || touchesSelectedNode

    return {
      ...edge,
      className: `${edge.className}${isFocused ? ' directed-focus' : ' directed-dim'}`,
      data: {
        ...edge.data,
        showTimingLabel: isFocused,
      },
    }
  }), [
    baseEdges,
    showAssociations,
    activeAssociationId,
    selectedNodeId,
    selectedDirectedEdgeId,
    hasDirectedFocus,
  ])

  const associationEndpointIds = useMemo(() => {
    if (!activeAssociationId) return null
    const activeEdge = baseEdges.find((edge) => edge.id === activeAssociationId)
    return activeEdge ? new Set([activeEdge.source, activeEdge.target]) : null
  }, [baseEdges, activeAssociationId])

  const displayNodes = useMemo(() => {
    if (!associationEndpointIds) return nodes
    return nodes.map((node) => (
      associationEndpointIds.has(node.id)
        ? { ...node, data: { ...node.data, isAssociationEndpoint: true } }
        : node
    ))
  }, [nodes, associationEndpointIds])

  // Drives the vertical scrollbar: kept in sync with every viewport change
  // (zoom, fit view, drag pan, the horizontal-trackpad handler below, and
  // the scrollbar's own drag) via React Flow's onMove callback, so the
  // scrollbar always reflects reality instead of tracking it separately.
  const [viewport, setViewport] = useState({ x: 0, y: 0, zoom: 1 })
  const [panePixelHeight, setPanePixelHeight] = useState(0)
  const contentBounds = useMemo(() => {
    if (nodes.length === 0) return null
    let top = Infinity
    let bottom = -Infinity
    nodes.forEach((node) => {
      top = Math.min(top, node.position.y)
      bottom = Math.max(bottom, node.position.y + NODE_HEIGHT_ESTIMATE)
    })
    return { top, bottom }
  }, [nodes])

  const scrollViewportTo = (nextY) => {
    const instance = flowInstanceRef.current
    if (!instance) return
    instance.setViewport({ ...instance.getViewport(), y: nextY })
  }

  const timelineById = useMemo(
    () => new Map((component.timeline || []).map((entry) => [String(entry.node_id), entry])),
    [component.timeline],
  )
  const timedEntries = useMemo(
    () => (component.timeline || []).filter((entry) => isPresent(entry.relative_ms)),
    [component.timeline],
  )
  const maxRelative = timedEntries.length > 0
    ? Math.max(...timedEntries.map((entry) => Number(entry.relative_ms)))
    : null
  const minRelative = timedEntries.length > 0
    ? Math.min(...timedEntries.map((entry) => Number(entry.relative_ms)))
    : null
  const associationCount = component.associations?.length || 0
  const rootCandidateById = useMemo(
    () => new Map((component.root_causes || []).map((candidate) => [String(candidate.node_id), candidate])),
    [component.root_causes],
  )

  useEffect(() => {
    if (selection) detailPanelRef.current?.focus()
  }, [selection])

  useEffect(() => {
    const node = graphFlowRef.current
    if (!node) return undefined

    // React Flow's built-in zoomOnScroll only reads deltaY, so a horizontal
    // trackpad swipe is otherwise inert. Intercept it in the capture phase
    // (before React Flow's own wheel listener sees it) and pan manually,
    // leaving vertical wheel/trackpad movement to the existing zoom handling.
    const handleWheel = (event) => {
      if (event.ctrlKey) return
      if (Math.abs(event.deltaX) <= Math.abs(event.deltaY)) return

      const instance = flowInstanceRef.current
      if (!instance) return

      event.preventDefault()
      event.stopPropagation()

      const deltaNormalize = event.deltaMode === 1 ? 20 : 1
      const panOnScrollSpeed = 0.5
      const viewport = instance.getViewport()
      instance.setViewport({
        ...viewport,
        x: viewport.x - event.deltaX * deltaNormalize * panOnScrollSpeed,
      })
    }

    node.addEventListener('wheel', handleWheel, { capture: true, passive: false })
    return () => node.removeEventListener('wheel', handleWheel, { capture: true })
  }, [])

  useEffect(() => {
    const wrapper = graphFlowRef.current
    if (!wrapper) return undefined

    // Measure the real React Flow pane (not the padded .graph-flow wrapper
    // around it) so the scrollbar's track/thumb size stays correct across
    // zoom, window resize, and the normal <-> fullscreen transition (the
    // pane's rendered height changes there even though the viewport
    // transform itself does not).
    const paneNode = wrapper.querySelector('.react-flow')
    if (!paneNode) return undefined

    const observer = new ResizeObserver(([entry]) => {
      setPanePixelHeight(entry.contentRect.height)
    })
    observer.observe(paneNode)
    return () => observer.disconnect()
  }, [])

  useLayoutEffect(() => {
    if (!isFullscreen) return undefined

    const frame = graphFrameRef.current
    const animatedAncestor = frame?.closest('.view-enter')
    const scrollContainer = frame?.closest('.workspace-main')
    const previousAncestorAnimation = animatedAncestor?.style.animation
    const previousAncestorTransform = animatedAncestor?.style.transform
    const previousBodyOverflow = document.body.style.overflow
    const previousScrollOverflow = scrollContainer?.style.overflow
    const previousOverscrollBehavior = scrollContainer?.style.overscrollBehavior
    document.body.style.overflow = 'hidden'
    if (animatedAncestor) {
      animatedAncestor.style.animation = 'none'
      animatedAncestor.style.transform = 'none'
    }
    if (scrollContainer) {
      scrollContainer.style.overflow = 'hidden'
      scrollContainer.style.overscrollBehavior = 'none'
    }

    const focusFrame = window.requestAnimationFrame(() => fullscreenCloseRef.current?.focus())
    const handleKeyDown = (event) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        event.stopPropagation()
        setIsFullscreen(false)
        return
      }
      if (event.key !== 'Tab' || !frame) return

      const focusable = [...frame.querySelectorAll(FOCUSABLE_SELECTOR)]
      if (focusable.length === 0) {
        event.preventDefault()
        frame.focus()
        return
      }

      const first = focusable[0]
      const last = focusable[focusable.length - 1]
      if (event.shiftKey && (document.activeElement === first || !frame.contains(document.activeElement))) {
        event.preventDefault()
        last.focus()
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault()
        first.focus()
      }
    }
    document.addEventListener('keydown', handleKeyDown, true)

    return () => {
      window.cancelAnimationFrame(focusFrame)
      document.removeEventListener('keydown', handleKeyDown, true)
      document.body.style.overflow = previousBodyOverflow
      if (animatedAncestor) {
        // The entrance transition has already completed. Keeping animation
        // disabled avoids replaying it when the fullscreen overlay closes.
        animatedAncestor.style.animation = previousAncestorAnimation || 'none'
        animatedAncestor.style.transform = previousAncestorTransform || ''
      }
      if (scrollContainer) {
        scrollContainer.style.overflow = previousScrollOverflow || ''
        scrollContainer.style.overscrollBehavior = previousOverscrollBehavior || ''
      }
      const trigger = fullscreenTriggerRef.current
      if (trigger?.isConnected) window.requestAnimationFrame(() => trigger.focus())
    }
  }, [isFullscreen])

  const selectDetail = (event, nextSelection) => {
    detailTriggerRef.current = typeof event.currentTarget?.focus === 'function'
      ? event.currentTarget
      : null
    setSelection(nextSelection)
  }

  const closeDetail = () => {
    const trigger = detailTriggerRef.current
    setSelection(null)
    detailTriggerRef.current = null
    window.requestAnimationFrame(() => {
      if (trigger?.isConnected) trigger.focus()
    })
  }

  const fitGraph = () => {
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    flowInstanceRef.current?.fitView({ padding: 0.2, duration: reducedMotion ? 0 : 360 })
  }

  const toggleFullscreen = (event) => {
    if (!isFullscreen) fullscreenTriggerRef.current = event.currentTarget
    setIsFullscreen((value) => !value)
  }

  if (nodes.length === 0) {
    return (
      <section className="empty-graph result-panel">
        <h2>Primary incident graph unavailable</h2>
        <p>This stored result does not include a primary component to visualize.</p>
      </section>
    )
  }

  return (
    <section className="correlation-section result-section" aria-labelledby="correlation-heading">
      <div className="section-heading-row graph-heading-row">
        <div>
          <p className="eyebrow">Deterministic correlation</p>
          <h2 id="correlation-heading">Primary incident flow</h2>
        </div>
        <AssociationToggle
          count={associationCount}
          active={showAssociations}
          onToggle={() => setShowAssociations((value) => !value)}
        />
      </div>

      <div
        ref={graphFrameRef}
        className={`graph-frame graph-canvas-reveal${isFullscreen ? ' graph-fullscreen' : ''}`}
        role={isFullscreen ? 'dialog' : undefined}
        aria-modal={isFullscreen ? 'true' : undefined}
        aria-label={isFullscreen ? 'Fullscreen primary incident graph' : undefined}
        tabIndex={isFullscreen ? -1 : undefined}
      >
        {isFullscreen && (
          <div className="graph-fullscreen-bar">
            <strong>Primary incident flow</strong>
            <AssociationToggle
              count={associationCount}
              active={showAssociations}
              onToggle={() => setShowAssociations((value) => !value)}
            />
            <button
              ref={fullscreenCloseRef}
              type="button"
              className="graph-fullscreen-close"
              onClick={() => setIsFullscreen(false)}
              aria-label="Close fullscreen graph"
              title="Close fullscreen graph"
            >
              ×
            </button>
          </div>
        )}
        <div className="graph-axis" aria-hidden="true">
          <div className="graph-lanes"><span>Earlier / upstream</span><span>Directed incident flow</span><span>Later / downstream</span></div>
          {maxRelative !== null && (
            <div className="graph-time-range">
              <span>{formatRelativeMs(minRelative)}</span>
              {maxRelative > minRelative && <span>{formatRelativeMs(maxRelative)}</span>}
            </div>
          )}
        </div>
        <div className="graph-flow" ref={graphFlowRef}>
          <ReactFlow
            nodes={displayNodes}
            edges={edges}
            nodeTypes={NODE_TYPES}
            edgeTypes={EDGE_TYPES}
            fitView
            fitViewOptions={{ padding: 0.2, duration: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 360 }}
            minZoom={0.22}
            maxZoom={1.6}
            nodesConnectable={false}
            nodesFocusable
            edgesFocusable
            zoomOnScroll
            zoomOnPinch
            panOnDrag
            onNodesChange={onNodesChange}
            onInit={(instance) => {
              flowInstanceRef.current = instance
              setViewport(instance.getViewport())
            }}
            onMove={(event, nextViewport) => setViewport(nextViewport)}
            onNodeClick={(event, node) => selectDetail(event, { kind: 'node', value: node.data.node })}
            onEdgeClick={(event, edge) => selectDetail(event, { kind: edge.data.kind, value: edge.data.original, edgeId: edge.id })}
            onEdgeMouseEnter={(event, edge) => {
              if (edge.data?.kind === 'association') setHoveredAssociationId(edge.id)
            }}
            onEdgeMouseLeave={(event, edge) => {
              if (edge.data?.kind !== 'association') return
              setHoveredAssociationId((current) => (current === edge.id ? null : current))
            }}
            onPaneClick={() => {
              detailTriggerRef.current = null
              setSelection(null)
            }}
            proOptions={{ hideAttribution: true }}
          >
            <Background variant={BackgroundVariant.Dots} gap={28} size={1} color="#28313d" />
            <Controls showFitView={false} showInteractive={false} position="bottom-left">
              <ControlButton
                className="graph-control-button"
                onClick={fitGraph}
                aria-label="Fit graph to view"
                title="Fit graph to view"
              >
                <FitViewIcon />
              </ControlButton>
              <ControlButton
                className="graph-control-button"
                onClick={toggleFullscreen}
                aria-label={isFullscreen ? 'Exit fullscreen graph' : 'Open fullscreen graph'}
                title={isFullscreen ? 'Exit fullscreen graph' : 'Open fullscreen graph'}
              >
                <FullscreenIcon active={isFullscreen} />
              </ControlButton>
            </Controls>
          </ReactFlow>
        </div>
        <GraphVScrollbar
          viewport={viewport}
          paneHeight={panePixelHeight}
          contentBounds={contentBounds}
          onScrollTo={scrollViewportTo}
        />
        <DetailPanel
          selection={selection}
          timelineById={timelineById}
          rootCandidateById={rootCandidateById}
          onClose={closeDetail}
          panelRef={detailPanelRef}
        />
      </div>
      <p className="graph-help">Pan, zoom or select a node or relationship for exact backend details. Dragging only adjusts the view; backend relationships do not change.</p>
    </section>
  )
}
