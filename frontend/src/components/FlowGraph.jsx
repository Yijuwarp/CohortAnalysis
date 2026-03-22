import { nodeKey, formatPct, formatTime } from './FlowTable'

export default function FlowGraph({ rootRows, cohorts, expandedNodes, loadingNodes, getChildren, onToggle, maxDepth }) {
  const primaryCohort = cohorts[0]

  const renderNode = (row) => {
    const key = nodeKey(row.path)
    const depth = row.path.length - 1
    const val = row.values[primaryCohort] || {}
    const expanded = expandedNodes.has(key)
    const canExpand = row.path[row.path.length - 1] !== 'Other' && depth < maxDepth
    const children = getChildren(row.path) || []

    return (
      <div key={key} style={{ marginLeft: (depth - 1) * 26, marginTop: 10 }}>
        <div
          style={{ border: '1px solid #d1d5db', borderRadius: 8, padding: 10, minWidth: 180, display: 'inline-block', cursor: canExpand ? 'pointer' : 'default' }}
          onClick={() => canExpand && onToggle(row.path)}
          title={`Continue ${formatPct(val.continue_pct || 0)} | Drop-off ${formatPct(val.dropoff_pct ?? 1)} | Median ${formatTime(val.median_time_sec)} | P90 ${formatTime(val.p90_time_sec)}`}
        >
          <div style={{ fontWeight: 700 }}>{row.path[row.path.length - 1]}</div>
          <div style={{ fontSize: 12 }}>{formatPct(val.pct || 0)} · ↓{formatPct(val.dropoff_pct ?? 1)}</div>
          <div style={{ width: '100%', height: 6, background: '#e5e7eb', borderRadius: 3, overflow: 'hidden', marginTop: 6 }}>
            <div style={{ width: `${Math.max(4, (val.continue_pct || 0) * 100)}%`, height: '100%', background: '#2563eb' }} />
          </div>
          {canExpand && <div style={{ fontSize: 12, marginTop: 4 }}>{expanded ? 'Collapse' : 'Expand'}</div>}
        </div>
        {expanded && loadingNodes[key] && <div style={{ marginLeft: 12, color: '#6b7280' }}>Loading...</div>}
        {expanded && !loadingNodes[key] && children.length === 0 && <div style={{ marginLeft: 12, color: '#6b7280' }}>No transitions found</div>}
        {expanded && children.map(child => renderNode(child))}
      </div>
    )
  }

  if (!rootRows || rootRows.length === 0) return <p style={{ marginTop: 16 }}>No transitions found</p>

  return <div style={{ overflowX: 'auto', marginTop: 16 }}>{rootRows.map(renderNode)}</div>
}
