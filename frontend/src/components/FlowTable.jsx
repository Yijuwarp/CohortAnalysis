function formatPct(value) {
  return `${((value || 0) * 100).toFixed(1)}%`
}

function formatTime(sec) {
  if (sec === null || sec === undefined) return '—'
  const total = Math.max(0, Math.round(sec))
  if (total < 60) return `${total}s`
  if (total < 3600) return `${Math.floor(total / 60)}m ${total % 60}s`
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  return `${h}h ${m}m`
}

function compactNumber(n) {
  if (n < 1000) return n.toLocaleString()
  return new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1
  }).format(n).toLowerCase()
}

function computePct(userCount, parentUsers) {
  if (!parentUsers || parentUsers <= 0) return 0
  return userCount / parentUsers
}

function nodeKey(path) {
  return path.join('>')
}

export default function FlowTable({
  rootRows,
  cohorts,
  cohortMap,
  expandedNodes,
  loadingNodes,
  getChildren,
  onToggle,
  onExpandOther,
  nodeExpansion,
  maxDepth,
}) {
  // No further action is now backend-driven

  const renderValueCell = (row, cid) => {
    const val = row.values[cid]
    if (!val) return <td key={cid}>—</td>
    if (val.has_event === false) {
      return <td key={cid}>—</td>
    }
    const pct = computePct(val.user_count, val.parent_users)

    return (
      <td key={cid} title={`${val.user_count?.toLocaleString() || 0} users | P20 ${formatTime(val.p20_time_sec)} | Median ${formatTime(val.median_time_sec)} | P80 ${formatTime(val.p80_time_sec)}`}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontWeight: 700 }}>{compactNumber(val.user_count)} ({formatPct(pct)})</div>
          <div style={{ fontSize: 12, color: '#6b7280' }}>{formatTime(val.median_time_sec)}</div>
        </div>
      </td>
    )
  }

  const renderRows = (rows) => rows.flatMap(row => {
    const key = nodeKey(row.path)
    const depth = row.path.length - 1
    const indentPx = depth * 16
    const expanded = expandedNodes.has(key)
    const eventName = row.path[row.path.length - 1]
    const isOtherRow = eventName === '__OTHER__'
    const isNoFurtherAction = eventName === 'No further action'
    const isLoopRow = row.path.slice(0, -1).includes(eventName)
    const canExpand = !isNoFurtherAction && !isOtherRow && depth < maxDepth && !isLoopRow
    const children = getChildren(row.path)

    const own = (
      <tr key={key} data-testid={`flow-row-d${depth}`}>
        <td className="sticky-col flow-path-col" onClick={() => canExpand && onToggle(row.path)} style={{ cursor: canExpand ? 'pointer' : 'default' }}>
          <div style={{ paddingLeft: `${indentPx}px`, display: 'flex', gap: 8, alignItems: 'center' }}>
            {canExpand && <span>{expanded ? '▼' : '▶'}</span>}
            <div>
              <div 
                style={{ 
                  fontWeight: 600, 
                  color: isNoFurtherAction ? '#9ca3af' : undefined, 
                  fontStyle: isNoFurtherAction ? 'italic' : 'normal' 
                }}
              >
                {isOtherRow ? 'Other' : eventName}
              </div>
              {isOtherRow && (
                <div style={{ paddingLeft: 0, marginTop: 4 }}>
                  <button 
                    className="button-link" 
                    onClick={(e) => {
                      e.stopPropagation();
                      onExpandOther(row.path.slice(0, -1));
                    }}
                    disabled={!cohorts.some(cid => (row.values?.[cid]?.user_count || 0) > 0)}
                  >
                    ▼ Show more ({(row.meta?.total_event_types || 0)} total)
                  </button>
                </div>
              )}
            </div>
          </div>
        </td>
        {cohorts.map(cid => renderValueCell(row, cid))}
      </tr>
    )

    if (!expanded) return [own]

    if (loadingNodes[key]) {
      return [own, <tr key={`${key}-loading`}><td className="sticky-col flow-path-col" style={{ paddingLeft: depth * 18 }}>Loading...</td>{cohorts.map(cid => <td key={cid}></td>)}</tr>]
    }

    if (!children || children.length === 0) {
      return [own]
    }

    return [own, ...renderRows(children)]
  })

  if (!rootRows || rootRows.length === 0) return <p style={{ marginTop: 16 }}>No transitions found</p>

  return (
    <div className="analytics-table table-responsive" style={{ marginTop: 16 }}>
      <table>
        <thead>
          <tr>
            <th className="sticky-col flow-path-col">Event</th>
            {cohorts.map(cid => {
              const totalUsers = rootRows?.[0]?.values?.[cid]?.parent_users || 0
              return (
                <th key={cid}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                    <div>{cohortMap[cid]?.name || `Cohort ${cid}`}</div>
                    <div style={{ fontSize: 11, fontWeight: 400, opacity: 0.8 }}>
                      ({compactNumber(totalUsers)} users)
                    </div>
                  </div>
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody>{renderRows(rootRows)}</tbody>
      </table>
    </div>
  )
}

export { formatTime, formatPct, nodeKey, computePct }
