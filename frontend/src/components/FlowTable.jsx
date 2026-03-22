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

function nodeKey(path) {
  return path.join('||')
}

export default function FlowTable({
  rootRows,
  cohorts,
  cohortMap,
  expandedNodes,
  loadingNodes,
  getChildren,
  onToggle,
  maxDepth,
}) {
  const renderValueCell = (row, cid) => {
    const val = row.values[cid]
    if (!val) return <td key={cid}>—</td>

    const continuePct = val.continue_pct ?? 0
    const dropoffPct = val.dropoff_pct ?? 1
    const minSeg = 3
    const cWidth = Math.max(minSeg, continuePct * 100)
    const dWidth = Math.max(minSeg, dropoffPct * 100)
    const norm = cWidth + dWidth

    const parentUsers = val.pct > 0 ? Math.round(val.count / val.pct) : 0
    const usersContinuing = Math.round(parentUsers * continuePct)
    const usersDropped = Math.max(0, parentUsers - usersContinuing)

    return (
      <td key={cid} title={`Continue ${formatPct(continuePct)} | Drop-off ${formatPct(dropoffPct)} | Users continuing ${usersContinuing} | Users dropped ${usersDropped} | Median ${formatTime(val.median_time_sec)} | P90 ${formatTime(val.p90_time_sec)}`}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontWeight: 700 }}>{formatPct(val.pct)}</div>
          <div style={{ fontSize: 12, color: '#6b7280' }}>↓{formatPct(dropoffPct)} · {formatTime(val.median_time_sec)} / {formatTime(val.p90_time_sec)}</div>
          <div style={{ width: '100%', height: 8, borderRadius: 4, overflow: 'hidden', background: '#e5e7eb', display: 'flex' }}>
            <div style={{ width: `${(cWidth / norm) * 100}%`, background: '#2563eb' }} />
            <div style={{ width: `${(dWidth / norm) * 100}%`, background: '#d1d5db' }} />
          </div>
        </div>
      </td>
    )
  }

  const renderRows = (rows) => rows.flatMap(row => {
    const key = nodeKey(row.path)
    const depth = row.path.length - 1
    const expanded = expandedNodes.has(key)
    const canExpand = row.path[row.path.length - 1] !== 'Other' && depth < maxDepth
    const children = getChildren(row.path)

    const own = (
      <tr key={key} data-testid={`flow-row-d${depth}`}>
        <td className="sticky-col flow-path-col" onClick={() => canExpand && onToggle(row.path)} style={{ cursor: canExpand ? 'pointer' : 'default' }}>
          <div style={{ paddingLeft: (depth - 1) * 18, display: 'flex', gap: 8, alignItems: 'center' }}>
            {canExpand && <span>{expanded ? '▼' : '▶'}</span>}
            <div>
              <div style={{ fontWeight: 600 }}>{row.path[row.path.length - 1]}</div>
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
      return [
        own,
        <tr key={`${key}-empty`}>
          <td className="sticky-col flow-path-col" style={{ paddingLeft: depth * 18, color: '#6b7280' }}>No transitions found</td>
          {cohorts.map(cid => <td key={cid} style={{ color: '#6b7280' }}>↓100%</td>)}
        </tr>,
      ]
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
            {cohorts.map(cid => (
              <th key={cid}>
                <div>{cohortMap[cid]?.name || `Cohort ${cid}`}</div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{renderRows(rootRows)}</tbody>
      </table>
    </div>
  )
}

export { formatTime, formatPct, nodeKey }
