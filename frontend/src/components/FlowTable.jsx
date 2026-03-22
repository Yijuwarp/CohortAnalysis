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
  maxDepth,
}) {
  const buildNoFurtherActionRow = (parentRow, children) => {
    const values = {}
    let hasAny = false

    cohorts.forEach((cid) => {
      const parentVal = parentRow.values?.[cid]
      const parentUsers = parentVal?.parent_users || 0
      const namedChildren = (children || []).filter((row) => row.path[row.path.length - 1] !== 'Other')
      const otherRow = (children || []).find((row) => row.path[row.path.length - 1] === 'Other')
      const childUsers = namedChildren.reduce((sum, row) => sum + (row.values?.[cid]?.user_count || 0), 0)
      const otherUsers = otherRow?.values?.[cid]?.user_count || 0
      const noFurtherActionUsers = Math.max(0, parentUsers - childUsers - otherUsers)
      const show = parentUsers > 0

      if (process.env.NODE_ENV === 'development') {
        const total = childUsers + otherUsers + noFurtherActionUsers
        if (Math.abs(total - parentUsers) > 1) {
          console.error('Flow math broken', {
            parentUsers,
            childUsers,
            otherUsers,
            noFurtherActionUsers,
            total,
            path: parentRow.path,
            cohort: cid,
          })
        }
      }

      if (show) {
        hasAny = true
        values[cid] = {
          user_count: noFurtherActionUsers,
          parent_users: parentUsers,
          median_time_sec: null,
          p20_time_sec: null,
          p80_time_sec: null,
        }
      }
    })

    if (!hasAny) return null
    return {
      path: [...parentRow.path, 'No further action'],
      values,
      isNoFurtherAction: true,
    }
  }

  const renderValueCell = (row, cid) => {
    const val = row.values[cid]
    if (!val) return <td key={cid}>—</td>
    const pct = computePct(val.user_count, val.parent_users)

    return (
      <td key={cid} title={`${val.user_count?.toLocaleString() || 0} users | Median ${formatTime(val.median_time_sec)} | P20 ${formatTime(val.p20_time_sec)} | P80 ${formatTime(val.p80_time_sec)}`}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontWeight: 700 }}>{formatPct(pct)}</div>
          <div style={{ fontSize: 12, color: '#6b7280' }}>{formatTime(val.p20_time_sec)} / {formatTime(val.p80_time_sec)}</div>
        </div>
      </td>
    )
  }

  const renderRows = (rows) => rows.flatMap(row => {
    const key = nodeKey(row.path)
    const depth = row.path.length - 1
    const indentPx = depth * 16
    const expanded = expandedNodes.has(key)
    const canExpand = !row.isNoFurtherAction && depth < maxDepth && row.path[row.path.length - 1] !== 'Other'
    const children = getChildren(row.path)
    const basedOnUsers = Math.round(Number(row.values?.[cohorts[0]]?.parent_users || 0))

    const own = (
      <tr key={key} data-testid={`flow-row-d${depth}`}>
        <td className="sticky-col flow-path-col" onClick={() => canExpand && onToggle(row.path)} style={{ cursor: canExpand ? 'pointer' : 'default' }}>
          <div style={{ paddingLeft: `${indentPx}px`, display: 'flex', gap: 8, alignItems: 'center' }}>
            {canExpand && <span>{expanded ? '▼' : '▶'}</span>}
            <div>
              <div style={{ fontWeight: 600, color: row.isNoFurtherAction ? '#9ca3af' : undefined, fontStyle: row.isNoFurtherAction ? 'italic' : 'normal' }}>{row.path[row.path.length - 1]}</div>
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

    const noFurtherActionRow = buildNoFurtherActionRow(row, children || [])
    const contextRow = (
      <tr key={`${key}-context`}>
        <td className="sticky-col flow-path-col">
          <div style={{ paddingLeft: `${indentPx + 16}px` }} className="flow-subtle-label">
            Based on {basedOnUsers.toLocaleString()} users
          </div>
        </td>
        {cohorts.map(cid => <td key={cid}></td>)}
      </tr>
    )

    if (!children || children.length === 0) {
      return [
        own,
        contextRow,
        ...(noFurtherActionRow ? renderRows([noFurtherActionRow]) : []),
      ]
    }

    return [own, contextRow, ...renderRows(children), ...(noFurtherActionRow ? renderRows([noFurtherActionRow]) : [])]
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

export { formatTime, formatPct, nodeKey, computePct }
