import { useEffect, useMemo, useState, useRef } from 'react'
import { getAvailabilityStyle } from '../utils/style_utils'
import { getRetention, listEvents } from '../api'
import { formatSplitValue } from '../utils/formatters'
import SearchableSelect from './SearchableSelect'
import RetentionGraph from './RetentionGraph'
import ComparePane from './ComparePane'

function formatNumber(n) {
  return new Intl.NumberFormat().format(n);
}

export default function RetentionTable({
  refreshToken,
  retentionEvent,
  onRetentionEventChange,
  maxDay,
  setMaxDay,
  showGlobalControls = true,
  state,
  setState,
  cohorts = [],
}) {
  const [isPinned, setIsPinned] = useState(state?.isPinned ?? true)
  const [events, setEvents] = useState([])
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [includeCI, setIncludeCI] = useState(state?.includeCI ?? false)
  const [confidence, setConfidence] = useState(state?.confidence ?? 0.95)
  const [viewMode, setViewMode] = useState(state?.viewMode || 'table')
  const [isComparePaneOpen, setIsComparePaneOpen] = useState(state?.isComparePaneOpen ?? false)
  const [mode, setMode] = useState(state?.mode || "day") // "day" | "hour"
  const [retentionType, setRetentionType] = useState(state?.retentionType || "classic") // "classic" | "ever_after"
  const [columnWidths, setColumnWidths] = useState(state?.columnWidths || {
    cohort: 160,
    split: 160,
    size: 80
  })
  const isResizingRef = useRef(false)

  const getStickyLeft = (colKey) => {
    if (colKey === "cohort") return 0
    if (colKey === "split") return columnWidths.cohort
    if (colKey === "size") {
      return columnWidths.cohort + (showSplit ? columnWidths.split : 0)
    }
    return 0
  }

  const startResize = (colKey, e) => {
    e.preventDefault()
    e.stopPropagation()

    isResizingRef.current = true

    const startX = e.clientX
    const startWidth = columnWidths[colKey] || (colKey === 'size' ? 80 : 160)

    const onMouseMove = (moveEvent) => {
      const minWidth = colKey === "size" ? 80 : 120
      const newWidth = Math.max(minWidth, startWidth + (moveEvent.clientX - startX))
      setColumnWidths(prev => ({ ...prev, [colKey]: newWidth }))
    }

    const onMouseUp = () => {
      window.removeEventListener("mousemove", onMouseMove)
      window.removeEventListener("mouseup", onMouseUp)
      document.body.classList.remove("resizing")

      setTimeout(() => {
        isResizingRef.current = false
      }, 0)
    }

    window.addEventListener("mousemove", onMouseMove)
    window.addEventListener("mouseup", onMouseUp)
    document.body.classList.add("resizing")
  }

  useEffect(() => {
    const nextState = {
      isPinned,
      includeCI,
      confidence,
      viewMode,
      isComparePaneOpen,
      mode,
      retentionType,
      columnWidths
    }
    setState(nextState)
  }, [isPinned, includeCI, confidence, viewMode, isComparePaneOpen, mode, retentionType, columnWidths])

  const loadRetention = async () => {
    setLoading(true)
    setError('')

    try {
      const response = await getRetention(Number(maxDay), retentionEvent, includeCI, confidence, retentionType, mode)
      setData(response.retention_table || [])
    } catch (err) {
      setError(err.message)
      setData([])
    } finally {
      setLoading(false)
    }
  }

  const loadEvents = async () => {
    try {
      const response = await listEvents()
      setEvents(response.events || [])
    } catch {
      setEvents([])
    }
  }

  useEffect(() => {
    loadEvents()
  }, [])

  useEffect(() => {
    loadRetention()
  }, [refreshToken, retentionEvent, maxDay, includeCI, confidence, mode, retentionType])

  const labelPrefix = mode === "hour" ? "H" : "D"
  const totalBuckets = mode === "hour" ? (maxDay * 24) : (Number(maxDay) + 1)
  const bucketColumns = Array.from({ length: totalBuckets }, (_, index) => index)
  const [sortConfig, setSortConfig] = useState({ key: 'size', direction: 'desc' })

  // 1. Build Metadata Lookup
  const cohortMetaMap = useMemo(() => {
    const map = {}
    ;(cohorts || []).forEach(c => {
      map[c.cohort_id] = c
    })
    return map
  }, [cohorts])

  const getSplitLabel = (row) => {
    const cohort = cohortMetaMap[row.cohort_id]
    if (!cohort?.split_type) return "NA"

    if (cohort.split_type === "random") {
      return `Group ${cohort.split_value}`
    }

    if (cohort.split_type === "property") {
      if (cohort.split_value === "__OTHER__") return "Other"
      return `${cohort.split_property} = ${formatSplitValue(cohort.split_value)}`
    }

    return "NA"
  }

  const getDisplayName = (row) => {
    const cohort = cohortMetaMap[row.cohort_id]
    if (cohort?.split_parent_cohort_id) {
      const parent = cohortMetaMap[cohort.split_parent_cohort_id]
      return parent?.cohort_name || parent?.name || row.cohort_name
    }
    return row.cohort_name
  }

  const getValue = (row, key) => {
    if (key === 'cohort_name') return row.cohort_name
    if (key === 'split') return getSplitLabel(row)
    if (key === 'size') return row.size || 0
    if (key.startsWith('D') || key.startsWith('H')) {
      const day = key.slice(1)
      return row.retention?.[day] ?? 0
    }
    return 0
  }

  const handleSort = (key) => {
    setSortConfig((prev) => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc',
    }))
  }

  const showSplit = useMemo(() => {
    return (cohorts || []).some(c => c.split_type != null)
  }, [cohorts])

  const sortedData = useMemo(() => {
    const rows = [...(data || [])]
    return rows.sort((a, b) => {
      const valA = getValue(a, sortConfig.key)
      const valB = getValue(b, sortConfig.key)

      if (sortConfig.key === 'split') {
        if (valA === "Other") return 1
        if (valB === "Other") return -1
        if (valA === "NA" && valB !== "NA") return 1
        if (valB === "NA" && valA !== "NA") return -1
      }

      const order = sortConfig.direction === 'asc' ? 1 : -1
      if (typeof valA === 'string') {
        return valA.localeCompare(valB) * order
      }
      if (valA < valB) return -1 * order
      if (valA > valB) return 1 * order
      return 0
    })
  }, [data, sortConfig, cohorts])

  return (
    <section className="card">
      <div className="retention-header">
        <h2>Retention</h2>

        <div className="retention-controls">
          <button
            type="button"
            className={`compare-open-button ${isComparePaneOpen ? 'active' : ''}`}
            onClick={() => setIsComparePaneOpen(prev => !prev)}
            title="Compare two cohorts statistically"
          >
            🔬 Compare
          </button>

          <div className="ci-control">
            <label>CI</label>
            <input
              type="checkbox"
              checked={includeCI}
              onChange={(e) => setIncludeCI(e.target.checked)}
            />
            {includeCI && (
              <select
                value={confidence}
                onChange={(e) => setConfidence(Number(e.target.value))}
              >
                <option value={0.9}>90%</option>
                <option value={0.95}>95%</option>
                <option value={0.99}>99%</option>
              </select>
            )}
          </div>

          <div className="granularity-toggle">
            <button
              className={mode === "day" ? "active" : ""}
              onClick={() => setMode("day")}
            >
              Day
            </button>
            <button
              className={mode === "hour" ? "active" : ""}
              onClick={() => setMode("hour")}
            >
            Hour
            </button>
          </div>

          <div className="retention-type-selector">
              <select
                value={retentionType}
                onChange={(e) => setRetentionType(e.target.value)}
                title={retentionType === 'classic' ? 'Classic: Users active on this period' : 'Ever-After: Users who will return at any point after this period'}
              >
                <option value="classic">Classic Retention</option>
                <option value="ever_after">Ever-After Retention</option>
              </select>
          </div>

          <div className="view-toggle">
            <button
              type="button"
              className={`view-button ${isPinned ? 'active' : ''}`}
              onClick={() => setIsPinned((prev) => !prev)}
              title="Pin Cohort Columns"
            >
              {isPinned ? "📌" : "📍"}
            </button>
            <button
              type="button"
              className={`view-button ${viewMode === 'table' ? 'active' : ''}`}
              onClick={() => setViewMode('table')}
            >
              Table
            </button>
            <button
              type="button"
              className={`view-button ${viewMode === 'graph' ? 'active' : ''}`}
              onClick={() => setViewMode('graph')}
            >
              Graph
            </button>
          </div>
        </div>
      </div>

      {showGlobalControls && (
        <div className="retention-sub-controls">
          <label>
            Max Day
            <input
              type="number"
              min="0"
              value={maxDay}
              onChange={(e) => setMaxDay(Number(e.target.value))}
            />
          </label>
          <label>
            Retention Event
            <SearchableSelect
              options={[{ label: 'Any Event', value: 'any' }, ...events]}
              value={retentionEvent}
              onChange={onRetentionEventChange}
              placeholder="Select retention event"
            />
          </label>
        </div>
      )}

      {error && <p className="error">{error}</p>}

      {loading ? (
        <div className="loader">Loading {mode === "hour" ? "hourly" : "daily"} retention...</div>
      ) : (
        <>
          {viewMode === 'table' && sortedData.length > 0 && (
            <div className={`analytics-table table-responsive ${showSplit ? 'has-split' : ''}`}>
              <table>
                <thead>
                  <tr>
                    <th
                      className={`${isPinned ? 'sticky-col sticky-col-cohort' : ''} sortable-header`}
                      style={{ 
                        width: columnWidths.cohort,
                        minWidth: columnWidths.cohort,
                        maxWidth: columnWidths.cohort,
                        left: isPinned ? getStickyLeft("cohort") : undefined
                      }}
                      onClick={() => {
                        if (isResizingRef.current) return
                        handleSort('cohort_name')
                      }}
                    >
                      Cohort {sortConfig.key === 'cohort_name' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                      <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('cohort', e); }} />
                    </th>
                    {showSplit && (
                      <th
                        className={`${isPinned ? 'sticky-col sticky-col-split' : ''} sortable-header`}
                        style={{ 
                          width: columnWidths.split, 
                          minWidth: columnWidths.split,
                          maxWidth: columnWidths.split,
                          left: isPinned ? getStickyLeft("split") : undefined 
                        }}
                        onClick={() => {
                          if (isResizingRef.current) return
                          handleSort('split')
                        }}
                      >
                        Split {sortConfig.key === 'split' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                        <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('split', e); }} />
                      </th>
                    )}
                    <th
                      className={`${isPinned ? 'sticky-col sticky-col-size' : ''} sortable-header`}
                      style={{ 
                        width: columnWidths.size, 
                        minWidth: columnWidths.size,
                        maxWidth: columnWidths.size,
                        left: isPinned ? getStickyLeft("size") : undefined 
                      }}
                      onClick={() => {
                        if (isResizingRef.current) return
                        handleSort('size')
                      }}
                    >
                      Size {sortConfig.key === 'size' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                      <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('size', e); }} />
                    </th>
                    {bucketColumns.map((b) => (
                      <th
                        key={b}
                        className="sortable-header"
                        onClick={() => {
                          if (isResizingRef.current) return
                          handleSort(`${labelPrefix}${b}`)
                        }}
                      >
                        {labelPrefix}{b} {sortConfig.key === `${labelPrefix}${b}` && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sortedData.map((row) => (
                    <tr key={row.cohort_id}>
                      <td 
                        className={`${isPinned ? 'sticky-col sticky-col-cohort' : ''} cohort-name-cell`} 
                        style={{ 
                          width: columnWidths.cohort,
                          minWidth: columnWidths.cohort,
                          maxWidth: columnWidths.cohort,
                          left: isPinned ? getStickyLeft("cohort") : undefined
                        }}
                        title={row.cohort_name}
                      >
                        {getDisplayName(row)}
                      </td>
                      {showSplit && (
                        <td 
                          className={`${isPinned ? 'sticky-col sticky-col-split' : ''} tabular-cell`}
                          style={{ 
                            width: columnWidths.split, 
                            minWidth: columnWidths.split,
                            maxWidth: columnWidths.split,
                            left: isPinned ? getStickyLeft("split") : undefined 
                          }}
                        >
                          {getSplitLabel(row)}
                        </td>
                      )}
                      <td 
                        className={isPinned ? 'sticky-col sticky-col-size' : ''}
                        style={{ 
                          width: columnWidths.size, 
                          minWidth: columnWidths.size,
                          maxWidth: columnWidths.size,
                          left: isPinned ? getStickyLeft("size") : undefined 
                        }}
                      >
                        {formatNumber(row.size)}
                      </td>
                      {bucketColumns.map((b) => {
                        const rawValue = row.retention[String(b)]
                        const hasValue = rawValue !== null && rawValue !== undefined
                        const value = hasValue ? Number(rawValue) : null
                        const ci = row.retention_ci?.[String(b)]

                        const availability = row.availability?.[String(b)] || {}
                        const {
                          eligible_users = row.size,
                          cohort_size = row.size
                        } = availability

                        const ratio = cohort_size > 0 ? (eligible_users / cohort_size) : 1
                        const cellStyle = getAvailabilityStyle(ratio)

                        const label = mode === "hour" ? "Hour" : "Day"
                        const tooltip = `${label} ${b}\n\nValue: ${hasValue ? `${value.toFixed(2)}%` : '—'}\nAvailable for ${eligible_users} / ${cohort_size} users`

                        return (
                          <td
                            key={b}
                            className="col-numeric tabular-cell"
                            style={cellStyle}
                            title={tooltip}
                          >
                            <div className="retention-main">{hasValue ? `${value.toFixed(2)}%` : '—'}</div>
                            {includeCI && ci && ci.lower !== null && ci.upper !== null && (
                              <div className="retention-ci">
                                {Number(ci.lower).toFixed(2)}% - {Number(ci.upper).toFixed(2)}%
                              </div>
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {viewMode === 'graph' && (
            <RetentionGraph data={data} maxDay={maxDay} includeCI={includeCI} mode={mode} />
          )}
        </>
      )}

      <ComparePane
        isOpen={isComparePaneOpen}
        onClose={() => setIsComparePaneOpen(false)}
        tab="retention"
        maxDay={maxDay}
        granularity={mode}
        retentionType={retentionType}
        defaultMetric="retention_rate"
        retentionEvent={retentionEvent}
      />
    </section>
  )
}
