import { useEffect, useMemo, useState, useRef } from 'react'
import { createPortal } from 'react-dom'
import { getAvailabilityStyle } from '../utils/style_utils'
import { getRetention, listEvents } from '../api'
import { formatSplitValue } from '../utils/formatters'
import SearchableSelect from './SearchableSelect'
import RetentionGraph from './RetentionGraph'
import ComparePane from './ComparePane'

// ---------------------------------------------------------------------------
// Action Icons
// ---------------------------------------------------------------------------

function MicroscopeIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M6 18h8" />
      <path d="M3 22h18" />
      <path d="M14 22a7 7 0 1 0 0-14h-1" />
      <path d="M9 14l2 2" />
      <path d="M9 12a2 2 0 1 1-2-2V6h6v4a2 2 0 1 1-2 2z" />
      <path d="M12 6V3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3" />
    </svg>
  )
}

function ExportIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"></path>
      <circle cx="12" cy="13" r="4"></circle>
    </svg>
  )
}

function SigmaIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M18 7V4H6l7 8-7 8h12v-3" />
    </svg>
  )
}

function PinIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="17" x2="12" y2="22" />
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6a3 3 0 0 0-3-3 3 3 0 0 0-3 3v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
    </svg>
  )
}

function PinOffIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.5 }}>
      <line x1="12" y1="17" x2="12" y2="22" />
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6a3 3 0 0 0-3-3 3 3 0 0 0-3 3v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
      <line x1="2" y1="2" x2="22" y2="22" />
    </svg>
  )
}

function TableIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
      <line x1="3" y1="9" x2="21" y2="9" />
      <line x1="3" y1="15" x2="21" y2="15" />
      <line x1="9" y1="3" x2="9" y2="21" />
    </svg>
  )
}

function GraphIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="18" y1="20" x2="18" y2="10" />
      <line x1="12" y1="20" x2="12" y2="4" />
      <line x1="6" y1="20" x2="6" y2="14" />
    </svg>
  )
}

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
  appliedFilters = [],
  onAddToExport,
}) {
  const [isPinned, setIsPinned] = useState(state?.isPinned ?? true)
  const [events, setEvents] = useState([])
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  
  // Refactored state names
  const [ciEnabled, setCiEnabled] = useState(state?.ciEnabled ?? state?.includeCI ?? false)
  const [ciLevel, setCiLevel] = useState(state?.ciLevel ?? (state?.confidence ? Math.round(state.confidence * 100) : 95))
  const [showCiDropdown, setShowCiDropdown] = useState(false)
  const ciButtonRef = useRef(null)

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
      ciEnabled,
      ciLevel,
      viewMode,
      isComparePaneOpen,
      mode,
      retentionType,
      columnWidths
    }
    setState(nextState)
  }, [isPinned, ciEnabled, ciLevel, viewMode, isComparePaneOpen, mode, retentionType, columnWidths])

  const loadRetention = async () => {
    setLoading(true)
    setError('')

    try {
      const confidence = ciLevel / 100
      const response = await getRetention(Number(maxDay), retentionEvent, ciEnabled, confidence, retentionType, mode)
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
  }, [refreshToken, retentionEvent, maxDay, ciEnabled, ciLevel, mode, retentionType])

  // Click outside for CI dropdown
  useEffect(() => {
    if (!showCiDropdown) return
    const handleClickOutside = (e) => {
      if (ciButtonRef.current && !ciButtonRef.current.contains(e.target)) {
        setShowCiDropdown(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [showCiDropdown])

  const handleCiToggle = () => {
    setShowCiDropdown(prev => !prev)
  }

  const handleCiSelect = (level) => {
    if (level === 'off') {
       setCiEnabled(false)
    } else {
       setCiLevel(level)
       setCiEnabled(true)
    }
    setShowCiDropdown(false)
  }

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
      if (cohort.split_value === "__OTHER__") return `${cohort.split_property} = Other`
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
        const isOtherA = valA.endsWith(' = Other') || valA === 'Other'
        const isOtherB = valB.endsWith(' = Other') || valB === 'Other'
        if (isOtherA && !isOtherB) return 1
        if (isOtherB && !isOtherA) return -1
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

  const handleAddToExport = () => {
    const payload = {
      id: crypto.randomUUID(),
      version: 2,
      type: 'retention',
      title: 'Retention Analysis',
      summary: `Retention — ${sortedData.length} cohorts • Event: ${retentionEvent} • Max Day: ${maxDay}`,
      tables: [{
        title: `Retention Table (${retentionType === 'classic' ? 'Classic' : 'Ever-After'})`,
        columns: [
          { key: 'cohort', label: 'Cohort', type: 'string' },
          { key: 'split', label: 'Split', type: 'string' },
          { key: 'size', label: 'Size', type: 'number' },
          ...bucketColumns.map(b => ({ key: `${labelPrefix}${b}`, label: `${labelPrefix}${b}`, type: 'percentage' }))
        ],
        data: sortedData.map(row => {
          const rowObj = {
            cohort: getDisplayName(row),
            split: getSplitLabel(row),
            size: row.size
          }
          bucketColumns.forEach(b => {
            const val = row.retention[String(b)]
            rowObj[`${labelPrefix}${b}`] = val !== null && val !== undefined ? val / 100 : null
          })
          return rowObj
        })
      }],
      meta: {
        filters: appliedFilters,
        cohorts: cohorts.filter(c => sortedData.some(row => row.cohort_id === c.cohort_id)),
        settings: {
          'Max Day': maxDay,
          'Retention Event': retentionEvent,
          'Retention Type': retentionType,
          'Granularity': mode
        }
      }
    }
    onAddToExport(payload)
  }

  return (
    <section className="card">
      <div className="retention-header">
        <h2>Retention</h2>

        <div className="retention-controls">
          <div className="path-actions-group">
            <button
              type="button"
              className={`action-icon-button ${isComparePaneOpen ? 'compare-toggle-active' : ''}`}
              onClick={() => setIsComparePaneOpen(prev => !prev)}
              title="Statistical analysis"
              aria-label="Statistical analysis"
            >
              <MicroscopeIcon />
            </button>

            <button
              ref={ciButtonRef}
              type="button"
              className={`action-icon-button ${ciEnabled ? 'ci-active' : ''}`}
              onClick={handleCiToggle}
              title="Confidence interval"
              aria-label="Confidence interval"
              style={{ padding: '0 8px', width: 'auto', minWidth: '32px', gap: '4px' }}
            >
              <SigmaIcon />
              {ciEnabled && <span style={{ fontSize: '12px', fontWeight: 700 }}>({ciLevel}%)</span>}
            </button>

            <button
              type="button"
              className="action-icon-button"
              onClick={handleAddToExport}
              title="Export"
              aria-label="Export"
            >
              <ExportIcon />
            </button>
          </div>

          <div className="toolbar-separator" style={{ height: '24px', width: '1px', background: '#e2e8f0', margin: '0 8px' }} />

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
              title={isPinned ? "Unpin Columns" : "Pin Columns"}
            >
              {isPinned ? <PinIcon /> : <PinOffIcon />}
            </button>
            <button
              type="button"
              className={`view-button ${viewMode === 'table' ? 'active' : ''}`}
              onClick={() => setViewMode('table')}
              title="Table view"
            >
              <TableIcon />
            </button>
            <button
              type="button"
              className={`view-button ${viewMode === 'graph' ? 'active' : ''}`}
              onClick={() => setViewMode('graph')}
              title="Graph view"
            >
              <GraphIcon />
            </button>
          </div>
        </div>
      </div>

      {showCiDropdown && ciButtonRef.current && createPortal(
        <div 
          className="dropdown-menu animate-fade-in"
          style={{
            position: 'fixed',
            top: ciButtonRef.current.getBoundingClientRect().bottom + 6,
            left: ciButtonRef.current.getBoundingClientRect().left,
            zIndex: 10002
          }}
        >
          <div style={{ padding: '6px 12px 2px', fontSize: '11px', fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em' }}>CI Settings</div>
          <button 
            className={!ciEnabled ? 'selected' : ''}
            onClick={() => handleCiSelect('off')}
          >
            Off
            {!ciEnabled && <span>✓</span>}
          </button>
          <div style={{ height: '1px', background: '#f1f5f9', margin: '4px 0' }} />
          {[99, 95, 90].map(level => (
            <button 
              key={level} 
              className={ciEnabled && ciLevel === level ? 'selected' : ''}
              onClick={() => handleCiSelect(level)}
            >
              {level}%
              {ciEnabled && ciLevel === level && <span>✓</span>}
            </button>
          ))}
        </div>,
        document.body
      )}

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
              column="event_name"
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
                        const availabilityPct = Math.round((eligible_users / cohort_size) * 100)
                        const tooltip = `${label} ${b}\n\nValue: ${hasValue ? `${value.toFixed(2)}%` : '—'}\nAvailability: ${availabilityPct}% (${eligible_users} / ${cohort_size} users)`

                        return (
                          <td
                            key={b}
                            className="col-numeric tabular-cell"
                            style={cellStyle}
                            title={tooltip}
                          >
                            <div className="retention-main">{hasValue ? `${value.toFixed(2)}%` : '—'}</div>
                            {ciEnabled && ci && ci.lower !== null && ci.upper !== null && (
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
            <RetentionGraph data={data} maxDay={maxDay} includeCI={ciEnabled} mode={mode} />
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
        appliedFilters={appliedFilters}
        onAddToExport={onAddToExport}
      />
    </section>
  )
}
