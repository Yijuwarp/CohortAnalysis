import { useEffect, useMemo, useState, useRef } from 'react'
import { getAvailabilityStyle } from '../utils/style_utils'
import { getMonetization } from '../api'
import { buildMonetizationRows } from '../monetization'
import { formatCurrency, formatSplitValue } from '../utils/formatters'
import { fitPowerLaw, generateProjection } from '../utils/ltvPrediction'
import MonetizationGraph from './MonetizationGraph'
import TunePredictionPane from './TunePredictionPane'
import ComparePane from './ComparePane'

// ---------------------------------------------------------------------------
// Action Icons
// ---------------------------------------------------------------------------

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
      <path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z" />
      <circle cx="12" cy="13" r="4" />
    </svg>
  )
}

function ReloadIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8" />
      <polyline points="21 3 21 8 16 8" />
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

function PredictIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 16H17" />
      <path d="M3 4V16" />
      <path d="M4 13L8 10L11 11L13 8" />
      <path d="M13 8L17 6" strokeDasharray="2 2" />
    </svg>
  )
}

function TuneIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 6H16" />
      <circle cx="8" cy="6" r="1.5" />
      <path d="M4 10H16" />
      <circle cx="12" cy="10" r="1.5" />
      <path d="M4 14H16" />
      <circle cx="10" cy="14" r="1.5" />
    </svg>
  )
}

const METRIC_OPTIONS = [
  { value: 'total_revenue', label: 'Total Revenue' },
  { value: 'cumulative_revenue', label: 'Cumulative Revenue' },
  { value: 'revenue_per_acquired_user', label: 'Revenue per Acquired User' },
  { value: 'cumulative_revenue_per_acquired_user', label: 'Cumulative Revenue per Acquired User' },
  { value: 'revenue_per_retained_user', label: 'Revenue per Retained User' },
]

export default function MonetizationTable({ refreshToken, maxDay, retentionEvent, state, setState, cohorts = [], appliedFilters = [], onAddToExport }) {
  const [metricType, setMetricType] = useState(state?.metricType || 'cumulative_revenue_per_acquired_user')
  const [viewMode, setViewMode] = useState(state?.viewMode || 'table')
  const [revenueRows, setRevenueRows] = useState([])
  const [cohortSizes, setCohortSizes] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [eligibilityRows, setEligibilityRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [predictions, setPredictions] = useState(state?.predictions || null)
  const [predictionHorizon, setPredictionHorizon] = useState(state?.predictionHorizon ?? 90)
  const [isTuningPaneOpen, setIsTuningPaneOpen] = useState(state?.isTuningPaneOpen ?? false)
  const [predictionBaseline, setPredictionBaseline] = useState(state?.predictionBaseline || null)
  const [isComparePaneOpen, setIsComparePaneOpen] = useState(state?.isComparePaneOpen ?? false)
  const [showPredictionSummary, setShowPredictionSummary] = useState(state?.showPredictionSummary ?? true)
  const [sortConfig, setSortConfig] = useState({ key: 'size', direction: 'desc' })
  const [isPinned, setIsPinned] = useState(state?.isPinned ?? true)
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

  // 1. Build Metadata Lookup
  const cohortMetaMap = useMemo(() => {
    const map = {}
    cohorts.forEach(c => {
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

  const getSortValue = (row, key) => {
    if (key === 'cohort_name') return row.cohort_name
    if (key === 'split') return getSplitLabel(row)
    if (key === 'size') return row.size || 0
    if (key === 'predicted') return predictions?.[row.cohort_id]?.projectedCurve?.[predictionHorizon] || 0
    if (key.startsWith('D')) {
      const day = key.slice(1)
      return row.values?.[day] ?? 0
    }
    return row.values?.[key] ?? 0
  }

  const handleSort = (key) => {
    setSortConfig((prev) => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc',
    }))
  }

  useEffect(() => {
    const nextState = {
      metricType,
      viewMode,
      predictions,
      predictionHorizon,
      isTuningPaneOpen,
      predictionBaseline,
      isComparePaneOpen,
      showPredictionSummary,
      isPinned,
      columnWidths
    }
    setState(nextState)
  }, [metricType, viewMode, predictions, predictionHorizon, isTuningPaneOpen, predictionBaseline, isComparePaneOpen, showPredictionSummary, isPinned, columnWidths])

  const safeMaxDay = useMemo(() => {
    const numericMaxDay = Number(maxDay)
    if (!Number.isFinite(numericMaxDay) || numericMaxDay < 0) {
      return 7
    }

    return Math.floor(numericMaxDay)
  }, [maxDay])

  const predictionEnabled = metricType === 'cumulative_revenue' || metricType === 'cumulative_revenue_per_acquired_user'

  const loadData = async (overrideMaxDay) => {
    const nextMaxDay = typeof overrideMaxDay === 'number' ? overrideMaxDay : safeMaxDay

    setLoading(true)
    setError('')
    try {
      const response = await getMonetization(nextMaxDay)
      setRevenueRows(response.revenue_table || [])
      setCohortSizes(response.cohort_sizes || [])
      setRetainedRows(response.retained_users_table || [])
      setEligibilityRows(response.eligibility_table || [])
    } catch (err) {
      setError(err.message)
      setRevenueRows([])
      setCohortSizes([])
      setRetainedRows([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData(safeMaxDay)
  }, [refreshToken, safeMaxDay])

  const dayColumns = useMemo(() => Array.from({ length: safeMaxDay + 1 }, (_, idx) => idx), [safeMaxDay])

  const displayRows = useMemo(() => buildMonetizationRows({
    cohortSizes,
    retainedRows,
    revenueRows,
    eligibilityRows,
    dayColumns,
    metricType,
  }), [cohortSizes, dayColumns, metricType, retainedRows, revenueRows, eligibilityRows])

  const showSplit = useMemo(() => {
    return (cohorts || []).some(c => c.split_type != null)
  }, [cohorts])

  const sortedRows = useMemo(() => {
    const rows = [...(displayRows || [])]
    return rows.sort((a, b) => {
      const valA = getSortValue(a, sortConfig.key)
      const valB = getSortValue(b, sortConfig.key)

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
  }, [displayRows, sortConfig, predictions, predictionHorizon, cohorts])

  const effectiveMaxDay = safeMaxDay

  const visibleDayColumns = useMemo(
    () => Array.from({ length: effectiveMaxDay + 1 }, (_, idx) => idx),
    [effectiveMaxDay],
  )

  const handleProjectRevenue = () => {
    if (predictions) {
      setPredictions(null)
      setPredictionBaseline(null)
      setIsTuningPaneOpen(false)
      return
    }
    if (!predictionEnabled) {
      return
    }

    const nextPredictions = {}

    displayRows.forEach((row) => {
      const days = []
      const values = []

      for (let day = 0; day <= Number(effectiveMaxDay); day += 1) {
        const value = Number(row.numericValues?.[String(day)])
        if (Number.isFinite(value)) {
          days.push(day)
          values.push(value)
        }
      }

      if (days.length < 2) {
        return
      }

      const fit = fitPowerLaw(days, values)
      const projection = generateProjection({
        A: fit.A,
        B: fit.B,
        lastObservedDay: Number(effectiveMaxDay),
        horizonDays: 365,
        residualVariance: fit.residualVariance,
      })

      nextPredictions[row.cohort_id] = {
        A: fit.A,
        B: fit.B,
        residualVariance: fit.residualVariance,
        lastObservedDay: Number(effectiveMaxDay),
        projectedCurve: projection.projectedCurve,
        upperCI: projection.upperCI,
        lowerCI: projection.lowerCI,
      }
    })

    setPredictions(nextPredictions)
    setPredictionBaseline(nextPredictions)
    setShowPredictionSummary(true)
  }

  const handleAddToExport = () => {
    const isCumulative = metricType.includes('cumulative')
    const metricLabel = METRIC_OPTIONS.find(o => o.value === metricType)?.label || metricType

    const columns = [
      { key: 'cohort', label: 'Cohort', type: 'string' },
      { key: 'split', label: 'Split', type: 'string' },
      { key: 'size', label: 'Size', type: 'number' },
      ...visibleDayColumns.map(d => ({ 
        key: `D${d}`, 
        label: `D${d}`, 
        type: 'currency' 
      }))
    ]

    if (predictions) {
      columns.push({ key: 'predicted', label: `Predicted (${predictionHorizon}D)`, type: 'currency' })
    }

    const payload = {
      id: crypto.randomUUID(),
      version: 2,
      type: 'monetization',
      title: 'Monetization Analysis',
      summary: `Monetization — ${metricLabel} • Prediction: ${predictions ? 'Enabled' : 'Disabled'}`,
      tables: [{
        title: `${metricLabel} (${isCumulative ? 'Cumulative' : 'Non-Cumulative'})`,
        columns,
        data: sortedRows.map(row => {
          const rowObj = {
            cohort: getDisplayName(row),
            split: getSplitLabel(row),
            size: row.size
          }
          visibleDayColumns.forEach(d => {
            rowObj[`D${d}`] = row.numericValues?.[String(d)] ?? null
          })
          if (predictions) {
            rowObj.predicted = predictions[row.cohort_id]?.projectedCurve?.[predictionHorizon] ?? null
          }
          return rowObj
        })
      }],
      meta: {
        filters: appliedFilters,
        cohorts: cohorts.filter(c => displayRows.some(row => row.cohort_id === c.cohort_id)),
        settings: {
          'Metric': metricLabel,
          'Max Day': maxDay,
          'Retention Event': retentionEvent,
          'Prediction Horizon': `${predictionHorizon}D`,
          'Prediction Baseline': predictionBaseline ? 'Custom' : 'System'
        }
      }
    }
    onAddToExport(payload)
  }

  const predictionSummary = useMemo(() => {
    if (!predictions) return []
    return displayRows
      .filter((row) => predictions[row.cohort_id])
      .map((row) => ({
        id: row.cohort_id,
        name: row.cohort_name,
        value: predictions[row.cohort_id]?.projectedCurve?.[predictionHorizon],
      }))
  }, [displayRows, predictionHorizon, predictions])

  const hasSidebarContent = (predictionSummary.length > 0 && showPredictionSummary) || isTuningPaneOpen;

  return (
    <section className="card monetization-refactor-card">
      <div className="usage-query-container">
        {/* Row 1: Primary Controls */}
        <div className="usage-query-row">
          <div className="query-main">
            <span className="query-label" style={{ marginRight: '8px' }}>Metric</span>
            <select
              className="metric-select"
              value={metricType}
              onChange={(e) => {
                setMetricType(e.target.value)
                setPredictions(null)
                setPredictionBaseline(null)
                setIsTuningPaneOpen(false)
              }}
            >
              {METRIC_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
            
            <div className="view-toggle" style={{ marginLeft: '12px' }}>
              <button
                type="button"
                className={`view-toggle-button ${viewMode === 'table' ? 'active' : ''}`}
                onClick={() => setViewMode('table')}
                title="Table View"
              >
                <TableIcon />
              </button>
              <button
                type="button"
                className={`view-toggle-button ${viewMode === 'graph' ? 'active' : ''}`}
                onClick={() => setViewMode('graph')}
                title="Graph View"
              >
                <GraphIcon />
              </button>
            </div>
          </div>

          <div className="query-actions">
            <button
              type="button"
              className={`action-icon-button ${isPinned ? 'active' : ''}`}
              onClick={() => setIsPinned((prev) => !prev)}
              title={isPinned ? "Unpin Cohort Columns" : "Pin Cohort Columns"}
            >
              {isPinned ? <PinIcon /> : <PinOffIcon />}
            </button>

            <button
              type="button"
              className="action-icon-button"
              onClick={() => loadData()}
              disabled={loading}
              title={loading ? "Loading..." : "Reload Analysis"}
            >
              <ReloadIcon />
            </button>

            <button
              type="button"
              className={`action-icon-button ${isComparePaneOpen ? 'active' : ''}`}
              onClick={() => setIsComparePaneOpen(true)}
              title="Compare cohorts"
              disabled={!displayRows.length}
            >
              <MicroscopeIcon />
            </button>

            <button
              type="button"
              className="action-icon-button"
              onClick={handleAddToExport}
              title="Snapshot (Add to Export)"
              disabled={displayRows.length === 0}
            >
              <ExportIcon />
            </button>
          </div>
        </div>

        {/* Row 2: Prediction Controls */}
        <div className="usage-query-row" style={{ borderTop: '1px solid #e2e8f0' }}>
          <div className="query-main">
            <button
              className={`action-icon-button ${predictions ? 'active' : ''}`}
              type="button"
              onClick={handleProjectRevenue}
              disabled={!predictionEnabled || displayRows.length === 0}
              title={predictionEnabled ? 'Predict revenue' : 'Prediction only available for cumulative metrics'}
            >
              <PredictIcon />
            </button>

            <span className="query-label" style={{ marginLeft: '12px', marginRight: '8px' }}>Horizon</span>
            <select 
              className="metric-select"
              value={predictionHorizon} 
              onChange={(e) => setPredictionHorizon(Number(e.target.value))}
            >
              {[30, 60, 90, 180, 365].map((day) => (
                <option key={day} value={day}>{day}D</option>
              ))}
            </select>

            {predictions && Object.keys(predictions).length > 0 && (
              <button
                className={`action-icon-button ${isTuningPaneOpen ? 'active' : ''}`}
                type="button"
                onClick={() => {
                  if (isTuningPaneOpen) {
                    setIsTuningPaneOpen(false)
                  } else {
                    setPredictionBaseline(JSON.parse(JSON.stringify(predictions)))
                    setIsTuningPaneOpen(true)
                  }
                }}
                style={{ marginLeft: '12px' }}
                title="Tune prediction parameters"
              >
                <TuneIcon />
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="monetization-layout" style={{ marginTop: '24px' }}>
        <div className="monetization-main">
          {!loading && error && <p className="error">{error}</p>}
          {loading && <div className="loader">Loading monetization data...</div>}
          {!loading && !error && displayRows.length === 0 && (
            <div className="loader">No revenue data available</div>
          )}

          {!loading && displayRows.length > 0 && viewMode === 'table' && (
            <div className={`analytics-table table-responsive monetization-data-table ${showSplit ? 'has-split' : ''}`}>
              <table>
                <thead>
                  <tr>
                    <th
                      className={`${isPinned ? 'sticky-col sticky-col-cohort' : ''} sticky-col-top sortable-header`}
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
                        className={`${isPinned ? 'sticky-col sticky-col-split' : ''} sticky-col-top sortable-header`}
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
                      className={`${isPinned ? 'sticky-col sticky-col-size' : ''} sticky-col-top sortable-header`}
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
                    {visibleDayColumns.map((day) => (
                      <th
                        key={day}
                        className="sticky-col sticky-col-top col-numeric sortable-header"
                        onClick={() => {
                          if (isResizingRef.current) return
                          handleSort(`D${day}`)
                        }}
                      >
                        D{day} {sortConfig.key === `D${day}` && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                      </th>
                    ))}
                    <th
                      className="col-prediction sticky-col sticky-col-top col-numeric predicted-col-header sortable-header"
                      onClick={() => {
                        if (isResizingRef.current) return
                        handleSort('predicted')
                      }}
                    >
                      Predicted ({predictionHorizon}D) {sortConfig.key === 'predicted' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedRows.map((row) => (
                    <tr key={row.cohort_id}>
                      <td 
                        className={isPinned ? 'sticky-col sticky-col-cohort' : ''} 
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
                          className={isPinned ? 'sticky-col sticky-col-split' : ''}
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
                        {Number(row.size).toLocaleString()}
                      </td>
                      {visibleDayColumns.map((day) => {
                        const displayValue = row.displayValues[String(day)] ?? '—'
                        const availability = row.availabilityValues?.[String(day)] || {}
                        const {
                          eligible_users = row.size,
                          cohort_size = row.size
                        } = availability

                        const ratio = cohort_size > 0 ? (eligible_users / cohort_size) : 1
                        const cellStyle = getAvailabilityStyle(ratio)

                        const availabilityPct = Math.round((eligible_users / cohort_size) * 100)
                        const tooltip = `Day ${day}\n\nValue: ${displayValue}\nAvailability: ${availabilityPct}% (${eligible_users} / ${cohort_size} users)`

                        return (
                          <td
                            key={day}
                            className="col-numeric tabular-cell"
                            style={cellStyle}
                            title={tooltip}
                          >
                            {displayValue}
                          </td>
                        )
                      })}
                      <td className="col-prediction col-numeric predicted-cell">{formatCurrency(predictions?.[row.cohort_id]?.projectedCurve?.[predictionHorizon])}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {!loading && viewMode === 'graph' && (
            <MonetizationGraph
              rows={displayRows}
              maxDay={effectiveMaxDay}
              metricType={metricType}
              predictions={predictions}
              predictionHorizon={predictionHorizon}
              effectiveMaxDay={effectiveMaxDay}
            />
          )}
        </div>

        {hasSidebarContent && (
          <div className="monetization-side-panel">
            {predictionSummary.length > 0 && showPredictionSummary && (
              <aside className="prediction-sticky-card summary-card">
                <div className="summary-header">
                  <h4>Prediction Summary</h4>
                  <button 
                    type="button" 
                    className="summary-close-btn"
                    onClick={() => setShowPredictionSummary(false)}
                  >
                    ✕
                  </button>
                </div>
                <div className="summary-body">
                  {predictionSummary.map((item) => (
                    <div key={item.id} className="prediction-row">
                      <span className="summary-name">{item.name}</span>
                      <strong className="summary-val">{formatCurrency(item.value)}</strong>
                    </div>
                  ))}
                </div>
              </aside>
            )}

            <TunePredictionPane
              isOpen={isTuningPaneOpen}
              onClose={() => setIsTuningPaneOpen(false)}
              predictions={predictions}
              displayRows={displayRows}
              onLiveUpdate={setPredictions}
              onCancel={() => {
                if (predictionBaseline) {
                  setPredictions(JSON.parse(JSON.stringify(predictionBaseline)))
                }
                setIsTuningPaneOpen(false)
              }}
            />
          </div>
        )}

        <ComparePane
          isOpen={isComparePaneOpen}
          onClose={() => setIsComparePaneOpen(false)}
          tab="monetization"
          maxDay={safeMaxDay}
          retentionEvent={retentionEvent}
          defaultMetric={metricType === 'cumulative_revenue' || metricType === 'total_revenue' ? 'revenue_per_acquired_user' : metricType}
          appliedFilters={appliedFilters}
          onAddToExport={onAddToExport}
        />
      </div>
    </section>
  )
}
