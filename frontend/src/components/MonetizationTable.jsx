import { useEffect, useMemo, useState, useRef } from 'react'
import { getAvailabilityStyle } from '../utils/style_utils'
import { getMonetization } from '../api'
import { buildMonetizationRows } from '../monetization'
import { formatCurrency, formatSplitValue } from '../utils/formatters'
import { fitPowerLaw, generateProjection } from '../utils/ltvPrediction'
import MonetizationGraph from './MonetizationGraph'
import TunePredictionPane from './TunePredictionPane'
import ComparePane from './ComparePane'

const METRIC_OPTIONS = [
  { value: 'total_revenue', label: 'Total Revenue' },
  { value: 'cumulative_revenue', label: 'Cumulative Revenue' },
  { value: 'revenue_per_acquired_user', label: 'Revenue per Acquired User' },
  { value: 'cumulative_revenue_per_acquired_user', label: 'Cumulative Revenue per Acquired User' },
  { value: 'revenue_per_retained_user', label: 'Revenue per Retained User' },
]

export default function MonetizationTable({ refreshToken, maxDay, retentionEvent, state, setState, cohorts = [] }) {
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
    <section className="card">
      <h2>Monetization</h2>
      <div className="monetization-layout">
        <div className="monetization-main">
          <div className="retention-header monetization-controls">
              <label>
                Metric
                <select
                  value={metricType}
                  onChange={(e) => {
                    setMetricType(e.target.value)
                    setPredictions(null)
                    setPredictionBaseline(null)
                    setIsTuningPaneOpen(false)
                  }}
                >
                  {METRIC_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
                </select>
              </label>
              <label>
                Horizon
                <select value={predictionHorizon} onChange={(e) => setPredictionHorizon(Number(e.target.value))}>
                  {[30, 60, 90, 180, 365].map((day) => <option key={day} value={day}>{day}D</option>)}
                </select>
              </label>

              <div className="view-toggle">
                <button
                  type="button"
                  className={`view-button ${viewMode === 'table' ? 'active ui-toggle-active' : ''}`}
                  onClick={() => setViewMode('table')}
                >
                  Table
                </button>
                <button
                  type="button"
                  className={`view-button ${viewMode === 'graph' ? 'active ui-toggle-active' : ''}`}
                  onClick={() => setViewMode('graph')}
                >
                  Graph
                </button>
              </div>

                <button
                  className={`view-button ${isPinned ? 'active' : ''}`}
                  onClick={() => setIsPinned((prev) => !prev)}
                  title="Pin Cohort Columns"
                >
                  {isPinned ? "📌" : "📍"}
                </button>
 
              <button className="button button-primary" onClick={loadData} disabled={loading}>
                {loading ? 'Loading...' : 'Load'}
              </button>
              <button
                className="button button-primary predict-button"
                type="button"
                onClick={handleProjectRevenue}
                disabled={!predictionEnabled || displayRows.length === 0}
                title={predictionEnabled ? '' : 'Prediction only available for cumulative metrics'}
              >
                Predict
              </button>
              {predictions && Object.keys(predictions).length > 0 && (
                <button
                  className="button button-secondary"
                  type="button"
                  onClick={() => {
                    setPredictionBaseline(JSON.parse(JSON.stringify(predictions)))
                    setIsTuningPaneOpen(true)
                  }}
                >
                  Tune
                </button>
              )}

              <button
                className="button button-secondary"
                type="button"
                onClick={() => setIsComparePaneOpen(true)}
              >
                🔬 Compare
              </button>
          </div>

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

                        const tooltip = `Day ${day}\n\nValue: ${displayValue}\nAvailable for ${eligible_users} / ${cohort_size} users`

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
        />
      </div>
    </section>
  )
}
