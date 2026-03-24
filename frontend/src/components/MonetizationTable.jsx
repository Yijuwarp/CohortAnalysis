import { useEffect, useMemo, useState } from 'react'
import { getMonetization } from '../api'
import { buildMonetizationRows } from '../monetization'
import { formatCurrency } from '../utils/formatters'
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

export default function MonetizationTable({ refreshToken, maxDay, retentionEvent, state, setState }) {
  const [metricType, setMetricType] = useState(state?.metricType || 'cumulative_revenue_per_acquired_user')
  const [viewMode, setViewMode] = useState(state?.viewMode || 'table')
  const [revenueRows, setRevenueRows] = useState([])
  const [cohortSizes, setCohortSizes] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [predictions, setPredictions] = useState(state?.predictions || null)
  const [predictionHorizon, setPredictionHorizon] = useState(state?.predictionHorizon ?? 90)
  const [isTuningPaneOpen, setIsTuningPaneOpen] = useState(state?.isTuningPaneOpen ?? false)
  const [predictionBaseline, setPredictionBaseline] = useState(state?.predictionBaseline || null)
  const [isComparePaneOpen, setIsComparePaneOpen] = useState(state?.isComparePaneOpen ?? false)
  const [showPredictionSummary, setShowPredictionSummary] = useState(state?.showPredictionSummary ?? true)

  useEffect(() => {
    setState({
      metricType,
      viewMode,
      predictions,
      predictionHorizon,
      isTuningPaneOpen,
      predictionBaseline,
      isComparePaneOpen,
      showPredictionSummary
    })
  }, [metricType, viewMode, predictions, predictionHorizon, isTuningPaneOpen, predictionBaseline, isComparePaneOpen, showPredictionSummary, setState])

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
    dayColumns,
    metricType,
  }), [cohortSizes, dayColumns, metricType, retainedRows, revenueRows])

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
                type="button"
                className={`compare-open-button button-secondary compare-btn-align ${isComparePaneOpen ? 'active' : ''}`}
                onClick={() => setIsComparePaneOpen(prev => !prev)}
                title="Compare two cohorts statistically"
              >
                Compare
              </button>
          </div>

          {!loading && error && <p className="error">{error}</p>}
          {loading && <div className="loader">Loading monetization data...</div>}
          {!loading && !error && displayRows.length === 0 && (
            <div className="loader">No revenue data available</div>
          )}

          {!loading && displayRows.length > 0 && viewMode === 'table' && (
            <div className="analytics-table table-responsive monetization-data-table">
              <table>
                <thead>
                  <tr>
                    <th className="sticky-col sticky-col-top sticky-col-left">Cohort</th>
                    <th className="sticky-col sticky-col-top col-numeric">Size</th>
                    {visibleDayColumns.map((day) => <th key={day} className="sticky-col sticky-col-top col-numeric">D{day}</th>)}
                    <th className="col-prediction sticky-col sticky-col-top col-numeric predicted-col-header">Predicted ({predictionHorizon}D)</th>
                  </tr>
                </thead>
                <tbody>
                  {displayRows.map((row) => (
                    <tr key={row.cohort_id}>
                      <td className="sticky-col sticky-col-left cohort-name-cell" title={row.cohort_name}>{row.cohort_name}</td>
                      <td className="col-numeric cohort-size-cell">{Number(row.size).toLocaleString()}</td>
                      {visibleDayColumns.map((day) => <td key={day} className="col-numeric tabular-cell">{row.displayValues[String(day)] ?? '—'}</td>)}
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
