import { useEffect, useMemo, useState } from 'react'
import { getMonetization } from '../api'
import { buildMonetizationRows } from '../monetization'
import { formatCurrency } from '../utils/formatters'
import { fitPowerLaw, generateProjection } from '../utils/ltvPrediction'
import MonetizationGraph from './MonetizationGraph'
import TunePredictionPane from './TunePredictionPane'

const METRIC_OPTIONS = [
  { value: 'total_revenue', label: 'Total Revenue' },
  { value: 'cumulative_revenue', label: 'Cumulative Revenue' },
  { value: 'revenue_per_acquired_user', label: 'Revenue per Acquired User' },
  { value: 'cumulative_revenue_per_acquired_user', label: 'Cumulative Revenue per Acquired User' },
  { value: 'revenue_per_retained_user', label: 'Revenue per Retained User' },
]

const MAX_DAY_DETECTION_WINDOW = 365

export default function MonetizationTable({ refreshToken, maxDay }) {
  const [isPinned, setIsPinned] = useState(true)
  const [metricType, setMetricType] = useState('cumulative_revenue_per_acquired_user')
  const [viewMode, setViewMode] = useState('table')
  const [revenueRows, setRevenueRows] = useState([])
  const [cohortSizes, setCohortSizes] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [predictions, setPredictions] = useState(null)
  const [predictionHorizon, setPredictionHorizon] = useState(90)
  const [isTuningPaneOpen, setIsTuningPaneOpen] = useState(false)

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
  }

  return (
    <section className="card">
      <h2>7. Monetization</h2>
      <div className="retention-header">
        <div className="retention-controls-left">
          <label>
            Max Day
            <input
              type="number"
              min="0"
              value={maxDay}
              onChange={(e) => {
                setUserModifiedMaxDay(true)
                setMaxDay(e.target.value)
              }}
            />
          </label>
          <label>
            Metric
            <select
              value={metricType}
              onChange={(e) => {
                setMetricType(e.target.value)
                setPredictions(null)
              }}
            >
              {METRIC_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label>
            Prediction Horizon
            <select value={predictionHorizon} onChange={(e) => setPredictionHorizon(Number(e.target.value))}>
              {[30, 60, 90, 180, 365].map((day) => <option key={day} value={day}>{day}D</option>)}
            </select>
          </label>
          <button className="button button-primary" onClick={loadData} disabled={loading}>
            {loading ? 'Loading...' : 'Load Monetization'}
          </button>
          <button
            className="button"
            type="button"
            onClick={handleProjectRevenue}
            disabled={!predictionEnabled || displayRows.length === 0}
            title={predictionEnabled ? '' : 'Prediction only available for cumulative metrics'}
          >
            Project Revenue
          </button>
          <button
            className="button button-secondary"
            type="button"
            onClick={() => setIsTuningPaneOpen(true)}
            disabled={!predictions || Object.keys(predictions).length === 0}
            title="Tune the A and B parameters of current predictions"
          >
            Tune Prediction
          </button>
        </div>

        <div className="retention-controls-right">
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

      {error && <p className="error">{error}</p>}

      {displayRows.length > 0 && viewMode === 'table' && (
        <div className="analytics-table table-responsive">
          <table>
            <thead>
              <tr>
                <th className={isPinned ? 'sticky-col sticky-col-cohort' : ''}>Cohort</th>
                <th className={isPinned ? 'sticky-col sticky-col-size' : ''}>Size</th>
                {visibleDayColumns.map((day) => <th key={day}>D{day}</th>)}
                <th className="col-prediction">Predicted Revenue ({predictionHorizon}D)</th>
              </tr>
            </thead>
            <tbody>
              {displayRows.map((row) => (
                <tr key={row.cohort_id}>
                  <td
                    className={isPinned ? 'sticky-col sticky-col-cohort' : ''}
                    title={row.cohort_name}
                  >
                    {row.cohort_name}
                  </td>
                  <td className={isPinned ? 'sticky-col sticky-col-size' : ''}>{row.size}</td>
                  {visibleDayColumns.map((day) => <td key={day}>{row.displayValues[String(day)] ?? '—'}</td>)}
                  <td className="col-prediction">
                    {formatCurrency(predictions?.[row.cohort_id]?.projectedCurve?.[predictionHorizon])}
                    {predictions?.[row.cohort_id]?.lastObservedDay && predictions?.[row.cohort_id]?.lastObservedDay !== effectiveMaxDay && (
                      <span className="muted-text" style={{ display: 'block', fontSize: '10px' }}>
                        Based on D{predictions[row.cohort_id].lastObservedDay}
                      </span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {viewMode === 'graph' && (
        <MonetizationGraph
          rows={displayRows}
          maxDay={effectiveMaxDay}
          metricType={metricType}
          predictions={predictions}
          predictionHorizon={predictionHorizon}
          effectiveMaxDay={effectiveMaxDay}
        />
      )}

      <TunePredictionPane
        isOpen={isTuningPaneOpen}
        onClose={() => setIsTuningPaneOpen(false)}
        predictions={predictions}
        displayRows={displayRows}
        effectiveMaxDay={effectiveMaxDay}
        predictionHorizon={predictionHorizon}
        onUpdate={(newPredictions) => setPredictions(newPredictions)}
      />
    </section>
  )
}
