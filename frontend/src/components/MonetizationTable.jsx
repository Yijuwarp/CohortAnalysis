import { useEffect, useMemo, useState } from 'react'
import { getMonetization, getRevenueEvents } from '../api'
import { buildMonetizationRows } from '../monetization'
import { formatCurrency } from '../utils/formatters'
import { fitSaturatingExponential, generateProjection } from '../utils/ltvPrediction'
import MonetizationGraph from './MonetizationGraph'

const METRIC_OPTIONS = [
  { value: 'total_revenue', label: 'Total Revenue' },
  { value: 'cumulative_revenue', label: 'Cumulative Revenue' },
  { value: 'revenue_per_acquired_user', label: 'Revenue per Acquired User' },
  { value: 'cumulative_revenue_per_acquired_user', label: 'Cumulative Revenue per Acquired User' },
  { value: 'revenue_per_retained_user', label: 'Revenue per Retained User' },
]


export default function MonetizationTable({ refreshToken }) {
  const [maxDay, setMaxDay] = useState(7)
  const [effectiveMaxDay, setEffectiveMaxDay] = useState(7)
  const [userModifiedMaxDay, setUserModifiedMaxDay] = useState(false)
  const [metricType, setMetricType] = useState('cumulative_revenue_per_acquired_user')
  const [viewMode, setViewMode] = useState('table')
  const [revenueRows, setRevenueRows] = useState([])
  const [cohortSizes, setCohortSizes] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [hasRevenueMapping, setHasRevenueMapping] = useState(false)
  const [hasNoSelectedRevenueEvents, setHasNoSelectedRevenueEvents] = useState(false)
  const [predictions, setPredictions] = useState(null)
  const [predictionHorizon, setPredictionHorizon] = useState(90)
  const [availableRevenueEvents, setAvailableRevenueEvents] = useState([])
  const [pendingRevenueConfig, setPendingRevenueConfig] = useState({})
  const [pendingOverrideInputs, setPendingOverrideInputs] = useState({})
  const [revenueConfig, setRevenueConfig] = useState({})
  const [eventToAdd, setEventToAdd] = useState('')

  const predictionEnabled = metricType === 'cumulative_revenue' || metricType === 'cumulative_revenue_per_acquired_user'

  const loadRevenueConfig = async () => {
    try {
      const payload = await getRevenueEvents()
      const events = payload.events || []
      const nextPendingConfig = events.reduce((acc, event) => {
        acc[event.event_name] = {
          included: Boolean(event.is_included),
          override: null,
        }
        return acc
      }, {})
      setHasRevenueMapping(Boolean(payload.has_revenue_mapping))
      setHasNoSelectedRevenueEvents(Boolean(payload.has_revenue_mapping) && events.length > 0 && events.every((event) => !event.is_included))
      setAvailableRevenueEvents(events.map((event) => event.event_name))
      setPendingRevenueConfig(nextPendingConfig)
      setPendingOverrideInputs(events.reduce((acc, event) => ({ ...acc, [event.event_name]: '' }), {}))
      setRevenueConfig({})
      setEventToAdd('')
    } catch {
      setHasRevenueMapping(false)
      setHasNoSelectedRevenueEvents(false)
      setAvailableRevenueEvents([])
      setPendingRevenueConfig({})
      setPendingOverrideInputs({})
      setRevenueConfig({})
      setEventToAdd('')
    }
  }

  const applyRevenueOverrides = (rawRevenueRows, config) => {
    if (!Array.isArray(rawRevenueRows) || rawRevenueRows.length === 0) {
      return []
    }

    if (!config || Object.keys(config).length === 0) {
      return rawRevenueRows
    }


    const aggregatedRows = new Map()
    rawRevenueRows.forEach((row) => {
      const eventConfig = config[row.event_name]
      if (!eventConfig || eventConfig.included === false) {
        return
      }

      const eventCount = Number(row.event_count ?? 1)
      const safeEventCount = Number.isFinite(eventCount) ? eventCount : 1
      const rowRevenue = eventConfig.override !== null
        ? safeEventCount * Number(eventConfig.override)
        : Number(row.revenue ?? 0)

      const key = `${row.cohort_id}:${row.day_number}`
      const existing = aggregatedRows.get(key)
      if (existing) {
        existing.revenue += rowRevenue
        return
      }

      aggregatedRows.set(key, {
        cohort_id: row.cohort_id,
        cohort_name: row.cohort_name,
        day_number: row.day_number,
        revenue: rowRevenue,
      })
    })

    return Array.from(aggregatedRows.values())
  }

  const loadData = async () => {
    setLoading(true)
    setError('')
    try {
      const response = await getMonetization(Number(maxDay))
      setRevenueRows(response.revenue_table || [])
      setCohortSizes(response.cohort_sizes || [])
      setRetainedRows(response.retained_users_table || [])
      setPredictions(null)
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
    const run = async () => {
      await loadRevenueConfig()
    }
    run()
  }, [refreshToken])

  useEffect(() => {
    if (hasRevenueMapping) {
      loadData()
    }
  }, [hasRevenueMapping, maxDay, refreshToken])

  const dayColumns = useMemo(() => Array.from({ length: Number(maxDay) + 1 }, (_, idx) => idx), [maxDay])

  const adjustedRevenueRows = useMemo(() => applyRevenueOverrides(revenueRows, revenueConfig), [revenueConfig, revenueRows])

  const displayRows = useMemo(() => buildMonetizationRows({
    cohortSizes,
    retainedRows,
    revenueRows: adjustedRevenueRows,
    dayColumns,
    metricType,
  }), [adjustedRevenueRows, cohortSizes, dayColumns, metricType, retainedRows])

  const invalidOverrideEvents = useMemo(
    () => Object.entries(pendingOverrideInputs)
      .filter(([, value]) => value !== '' && !Number.isFinite(Number(value)))
      .map(([eventName]) => eventName),
    [pendingOverrideInputs]
  )

  const canApplyRevenueChanges = invalidOverrideEvents.length === 0
  const hasCustomOverrides = Object.values(revenueConfig).some(
    (cfg) => cfg.override !== null || cfg.included === false
  )
  const addableRevenueEvents = useMemo(
    () => availableRevenueEvents.filter((eventName) => !pendingRevenueConfig[eventName]),
    [availableRevenueEvents, pendingRevenueConfig]
  )

  const handleOverrideChange = (eventName, value) => {
    setPendingOverrideInputs((previous) => ({ ...previous, [eventName]: value }))

    if (value === '') {
      setPendingRevenueConfig((previous) => ({
        ...previous,
        [eventName]: {
          ...previous[eventName],
          override: null,
        },
      }))
      return
    }

    const parsed = Number(value)
    if (!Number.isFinite(parsed)) {
      return
    }

    setPendingRevenueConfig((previous) => ({
      ...previous,
      [eventName]: {
        ...previous[eventName],
        override: parsed,
      },
    }))
  }

  const handleApplyRevenueChanges = () => {
    if (!canApplyRevenueChanges) {
      return
    }

    setRevenueConfig(pendingRevenueConfig)
    setPredictions(null)
  }

  useEffect(() => {
    if (userModifiedMaxDay) {
      setEffectiveMaxDay(Number(maxDay))
      return
    }

    if (!revenueRows.length) {
      return
    }


    let lastNonZero = 0
    displayRows.forEach((row) => {
      Object.entries(row.numericValues || {}).forEach(([day, value]) => {
        const numeric = Number(value)
        if (!Number.isNaN(numeric) && numeric !== 0) {
          lastNonZero = Math.max(lastNonZero, Number(day))
        }
      })
    })

    if (lastNonZero === 0) {
      return
    }

    const adjusted = Math.min(Number(maxDay), lastNonZero)
    setEffectiveMaxDay(adjusted)

    if (Number(maxDay) !== adjusted) {
      setMaxDay(adjusted)
    }
  }, [maxDay, revenueRows, userModifiedMaxDay])

  const visibleDayColumns = useMemo(
    () => Array.from({ length: Number(effectiveMaxDay) + 1 }, (_, idx) => idx),
    [effectiveMaxDay]
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

      const fit = fitSaturatingExponential(days, values)
      const projection = generateProjection({
        L: fit.L,
        k: fit.k,
        lastObservedDay: Number(effectiveMaxDay),
        horizonDays: 365,
        residualVariance: fit.residualVariance,
      })

      nextPredictions[row.cohort_id] = {
        L: fit.L,
        k: fit.k,
        projectedCurve: projection.projectedCurve,
        upperCI: projection.upperCI,
        lowerCI: projection.lowerCI,
      }
    })

    setPredictions(nextPredictions)
  }

  if (!hasRevenueMapping) {
    return null
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
        </div>

        <div className="retention-controls-right">
          <div className="view-toggle">
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

      {hasNoSelectedRevenueEvents && <p className="error">No revenue events selected. Monetization will show 0.</p>}
      {error && <p className="error">{error}</p>}
      {hasCustomOverrides && <p className="success">Revenue Modified (custom overrides active)</p>}

      <div className="revenue-config-panel">
        <h3>Revenue Configuration</h3>
        <div className="revenue-config-add">
          <label>
            Add Revenue Event
            <select value={eventToAdd} onChange={(e) => setEventToAdd(e.target.value)}>
              <option value="">Select event</option>
              {addableRevenueEvents.map((eventName) => (
                <option key={eventName} value={eventName}>{eventName}</option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className="button button-secondary"
            onClick={() => {
              if (!eventToAdd) {
                return
              }

              setPendingRevenueConfig((previous) => ({
                ...previous,
                [eventToAdd]: {
                  included: true,
                  override: null,
                },
              }))
              setPendingOverrideInputs((previous) => ({ ...previous, [eventToAdd]: '' }))
              setEventToAdd('')
            }}
            disabled={!eventToAdd}
          >
            Add Revenue Event +
          </button>
        </div>

        <table className="revenue-config-table">
          <thead>
            <tr>
              <th>Event Name</th>
              <th>Include</th>
              <th>Override ($)</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(pendingRevenueConfig).map(([eventName, config]) => {
              const isInvalid = invalidOverrideEvents.includes(eventName)

              return (
                <tr key={eventName}>
                  <td>{eventName}</td>
                  <td>
                    <input
                      type="checkbox"
                      checked={config.included}
                      onChange={(e) => setPendingRevenueConfig((previous) => ({
                        ...previous,
                        [eventName]: {
                          ...previous[eventName],
                          included: e.target.checked,
                        },
                      }))}
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={pendingOverrideInputs[eventName] ?? ''}
                      onChange={(e) => handleOverrideChange(eventName, e.target.value)}
                      placeholder="Leave blank for actual"
                      disabled={!config.included}
                      className={isInvalid ? 'invalid-number-input' : ''}
                    />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>

        <p className="muted-text">(Leave blank to use actual revenue)</p>
        <button
          type="button"
          className="button button-primary"
          onClick={handleApplyRevenueChanges}
          disabled={!canApplyRevenueChanges}
        >
          Modify Revenue
        </button>
      </div>

      {displayRows.length > 0 && viewMode === 'table' && (
        <table>
          <thead>
            <tr>
              <th>Cohort</th>
              <th>Size</th>
              {visibleDayColumns.map((day) => <th key={day}>D{day}</th>)}
              <th>Predicted Revenue ({predictionHorizon}D)</th>
            </tr>
          </thead>
          <tbody>
            {displayRows.map((row) => (
              <tr key={row.cohort_id}>
                <td>{row.cohort_name}</td>
                <td>{row.size}</td>
                {visibleDayColumns.map((day) => <td key={day}>{row.displayValues[String(day)]}</td>)}
                <td>{formatCurrency(predictions?.[row.cohort_id]?.projectedCurve?.[predictionHorizon])}</td>
              </tr>
            ))}
          </tbody>
        </table>
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
    </section>
  )
}
