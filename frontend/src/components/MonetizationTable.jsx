import { useEffect, useMemo, useState } from 'react'
import { getMonetization, getRevenueEvents } from '../api'
import { buildMonetizationRows } from '../monetization'

const METRIC_OPTIONS = [
  { value: 'total_revenue', label: 'Total Revenue' },
  { value: 'cumulative_revenue', label: 'Cumulative Revenue' },
  { value: 'revenue_per_acquired_user', label: 'Revenue per Acquired User' },
  { value: 'cumulative_revenue_per_acquired_user', label: 'Cumulative Revenue per Acquired User' },
  { value: 'revenue_per_retained_user', label: 'Revenue per Retained User' },
]

const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 })

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-'
  }
  return usd.format(Number(value))
}

export default function MonetizationTable({ refreshToken }) {
  const [maxDay, setMaxDay] = useState(7)
  const [effectiveMaxDay, setEffectiveMaxDay] = useState(7)
  const [userModifiedMaxDay, setUserModifiedMaxDay] = useState(false)
  const [metricType, setMetricType] = useState('cumulative_revenue_per_acquired_user')
  const [revenueRows, setRevenueRows] = useState([])
  const [cohortSizes, setCohortSizes] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [hasRevenueMapping, setHasRevenueMapping] = useState(false)
  const [hasNoSelectedRevenueEvents, setHasNoSelectedRevenueEvents] = useState(false)

  const loadRevenueConfig = async () => {
    try {
      const payload = await getRevenueEvents()
      const events = payload.events || []
      setHasRevenueMapping(Boolean(payload.has_revenue_mapping))
      setHasNoSelectedRevenueEvents(Boolean(payload.has_revenue_mapping) && events.length > 0 && events.every((event) => !event.is_included))
    } catch {
      setHasRevenueMapping(false)
      setHasNoSelectedRevenueEvents(false)
    }
  }

  const loadData = async () => {
    setLoading(true)
    setError('')
    try {
      const response = await getMonetization(Number(maxDay))
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

  const displayRows = useMemo(() => buildMonetizationRows({
    cohortSizes,
    retainedRows,
    revenueRows,
    dayColumns,
    metricType,
    formatCurrency,
  }), [cohortSizes, dayColumns, metricType, retainedRows, revenueRows])

  useEffect(() => {
    if (userModifiedMaxDay) {
      return
    }

    let lastNonZero = 0
    displayRows.forEach((row) => {
      Object.entries(row.values || {}).forEach(([day, value]) => {
        const numeric = Number(value)
        if (!Number.isNaN(numeric) && numeric !== 0) {
          lastNonZero = Math.max(lastNonZero, Number(day))
        }
      })
    })

    setEffectiveMaxDay(Math.min(Number(maxDay), lastNonZero))
  }, [displayRows, maxDay, userModifiedMaxDay])

  useEffect(() => {
    if (userModifiedMaxDay) {
      setEffectiveMaxDay(Number(maxDay))
    }
  }, [maxDay, userModifiedMaxDay])

  const visibleDayColumns = useMemo(
    () => Array.from({ length: Number(effectiveMaxDay) + 1 }, (_, idx) => idx),
    [effectiveMaxDay]
  )

  if (!hasRevenueMapping) {
    return null
  }

  return (
    <section className="card">
      <h2>7. Monetization</h2>
      <div className="inline-controls">
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
          <select value={metricType} onChange={(e) => setMetricType(e.target.value)}>
            {METRIC_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <button className="button button-primary" onClick={loadData} disabled={loading}>
          {loading ? 'Loading...' : 'Load Monetization'}
        </button>
      </div>

      {hasNoSelectedRevenueEvents && <p className="error">No revenue events selected. Monetization will show 0.</p>}
      {error && <p className="error">{error}</p>}

      {displayRows.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Cohort</th>
              <th>Size</th>
              {visibleDayColumns.map((day) => <th key={day}>D{day}</th>)}
            </tr>
          </thead>
          <tbody>
            {displayRows.map((row) => (
              <tr key={row.cohort_id}>
                <td>{row.cohort_name}</td>
                <td>{row.size}</td>
                {visibleDayColumns.map((day) => <td key={day}>{row.values[String(day)]}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
