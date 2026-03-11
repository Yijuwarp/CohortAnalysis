import { useEffect, useState } from 'react'
import { getRetention, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'
import RetentionGraph from './RetentionGraph'

const MAX_DAY_DETECTION_WINDOW = 365

export default function RetentionTable({ refreshToken, retentionEvent, onRetentionEventChange, maxDay, setMaxDay, showGlobalControls = true }) {
  const [hasInitializedMaxDay, setHasInitializedMaxDay] = useState(false)
  const [isPinned, setIsPinned] = useState(true)
  const [events, setEvents] = useState([])
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [includeCI, setIncludeCI] = useState(false)
  const [confidence, setConfidence] = useState(0.95)
  const [viewMode, setViewMode] = useState('table')

  const loadRetention = async (overrideMaxDay) => {
    setLoading(true)
    setError('')

    try {
      const response = await getRetention(Number(overrideMaxDay ?? maxDay), retentionEvent, includeCI, confidence)
      setData(response.retention_table)
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
    loadRetention(MAX_DAY_DETECTION_WINDOW) // Fetch more initially to detect the true maxDay
    setHasInitializedMaxDay(false)
  }, [refreshToken, retentionEvent])

  useEffect(() => {
    loadRetention()
  }, [retentionEvent, maxDay, includeCI, confidence])

  useEffect(() => {
    if (!data.length) {
      return
    }

    const allUsersRow = data.find((row) => row.cohort_name === 'All Users')
    if (!allUsersRow || !allUsersRow.retention) {
      return
    }

    let lastNonZero = 0
    Object.entries(allUsersRow.retention).forEach(([day, value]) => {
      const numeric = Number(value)
      if (!Number.isNaN(numeric) && numeric > 0) {
        lastNonZero = Math.max(lastNonZero, Number(day))
      }
    })

    const computedMaxDay = Math.max(1, lastNonZero)

    if (!hasInitializedMaxDay && computedMaxDay > 0) {
      setMaxDay(computedMaxDay)
      setHasInitializedMaxDay(true)
    }
  }, [data, maxDay, hasInitializedMaxDay])

  const dayColumns = Array.from({ length: Number(maxDay) + 1 }, (_, index) => index)

  return (
    <section className="card">
      <h2>Retention</h2>
      <div className="retention-header">
        <div className="retention-controls-left">
          {showGlobalControls && (
            <>
              <label>
                Max Day
                <input
                  type="number"
                  min="0"
                  value={maxDay}
                  onChange={(e) => {
                    setMaxDay(Number(e.target.value))
                  }}
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
            </>
          )}
        </div>

        <div className="retention-controls-right">
          <label className="significance-toggle">
            <span className="stat-icon">σ</span>
            <span>Significance</span>
            <input
              type="checkbox"
              checked={includeCI}
              onChange={(e) => setIncludeCI(e.target.checked)}
            />
          </label>
          {includeCI && (
            <label className="retention-aligned-control">
              CI Level
              <select
                value={confidence}
                onChange={(e) => setConfidence(Number(e.target.value))}
              >
                <option value={0.9}>90%</option>
                <option value={0.95}>95%</option>
                <option value={0.99}>99%</option>
              </select>
            </label>
          )}
          <div className="view-toggle retention-aligned-control">
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

      {viewMode === 'table' && data.length > 0 && (
        <div className="analytics-table table-responsive">
          <table>
            <thead>
              <tr>
                <th className={isPinned ? 'sticky-col sticky-col-cohort' : ''}>Cohort</th>
                <th className={isPinned ? 'sticky-col sticky-col-size' : ''}>Size</th>
                {dayColumns.map((day) => (
                  <th key={day}>D{day}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row) => (
                <tr key={row.cohort_id}>
                  <td
                    className={isPinned ? 'sticky-col sticky-col-cohort' : ''}
                    title={row.cohort_name}
                  >
                    {row.cohort_name}
                  </td>
                  <td className={isPinned ? 'sticky-col sticky-col-size' : ''}>{row.size}</td>
                  {dayColumns.map((day) => {
                    const rawValue = row.retention[String(day)]
                    const hasValue = rawValue !== null && rawValue !== undefined
                    const value = hasValue ? Number(rawValue) : null
                    const ci = row.retention_ci?.[String(day)]

                    return (
                      <td key={day}>
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
        <RetentionGraph data={data} maxDay={maxDay} includeCI={includeCI} />
      )}
    </section>
  )
}
