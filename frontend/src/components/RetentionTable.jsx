import { useEffect, useState } from 'react'
import { getRetention, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'
import RetentionGraph from './RetentionGraph'
import ComparePane from './ComparePane'

export default function RetentionTable({ refreshToken, retentionEvent, onRetentionEventChange, maxDay, setMaxDay, showGlobalControls = true }) {
  const [isPinned, setIsPinned] = useState(true)
  const [events, setEvents] = useState([])
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [includeCI, setIncludeCI] = useState(false)
  const [confidence, setConfidence] = useState(0.95)
  const [viewMode, setViewMode] = useState('table')
  const [isComparePaneOpen, setIsComparePaneOpen] = useState(false)

  const loadRetention = async (overrideMaxDay) => {
    setLoading(true)
    setError('')

    try {
      const response = await getRetention(Number(maxDay), retentionEvent, includeCI, confidence)
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
    loadRetention()
  }, [refreshToken, retentionEvent, maxDay, includeCI, confidence])



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
          <button
            type="button"
            className={`compare-open-button ${isComparePaneOpen ? 'active' : ''}`}
            onClick={() => setIsComparePaneOpen(prev => !prev)}
            title="Compare two cohorts statistically"
            data-testid="open-compare-pane"
          >
            ⚖ Compare
          </button>
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

      <ComparePane
        isOpen={isComparePaneOpen}
        onClose={() => setIsComparePaneOpen(false)}
        tab="retention"
        maxDay={maxDay}
        defaultMetric="retention_rate"
      />
    </section>
  )
}
