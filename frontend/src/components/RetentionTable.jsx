import { useEffect, useState } from 'react'
import { getRetention, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'
import RetentionGraph from './RetentionGraph'
import ComparePane from './ComparePane'

function formatNumber(n) {
  return new Intl.NumberFormat().format(n);
}

export default function RetentionTable({ refreshToken, retentionEvent, onRetentionEventChange, maxDay, setMaxDay, showGlobalControls = true, state, setState }) {
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

  useEffect(() => {
    const nextState = {
      isPinned,
      includeCI,
      confidence,
      viewMode,
      isComparePaneOpen,
      mode,
      retentionType
    }
    setState(nextState)
  }, [isPinned, includeCI, confidence, viewMode, isComparePaneOpen, mode, retentionType])

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
          {viewMode === 'table' && data.length > 0 && (
            <div className="analytics-table table-responsive">
              <table>
                <thead>
                  <tr>
                    <th className={isPinned ? 'sticky-col sticky-col-cohort' : ''}>Cohort</th>
                    <th className={isPinned ? 'sticky-col sticky-col-size' : ''}>Size</th>
                    {bucketColumns.map((b) => (
                      <th key={b}>{labelPrefix}{b}</th>
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
                      <td className={isPinned ? 'sticky-col sticky-col-size' : ''}>
                        {formatNumber(row.size)}
                      </td>
                      {bucketColumns.map((b) => {
                        const rawValue = row.retention[String(b)]
                        const hasValue = rawValue !== null && rawValue !== undefined
                        const value = hasValue ? Number(rawValue) : null
                        const ci = row.retention_ci?.[String(b)]

                        return (
                          <td key={b}>
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
