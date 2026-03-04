import { useEffect, useState } from 'react'
import { getRetention, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'

export default function RetentionTable({ refreshToken, retentionEvent, onRetentionEventChange }) {
  const [maxDay, setMaxDay] = useState(7)
  const [effectiveMaxDay, setEffectiveMaxDay] = useState(7)
  const [userModifiedMaxDay, setUserModifiedMaxDay] = useState(false)
  const [events, setEvents] = useState([])
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [includeCI, setIncludeCI] = useState(false)
  const [confidence, setConfidence] = useState(0.95)

  const loadRetention = async () => {
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
  }, [refreshToken])

  useEffect(() => {
    loadRetention()
  }, [retentionEvent, maxDay, includeCI, confidence])

  useEffect(() => {
    if (userModifiedMaxDay) {
      return
    }

    let lastNonZero = 0
    data.forEach((row) => {
      Object.entries(row.retention || {}).forEach(([day, value]) => {
        const numeric = Number(value)
        if (!Number.isNaN(numeric) && numeric !== 0) {
          lastNonZero = Math.max(lastNonZero, Number(day))
        }
      })
    })

    setEffectiveMaxDay(Math.min(Number(maxDay), lastNonZero))
  }, [data, maxDay, userModifiedMaxDay])

  useEffect(() => {
    if (userModifiedMaxDay) {
      setEffectiveMaxDay(Number(maxDay))
    }
  }, [maxDay, userModifiedMaxDay])

  const dayColumns = Array.from({ length: Number(effectiveMaxDay) + 1 }, (_, index) => index)

  return (
    <section className="card">
      <h2>5. Retention</h2>
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
          Retention Event
          <SearchableSelect
            options={[{ label: 'Any Event', value: 'any' }, ...events]}
            value={retentionEvent}
            onChange={onRetentionEventChange}
            placeholder="Select retention event"
          />
        </label>
        <div className="retention-controls-right">
          <label>
            Significance
            <input
              type="checkbox"
              checked={includeCI}
              onChange={(e) => setIncludeCI(e.target.checked)}
            />
          </label>
          <label>
            CI Level
            <select
              value={confidence}
              onChange={(e) => setConfidence(Number(e.target.value))}
              disabled={!includeCI}
            >
              <option value={0.9}>90%</option>
              <option value={0.95}>95%</option>
              <option value={0.99}>99%</option>
            </select>
          </label>
        </div>
      </div>

      {error && <p className="error">{error}</p>}

      {data.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Cohort</th>
              <th>Size</th>
              {dayColumns.map((day) => (
                <th key={day}>D{day}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row) => (
              <tr key={row.cohort_id}>
                <td>{row.cohort_name}</td>
                <td>{row.size}</td>
                {dayColumns.map((day) => {
                  const rawValue = row.retention[String(day)]
                  const hasValue = rawValue !== null && rawValue !== undefined
                  const value = hasValue ? Number(rawValue) : null
                  const ci = row.retention_ci?.[String(day)]

                  return (
                    <td key={day}>
                      <div className="retention-main">{hasValue ? `${value.toFixed(2)}%` : '—'}</div>
                      {includeCI && ci && ci.lower !== null && ci.upper !== null && (
                        <div className="retention-ci">({Number(ci.lower).toFixed(1)}–{Number(ci.upper).toFixed(1)})</div>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
