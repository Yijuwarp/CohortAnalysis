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

  const loadRetention = async () => {
    setLoading(true)
    setError('')

    try {
      const response = await getRetention(Number(maxDay), retentionEvent)
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
  }, [retentionEvent, maxDay])

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
        <button className="button button-primary" onClick={loadRetention} disabled={loading}>{loading ? 'Loading...' : 'Load Retention'}</button>
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
                {dayColumns.map((day) => (
                  <td key={day}>{Number(row.retention[String(day)] ?? 0).toFixed(2)}%</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
