import { useEffect, useState } from 'react'
import { getRetention, listEvents } from '../api'

export default function RetentionTable({ refreshToken }) {
  const [maxDay, setMaxDay] = useState(7)
  const [retentionEvent, setRetentionEvent] = useState('any')
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

  const dayColumns = Array.from({ length: Number(maxDay) + 1 }, (_, index) => index)

  return (
    <section className="card">
      <h2>5. Retention</h2>
      <div className="inline-controls">
        <label>
          Max Day
          <input type="number" min="0" value={maxDay} onChange={(e) => setMaxDay(e.target.value)} />
        </label>
        <label>
          Retention Event
          <select value={retentionEvent} onChange={(e) => setRetentionEvent(e.target.value)}>
            <option value="any">Any Event</option>
            {events.map((eventName) => (
              <option key={eventName} value={eventName}>
                {eventName}
              </option>
            ))}
          </select>
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
