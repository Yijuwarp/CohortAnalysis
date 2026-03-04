import { useEffect, useMemo, useState } from 'react'
import { getRevenueEvents, mapColumns, updateRevenueEvents } from '../api'

const TYPE_OPTIONS = ['TEXT', 'NUMERIC', 'TIMESTAMP', 'BOOLEAN']

export default function Mapping({ columns, detectedTypes = {}, onMappingComplete }) {
  const [form, setForm] = useState({
    user_id_column: '',
    event_name_column: '',
    event_time_column: '',
    event_count_column: '',
    revenue_column: '',
  })
  const [columnTypes, setColumnTypes] = useState({})
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [revenueEvents, setRevenueEvents] = useState([])

  useEffect(() => {
    const initial = {}
    columns.forEach((column) => {
      initial[column] = detectedTypes[column] || 'TEXT'
    })
    setColumnTypes(initial)
  }, [columns, detectedTypes])

  const mappingErrors = useMemo(() => {
    const errors = []
    if (form.user_id_column && columnTypes[form.user_id_column] !== 'TEXT') {
      errors.push('user_id mapping requires TEXT column type.')
    }
    if (form.event_name_column && columnTypes[form.event_name_column] !== 'TEXT') {
      errors.push('event_name mapping requires TEXT column type.')
    }
    if (form.event_time_column && columnTypes[form.event_time_column] !== 'TIMESTAMP') {
      errors.push('event_time mapping requires TIMESTAMP column type.')
    }
    if (form.event_count_column && columnTypes[form.event_count_column] !== 'NUMERIC') {
      errors.push('event_count mapping requires NUMERIC column type.')
    }
    if (form.revenue_column && columnTypes[form.revenue_column] !== 'NUMERIC') {
      errors.push('revenue mapping requires NUMERIC column type.')
    }
    return errors
  }, [columnTypes, form])

  const updateField = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const updateColumnType = (column, value) => {
    setColumnTypes((prev) => ({ ...prev, [column]: value }))
  }

  const loadRevenueEvents = async () => {
    try {
      const response = await getRevenueEvents()
      setRevenueEvents(response.events || [])
    } catch {
      setRevenueEvents([])
    }
  }

  const toggleRevenueEvent = async (eventName, isIncluded) => {
    const nextEvents = revenueEvents.map((item) => (
      item.event_name === eventName ? { ...item, is_included: isIncluded } : item
    ))
    setRevenueEvents(nextEvents)
    try {
      const updated = await updateRevenueEvents(nextEvents)
      setRevenueEvents(updated.events || [])
    } catch (err) {
      setError(err.message)
    }
  }

  const handleSubmit = async () => {
    setLoading(true)
    setError('')
    setMessage('')
    try {
      const payload = {
        ...form,
        event_count_column: form.event_count_column || null,
        revenue_column: form.revenue_column || null,
        column_types: columnTypes,
      }
      const data = await mapColumns(payload)
      setMessage(`Success! Normalized ${data.row_count} rows.`)
      if (payload.revenue_column) {
        await loadRevenueEvents()
      } else {
        setRevenueEvents([])
      }
      if (onMappingComplete) {
        onMappingComplete()
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const hasNoSelectedRevenueEvents = revenueEvents.length > 0 && revenueEvents.every((event) => !event.is_included)

  return (
    <section className="card">
      <h2>2. Review Schema & Map Columns</h2>
      <div style={{ maxHeight: 240, overflow: 'auto', marginBottom: 16 }}>
        <table className="retention-table">
          <thead>
            <tr>
              <th>Column</th>
              <th>Detected Type</th>
              <th>Override</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((column) => (
              <tr key={column}>
                <td>{column}</td>
                <td>{detectedTypes[column] || 'TEXT'}</td>
                <td>
                  <select value={columnTypes[column] || 'TEXT'} onChange={(e) => updateColumnType(column, e.target.value)}>
                    {TYPE_OPTIONS.map((type) => (
                      <option key={type} value={type}>{type}</option>
                    ))}
                  </select>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="grid">
        <label>
          User ID
          <select value={form.user_id_column} onChange={(e) => updateField('user_id_column', e.target.value)}>
            <option value="">Select column</option>
            {columns.map((column) => (
              <option key={column} value={column}>{column}</option>
            ))}
          </select>
        </label>
        <label>
          Event Name
          <select value={form.event_name_column} onChange={(e) => updateField('event_name_column', e.target.value)}>
            <option value="">Select column</option>
            {columns.map((column) => (
              <option key={column} value={column}>{column}</option>
            ))}
          </select>
        </label>
        <label>
          Event Time
          <select value={form.event_time_column} onChange={(e) => updateField('event_time_column', e.target.value)}>
            <option value="">Select column</option>
            {columns.map((column) => (
              <option key={column} value={column}>{column}</option>
            ))}
          </select>
        </label>
        <label>
          Event Count (optional)
          <select value={form.event_count_column} onChange={(e) => updateField('event_count_column', e.target.value)}>
            <option value="">None (default = 1)</option>
            {columns.map((column) => (
              <option key={column} value={column}>{column}</option>
            ))}
          </select>
        </label>
        <label>
          Revenue Column (optional)
          <select value={form.revenue_column} onChange={(e) => updateField('revenue_column', e.target.value)}>
            <option value="">None (default = 0)</option>
            {columns.map((column) => (
              <option key={column} value={column}>{column}</option>
            ))}
          </select>
        </label>
      </div>
      {mappingErrors.length > 0 && <p className="error">{mappingErrors.join(' ')}</p>}
      <button className="button button-primary" onClick={handleSubmit} disabled={loading || columns.length === 0 || mappingErrors.length > 0}>
        {loading ? 'Saving...' : 'Map Columns'}
      </button>
      {message && <p className="success">{message}</p>}
      {error && <p className="error">{error}</p>}

      {form.revenue_column && revenueEvents.length > 0 && (
        <div style={{ marginTop: 16 }}>
          <h3>Revenue Events</h3>
          {revenueEvents.map((event) => (
            <label key={event.event_name} style={{ display: 'block' }}>
              <input
                type="checkbox"
                checked={event.is_included}
                onChange={(e) => toggleRevenueEvent(event.event_name, e.target.checked)}
              />
              {' '}
              {event.event_name}
            </label>
          ))}
          {hasNoSelectedRevenueEvents && (
            <p className="error">No revenue events selected. Monetization will show 0.</p>
          )}
        </div>
      )}
    </section>
  )
}
