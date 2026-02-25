import { useState } from 'react'
import { mapColumns } from '../api'

export default function Mapping({ columns }) {
  const [form, setForm] = useState({
    user_id_column: '',
    event_name_column: '',
    event_time_column: '',
  })
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const updateField = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const handleSubmit = async () => {
    setLoading(true)
    setError('')
    setMessage('')
    try {
      const data = await mapColumns(form)
      setMessage(`Success! Normalized ${data.row_count} rows.`)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="card">
      <h2>2. Map Columns</h2>
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
      </div>
      <button onClick={handleSubmit} disabled={loading || columns.length === 0}>
        {loading ? 'Saving...' : 'Map Columns'}
      </button>
      {message && <p className="success">{message}</p>}
      {error && <p className="error">{error}</p>}
    </section>
  )
}
