import { useEffect, useMemo, useState } from 'react'
import { mapColumns } from '../api'

const TYPE_OPTIONS = ['TEXT', 'NUMERIC', 'TIMESTAMP', 'BOOLEAN']

export default function Mapping({ columns, detectedTypes = {}, onMappingComplete }) {
  const [form, setForm] = useState({
    user_id_column: '',
    event_name_column: '',
    event_time_column: '',
    event_count_column: '',
  })
  const [columnTypes, setColumnTypes] = useState({})
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

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
    return errors
  }, [columnTypes, form])

  const updateField = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const updateColumnType = (column, value) => {
    setColumnTypes((prev) => ({ ...prev, [column]: value }))
  }

  const handleSubmit = async () => {
    setLoading(true)
    setError('')
    setMessage('')
    try {
      const payload = {
        ...form,
        event_count_column: form.event_count_column || null,
        column_types: columnTypes,
      }
      const data = await mapColumns(payload)
      setMessage(`Success! Normalized ${data.row_count} rows.`)
      if (onMappingComplete) {
        onMappingComplete()
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

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
      </div>
      {mappingErrors.length > 0 && <p className="error">{mappingErrors.join(' ')}</p>}
      <button className="button button-primary" onClick={handleSubmit} disabled={loading || columns.length === 0 || mappingErrors.length > 0}>
        {loading ? 'Saving...' : 'Map Columns'}
      </button>
      {message && <p className="success">{message}</p>}
      {error && <p className="error">{error}</p>}
    </section>
  )
}
