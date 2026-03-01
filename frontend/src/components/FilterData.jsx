import { useEffect, useState } from 'react'
import { applyFilters, getColumns, getScope } from '../api'

const defaultFilter = { column: '', operator: '=', value: '' }

export default function FilterData({ refreshToken, onFiltersApplied }) {
  const [columns, setColumns] = useState([])
  const [dateRange, setDateRange] = useState({ start: '', end: '' })
  const [filters, setFilters] = useState([defaultFilter])
  const [summary, setSummary] = useState({ total_rows: 0, filtered_rows: 0, percentage: 0 })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const loadMetadata = async () => {
    try {
      const [columnResponse, scopeResponse] = await Promise.all([getColumns(), getScope()])
      setColumns(columnResponse.columns || [])
      const payload = scopeResponse.filters_json || { date_range: null, filters: [] }
      setDateRange({
        start: payload.date_range?.start || '',
        end: payload.date_range?.end || '',
      })
      setFilters(payload.filters?.length ? payload.filters.map((row) => ({ ...row, value: Array.isArray(row.value) ? row.value.join(',') : row.value })) : [defaultFilter])
      const totalRows = Number(scopeResponse.total_rows || 0)
      const filteredRows = Number(scopeResponse.filtered_rows || 0)
      setSummary({
        total_rows: totalRows,
        filtered_rows: filteredRows,
        percentage: totalRows > 0 ? (filteredRows / totalRows) * 100 : 0,
      })
    } catch {
      setColumns([])
    }
  }

  useEffect(() => {
    loadMetadata()
  }, [refreshToken])

  const updateFilter = (index, key, value) => {
    setFilters((prev) => {
      const next = [...prev]
      next[index] = { ...next[index], [key]: value }
      return next
    })
  }

  const toPayload = () => ({
    date_range: dateRange.start && dateRange.end ? dateRange : null,
    filters: filters
      .filter((row) => row.column && row.value !== '')
      .map((row) => {
        if (row.operator === 'IN' || row.operator === 'NOT IN') {
          return { ...row, value: String(row.value).split(',').map((part) => part.trim()).filter(Boolean) }
        }
        return row
      }),
  })

  const handleApply = async () => {
    setLoading(true)
    setError('')
    try {
      const response = await applyFilters(toPayload())
      setSummary(response)
      onFiltersApplied?.()
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleReset = async () => {
    setDateRange({ start: '', end: '' })
    setFilters([defaultFilter])
    setLoading(true)
    setError('')
    try {
      const response = await applyFilters({ date_range: null, filters: [] })
      setSummary(response)
      onFiltersApplied?.()
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="card">
      <h2>3. Filter Data</h2>
      <p>
        Filtered to {summary.filtered_rows} rows out of {summary.total_rows} rows ({summary.percentage.toFixed(2)}%)
      </p>

      <div className="grid">
        <label>
          Start Date
          <input type="date" value={dateRange.start} onChange={(e) => setDateRange((prev) => ({ ...prev, start: e.target.value }))} />
        </label>
        <label>
          End Date
          <input type="date" value={dateRange.end} onChange={(e) => setDateRange((prev) => ({ ...prev, end: e.target.value }))} />
        </label>
      </div>

      {filters.map((row, index) => (
        <div key={index} className="condition-row" style={{ marginBottom: '0.5rem' }}>
          <select value={row.column} onChange={(e) => updateFilter(index, 'column', e.target.value)}>
            <option value="">Select column</option>
            {columns.map((column) => (
              <option key={column.name} value={column.name}>
                {column.role ? `${column.name} (${column.role})` : column.name}
              </option>
            ))}
          </select>
          <select value={row.operator} onChange={(e) => updateFilter(index, 'operator', e.target.value)}>
            <option value="=">=</option>
            <option value="!=">!=</option>
            <option value=">">&gt;</option>
            <option value=">=">&gt;=</option>
            <option value="<">&lt;</option>
            <option value="<=">&lt;=</option>
            <option value="IN">IN</option>
            <option value="NOT IN">NOT IN</option>
          </select>
          <input
            value={row.value}
            placeholder={row.operator.includes('IN') ? 'comma,separated,values' : 'value'}
            onChange={(e) => updateFilter(index, 'value', e.target.value)}
          />
          {filters.length > 1 && (
            <button type="button" onClick={() => setFilters((prev) => prev.filter((_, i) => i !== index))}>
              Remove
            </button>
          )}
        </div>
      ))}

      <div className="inline-controls">
        <button type="button" onClick={() => setFilters((prev) => [...prev, defaultFilter])}>+ Add Filter</button>
        <button type="button" onClick={handleApply} disabled={loading}>{loading ? 'Applying...' : 'Apply Filters'}</button>
        <button type="button" onClick={handleReset} disabled={loading}>Reset Filters</button>
      </div>
      {error && <p className="error">{error}</p>}
    </section>
  )
}
