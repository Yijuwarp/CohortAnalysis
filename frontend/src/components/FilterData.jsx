import { useEffect, useMemo, useState } from 'react'
import { applyFilters, getColumns, getColumnValues, getDateRange, getScope } from '../api'

const OPERATOR_ORDER = ['IN', 'NOT IN', '=', '!=', '>', '>=', '<', '<=']

const TYPE_OPERATOR_MAP = {
  TEXT: ['IN', 'NOT IN', '=', '!='],
  NUMERIC: ['IN', 'NOT IN', '=', '!=', '>', '>=', '<', '<='],
  TIMESTAMP: ['=', '!=', '>', '>=', '<', '<='],
}

const defaultFilter = {
  column: '',
  operator: 'IN',
  value: [],
  enabled: true,
}

const normalizeColumnType = (dataType = '') => {
  const upper = String(dataType).toUpperCase()
  if (upper.includes('TIMESTAMP') || upper === 'DATE') {
    return 'TIMESTAMP'
  }
  if (
    [
      'TINYINT',
      'SMALLINT',
      'INTEGER',
      'BIGINT',
      'HUGEINT',
      'UTINYINT',
      'USMALLINT',
      'UINTEGER',
      'UBIGINT',
      'FLOAT',
      'REAL',
      'DOUBLE',
      'DECIMAL',
    ].includes(upper) ||
    upper.startsWith('DECIMAL')
  ) {
    return 'NUMERIC'
  }
  return 'TEXT'
}

const normalizeRowValue = (operator, value) => {
  const requiresMulti = operator === 'IN' || operator === 'NOT IN'
  if (requiresMulti) {
    if (Array.isArray(value)) {
      return value.map((item) => String(item))
    }
    if (value === '' || value === null || value === undefined) {
      return []
    }
    return [String(value)]
  }

  if (Array.isArray(value)) {
    return value.length > 0 ? String(value[0]) : ''
  }
  return value ?? ''
}

export default function FilterData({ refreshToken, onFiltersApplied }) {
  const [columns, setColumns] = useState([])
  const [dateRange, setDateRange] = useState({ start: '', end: '' })
  const [filters, setFilters] = useState([defaultFilter])
  const [summary, setSummary] = useState({ total_rows: 0, filtered_rows: 0, percentage: 0 })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [valueCache, setValueCache] = useState({})

  const columnByName = useMemo(
    () => Object.fromEntries(columns.map((column) => [column.name, column])),
    [columns]
  )

  const getAllowedOperators = (columnName, map = columnByName) => {
    if (!columnName || !map[columnName]) {
      return OPERATOR_ORDER
    }
    const type = normalizeColumnType(map[columnName]?.data_type)
    return TYPE_OPERATOR_MAP[type] || OPERATOR_ORDER
  }

  const ensureColumnValuesLoaded = async (columnName) => {
    if (!columnName || valueCache[columnName]) {
      return
    }

    try {
      const response = await getColumnValues(columnName)
      setValueCache((prev) => ({
        ...prev,
        [columnName]: {
          values: response.values || [],
          total_distinct: Number(response.total_distinct || 0),
        },
      }))
    } catch {
      setValueCache((prev) => ({
        ...prev,
        [columnName]: {
          values: [],
          total_distinct: 0,
        },
      }))
    }
  }

  const loadMetadata = async () => {
    try {
      // Clear stale cached distinct values
      setValueCache({})

      const [columnResponse, scopeResponse] = await Promise.all([getColumns(), getScope()])
      const loadedColumns = columnResponse.columns || []
      const loadedColumnMap = Object.fromEntries(loadedColumns.map((column) => [column.name, column]))
      setColumns(loadedColumns)

      const payload = scopeResponse.filters_json || { date_range: null, filters: [] }
      const hasDateRange = Boolean(payload.date_range?.start && payload.date_range?.end)

      if (hasDateRange) {
        setDateRange({
          start: payload.date_range.start,
          end: payload.date_range.end,
        })
      } else {
        try {
          const range = await getDateRange()
          setDateRange({
            start: range.min_date || '',
            end: range.max_date || '',
          })
        } catch {
          setDateRange({ start: '', end: '' })
        }
      }

      const mappedFilters = payload.filters?.length
        ? payload.filters.map((row) => {
            const allowed = getAllowedOperators(row.column, loadedColumnMap)
            const nextOperator = allowed.includes(row.operator) ? row.operator : allowed[0]
            return {
              ...row,
              operator: nextOperator,
              enabled: row.enabled ?? true,
              value: normalizeRowValue(nextOperator, row.value),
            }
          })
        : [defaultFilter]

      setFilters(mappedFilters)

      const columnsToLoad = [...new Set(mappedFilters.map((row) => row.column).filter(Boolean))]
      await Promise.all(columnsToLoad.map((columnName) => ensureColumnValuesLoaded(columnName)))

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
      const current = next[index]
      const updated = { ...current, [key]: value }

      if (key === 'column') {
        const allowed = getAllowedOperators(value)
        updated.operator = allowed.includes(updated.operator) ? updated.operator : allowed[0]
        updated.value = normalizeRowValue(updated.operator, updated.value)
      }

      if (key === 'operator') {
        updated.value = normalizeRowValue(value, updated.value)
      }

      next[index] = updated
      return next
    })
  }

  const toPayload = () => ({
    date_range: dateRange.start && dateRange.end ? dateRange : null,
    filters: filters
      .filter((row) => row.enabled)
      .filter((row) => row.column && (Array.isArray(row.value) ? row.value.length > 0 : row.value !== '')),
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
    setFilters([defaultFilter])
    setLoading(true)
    setError('')
    try {
      const [response, range] = await Promise.all([
        applyFilters({ date_range: null, filters: [] }),
        getDateRange(),
      ])
      setSummary(response)
      setDateRange({
        start: range.min_date || '',
        end: range.max_date || '',
      })
      onFiltersApplied?.()
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const activeFilterCount = filters.filter(
    (row) => row.enabled && row.column && (Array.isArray(row.value) ? row.value.length > 0 : row.value !== '')
  ).length

  return (
    <section className="card">
      <h2>3. Filter Data</h2>
      <p className="secondary-text">
        Filtered to {summary.filtered_rows} rows out of {summary.total_rows} rows ({summary.percentage.toFixed(2)}%)
      </p>
      <p className="secondary-text">Active Filters: {activeFilterCount}</p>

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

      {filters.map((row, index) => {
        const allowedOperators = getAllowedOperators(row.column)
        const currentValues = valueCache[row.column]?.values || []
        const currentDistinctCount = Number(valueCache[row.column]?.total_distinct || 0)
        const isMulti = row.operator === 'IN' || row.operator === 'NOT IN'
        const truncated = currentDistinctCount > currentValues.length && currentValues.length === 100

        return (
          <div key={index} className="condition-row" style={{ opacity: row.enabled ? 1 : 0.5 }}>
            <label className="inline-checkbox">
              <input
                type="checkbox"
                checked={row.enabled}
                onChange={() => updateFilter(index, 'enabled', !row.enabled)}
              />
              Enabled
            </label>
            <select
              value={row.column}
              onChange={(e) => {
                const nextColumn = e.target.value
                updateFilter(index, 'column', nextColumn)
                ensureColumnValuesLoaded(nextColumn)
              }}
            >
              <option value="">Select column</option>
              {columns.map((column) => {
                const totalDistinct = Number(valueCache[column.name]?.total_distinct || 0)
                const labelSuffix = totalDistinct > 0 ? ` (${totalDistinct} values)` : ''
                const roleLabel = column.role ? ` (${column.role})` : ''
                return (
                  <option key={column.name} value={column.name}>
                    {column.name}
                    {roleLabel}
                    {labelSuffix}
                  </option>
                )
              })}
            </select>
            <select
              value={row.operator}
              onChange={(e) => updateFilter(index, 'operator', e.target.value)}
              disabled={!row.column}
            >
              {OPERATOR_ORDER.map((operator) => (
                <option key={operator} value={operator} disabled={!allowedOperators.includes(operator)}>
                  {operator}
                </option>
              ))}
            </select>
            <select
              value={isMulti ? (Array.isArray(row.value) ? row.value : []) : (row.value || '')}
              multiple={isMulti}
              disabled={!row.column}
              onChange={(e) => {
                if (isMulti) {
                  updateFilter(
                    index,
                    'value',
                    Array.from(e.target.selectedOptions).map((option) => option.value)
                  )
                  return
                }
                updateFilter(index, 'value', e.target.value)
              }}
            >
              {!isMulti && <option value="">Select value</option>}
              {currentValues.map((option) => (
                <option key={`${row.column}-${option}`} value={option}>
                  {option}
                </option>
              ))}
            </select>
            {filters.length > 1 && (
              <button className="button button-danger" type="button" onClick={() => setFilters((prev) => prev.filter((_, i) => i !== index))}>
                Remove
              </button>
            )}
            {truncated && <small className="secondary-text">Showing first 100 of {currentDistinctCount} values</small>}
          </div>
        )
      })}

      <div className="inline-controls">
        <button className="button button-secondary" type="button" onClick={() => setFilters((prev) => [...prev, defaultFilter])}>+ Add Filter</button>
        <button className="button button-primary" type="button" onClick={handleApply} disabled={loading}>{loading ? 'Applying...' : 'Apply Filters'}</button>
        <button className="button button-secondary" type="button" onClick={handleReset} disabled={loading}>Reset Filters</button>
      </div>
      {error && <p className="error">{error}</p>}
    </section>
  )
}
