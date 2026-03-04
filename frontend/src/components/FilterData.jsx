import { useEffect, useMemo, useRef, useState } from 'react'
import { applyFilters, getColumns, getColumnValues, getDateRange, getScope } from '../api'
import SearchableSelect from './SearchableSelect'
import { formatPrettyDate } from '../utils/date'

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
  if (value === '' || value === null || value === undefined) {
    return ''
  }
  return String(value)
}

export default function FilterData({ refreshToken, onFiltersApplied }) {
  const [columns, setColumns] = useState([])
  const [dateRange, setDateRange] = useState({ start: '', end: '' })
  const [filters, setFilters] = useState([defaultFilter])
  const [summary, setSummary] = useState({ total_rows: 0, filtered_rows: 0, percentage: 0 })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [valueCache, setValueCache] = useState({})
  const previousColumnsSignatureRef = useRef('')

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
          values: (response.values || []).map((value) => String(value)),
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
      const [columnResponse, scopeResponse] = await Promise.all([getColumns(), getScope()])
      const loadedColumns = columnResponse.columns || []
      const nextColumnsSignature = JSON.stringify(
        loadedColumns.map((column) => ({
          name: String(column.name || ''),
          data_type: String(column.data_type || ''),
          role: column.role ? String(column.role) : '',
        }))
      )
      const hasColumnMetadataChanged = previousColumnsSignatureRef.current !== nextColumnsSignature
      if (hasColumnMetadataChanged) {
        setValueCache({})
      }
      previousColumnsSignatureRef.current = nextColumnsSignature

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

  const addFilterValue = (index, valueToAdd) => {
    const normalizedValue = String(valueToAdd)
    setFilters((prev) => {
      const next = [...prev]
      const current = next[index]
      if (!current) {
        return prev
      }
      const isMulti = current.operator === 'IN' || current.operator === 'NOT IN'
      const selectedValues = Array.isArray(current.value)
        ? current.value.map((value) => String(value))
        : current.value
          ? [String(current.value)]
          : []
      if (selectedValues.includes(normalizedValue)) {
        return prev
      }

      next[index] = {
        ...current,
        value: isMulti ? [...selectedValues, normalizedValue] : normalizedValue,
      }
      return next
    })
  }

  const removeFilterValue = (index, valueToRemove) => {
    const normalizedValue = String(valueToRemove)
    setFilters((prev) => {
      const next = [...prev]
      const current = next[index]
      if (!current) {
        return prev
      }
      const selectedValues = Array.isArray(current.value)
        ? current.value.map((value) => String(value))
        : current.value
          ? [String(current.value)]
          : []
      const remainingValues = selectedValues.filter((value) => value !== normalizedValue)
      const isMulti = current.operator === 'IN' || current.operator === 'NOT IN'
      next[index] = {
        ...current,
        value: isMulti ? remainingValues : (remainingValues[0] || ''),
      }
      return next
    })
  }

  const toPayload = () => ({
    date_range: dateRange.start && dateRange.end ? dateRange : null,
    filters: filters
      .filter((row) => row.enabled)
      .filter((row) => row.column && (Array.isArray(row.value) ? row.value.length > 0 : row.value !== ''))
      .map((row) => {
        if (Array.isArray(row.value)) {
          return {
            ...row,
            value: row.value.map((item) => String(item)),
          }
        }
        return {
          ...row,
          value: row.value === '' ? '' : String(row.value),
        }
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
          {dateRange.start && <div className="secondary-text">{formatPrettyDate(dateRange.start)}</div>}
        </label>
        <label>
          End Date
          <input type="date" value={dateRange.end} onChange={(e) => setDateRange((prev) => ({ ...prev, end: e.target.value }))} />
          {dateRange.end && <div className="secondary-text">{formatPrettyDate(dateRange.end)}</div>}
        </label>
      </div>

      {filters.map((row, index) => {
        const fetchedValues = (valueCache[row.column]?.values || []).map((value) => String(value))
        const selectedValues = Array.isArray(row.value)
          ? row.value.map((value) => String(value))
          : row.value
            ? [String(row.value)]
            : []
        const mergedValues = Array.from(new Set([...fetchedValues, ...selectedValues]))
        const availableValues = mergedValues.filter((value) => !selectedValues.includes(value))

        return (
          <div key={index} className={`filter-block ${row.enabled ? '' : 'filter-disabled'}`}>
            <div className="filter-header-row">
              <label className="inline-checkbox filter-enabled-toggle">
                <input
                  type="checkbox"
                  checked={row.enabled}
                  onChange={() => updateFilter(index, 'enabled', !row.enabled)}
                />
              </label>
              <SearchableSelect
                options={columns.map((column) => {
                  const totalDistinct = Number(valueCache[column.name]?.total_distinct || 0)
                  const labelSuffix = totalDistinct > 0 ? ` (${totalDistinct} values)` : ''
                  const roleLabel = column.role ? ` (${column.role})` : ''
                  return {
                    label: `${column.name}${roleLabel}${labelSuffix}`,
                    value: column.name,
                  }
                })}
                value={row.column}
                onChange={(nextColumn) => {
                  updateFilter(index, 'column', nextColumn)
                  ensureColumnValuesLoaded(nextColumn)
                }}
                placeholder="Select column"
              />
              <select
                value={row.operator}
                onChange={(e) => {
                  updateFilter(index, 'operator', e.target.value)
                }}
                disabled={!row.column}
              >
                {getAllowedOperators(row.column).map((operator) => (
                  <option key={operator} value={operator}>
                    {operator}
                  </option>
                ))}
              </select>
              {filters.length > 1 && (
                <button
                  className="filter-remove"
                  type="button"
                  onClick={() => {
                    setFilters((prev) => prev.filter((_, i) => i !== index))
                  }}
                >
                  X
                </button>
              )}
            </div>

            <p className="tertiary-label">Values:</p>
            <div className="filter-values">
              {selectedValues.map((value) => (
                <span className="filter-chip" key={`${row.column}-${value}`}>
                  {value}
                  <button className="chip-remove" type="button" onClick={() => removeFilterValue(index, value)}>
                    ×
                  </button>
                </span>
              ))}
            </div>

            <SearchableSelect
              options={availableValues}
              value=""
              onChange={(selectedValue) => addFilterValue(index, selectedValue)}
              placeholder={row.column ? 'Search values to add' : 'Select a column first'}
              disabled={!row.column}
            />
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
