import { useEffect, useMemo, useRef, useState } from 'react'
import { applyFilters, getColumns, getColumnValues, getDateRange, getScope } from '../api'
import SearchableSelect from './SearchableSelect'
import { formatPrettyDate } from '../utils/date'

const OPERATOR_ORDER = ['IN', 'NOT IN', '=', '!=', '>', '>=', '<', '<=']

const TYPE_OPERATOR_MAP = {
  TEXT: ['IN', 'NOT IN', '=', '!='],
  NUMERIC: ['IN', 'NOT IN', '=', '!=', '>', '>=', '<', '<='],
  TIMESTAMP: ['before', 'after', 'on', 'between', 'in', 'not in'],
}

const defaultFilter = {
  column: '',
  operator: 'IN',
  value: [],
  enabled: true,
const isTimestampOperator = (operator) => ['before', 'after', 'on', 'between'].includes(String(operator || '').toLowerCase())

const normalizeColumnType = (dataType = '') => {
  const upper = String(dataType).toUpperCase()
  if (upper.includes('TIMESTAMP') || upper === 'DATE') return 'TIMESTAMP'
  if (['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'FLOAT', 'REAL', 'DOUBLE', 'DECIMAL'].includes(upper) || upper.startsWith('DECIMAL')) return 'NUMERIC'
  return 'TEXT'
}

const normalizeRowValue = (operator, value) => {
  const opUpper = String(operator || '').toUpperCase()
  const requiresMulti = opUpper === 'IN' || opUpper === 'NOT IN'
  
  if (requiresMulti) {
    if (Array.isArray(value)) return value.map((item) => String(item))
    if (value === '' || value === null || value === undefined) return []
    // Fix 4: Preserve date when switching from structured to multi-select
    if (typeof value === 'object' && value !== null) {
      return value.date ? [String(value.date)] : (value.startDate ? [String(value.startDate)] : [])
    }
    return [String(value)]
  }

  const op = String(operator || '').toLowerCase()
  if (isTimestampOperator(op)) {
    if (op === 'between') {
      const source = (value && typeof value === 'object' && !Array.isArray(value)) ? value : {}
      return {
        startDate: source.startDate || '',
        endDate: source.endDate || '',
        startTime: source.startTime || '',
        endTime: source.endTime || '',
      }
    }
    if (op === 'on') {
      const source = (value && typeof value === 'object' && !Array.isArray(value)) ? value : {}
      return { date: source.date || '' }
    }
    const source = (value && typeof value === 'object' && !Array.isArray(value)) ? value : {}
    return { date: source.date || '', time: source.time || '' }
  }

  if (Array.isArray(value)) return value.length > 0 ? String(value[0]) : ''
  if (value === '' || value === null || value === undefined) return ''
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
  const startDateInputRef = useRef(null)
  const endDateInputRef = useRef(null)

  const columnByName = useMemo(() => Object.fromEntries(columns.map((column) => [column.name, column])), [columns])

  const getAllowedOperators = (columnName, map = columnByName) => {
    if (!columnName || !map[columnName]) return OPERATOR_ORDER
    const type = normalizeColumnType(map[columnName]?.data_type)
    return TYPE_OPERATOR_MAP[type] || OPERATOR_ORDER
  }

  const ensureColumnValuesLoaded = async (columnName) => {
    if (!columnName || valueCache[columnName]) return
    try {
      const response = await getColumnValues(columnName)
      setValueCache((prev) => ({ ...prev, [columnName]: { values: (response.values || []).map((value) => String(value)), total_distinct: Number(response.total_distinct || 0) } }))
    } catch {
      setValueCache((prev) => ({ ...prev, [columnName]: { values: [], total_distinct: 0 } }))
    }
  }

  const loadMetadata = async () => {
    try {
      const [columnResponse, scopeResponse] = await Promise.all([getColumns(), getScope()])
      const rawColumns = columnResponse.columns || []
      const loadedColumns = rawColumns.filter((column) => column.category !== 'metric')
      const nextColumnsSignature = JSON.stringify(loadedColumns.map((column) => ({ name: String(column.name || ''), data_type: String(column.data_type || ''), role: column.role ? String(column.role) : '' })))
      const hasColumnMetadataChanged = previousColumnsSignatureRef.current !== nextColumnsSignature
      if (hasColumnMetadataChanged) setValueCache({})
      previousColumnsSignatureRef.current = nextColumnsSignature

      const loadedColumnMap = Object.fromEntries(loadedColumns.map((column) => [column.name, column]))
      setColumns(loadedColumns)

      const payload = scopeResponse.filters_json || { date_range: null, filters: [] }
      const hasDateRange = Boolean(payload.date_range?.start && payload.date_range?.end)

      if (hasDateRange) {
        setDateRange({ start: payload.date_range.start, end: payload.date_range.end })
      } else {
        try {
          const range = await getDateRange()
          setDateRange({ start: range.min_date || '', end: range.max_date || '' })
        } catch {
          setDateRange({ start: '', end: '' })
        }
      }

      const mappedFilters = payload.filters?.length
        ? payload.filters.map((row) => {
          const allowed = getAllowedOperators(row.column, loadedColumnMap)
          const rowOperator = normalizeColumnType(loadedColumnMap[row.column]?.data_type) === 'TIMESTAMP' ? String(row.operator || '').toLowerCase() : row.operator
          const nextOperator = allowed.includes(rowOperator) ? rowOperator : allowed[0]
          return { ...row, operator: nextOperator, enabled: row.enabled ?? true, value: normalizeRowValue(nextOperator, row.value) }
        })
        : [defaultFilter]

      onFiltersApplied?.(payload.filters || [], { skipStale: true })

      setFilters(mappedFilters)
      const columnsToLoad = [...new Set(mappedFilters.map((row) => row.column).filter(Boolean))]
      await Promise.all(columnsToLoad.map((columnName) => ensureColumnValuesLoaded(columnName)))

      const totalRows = Number(scopeResponse.total_rows || 0)
      const filteredRows = Number(scopeResponse.filtered_rows || 0)
      setSummary({ total_rows: totalRows, filtered_rows: filteredRows, percentage: totalRows > 0 ? (filteredRows / totalRows) * 100 : 0 })
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
      const updated = { ...next[index], [key]: value }
      if (key === 'column') {
        const allowed = getAllowedOperators(value)
        updated.operator = allowed.includes(updated.operator) ? updated.operator : allowed[0]
        // Reset value when column changes to prevent cross-property leakage
        updated.value = normalizeRowValue(updated.operator, null)
      }
      if (key === 'operator') updated.value = normalizeRowValue(value, updated.value)
      next[index] = updated
      return next
    })
  }

  const addFilterValue = (index, valueToAdd) => {
    if (!valueToAdd) return
    const normalizedValue = typeof valueToAdd === 'object' ? String(valueToAdd?.value ?? '') : String(valueToAdd)
    if (!normalizedValue || normalizedValue === '[object Object]') return
    setFilters((prev) => {
      const next = [...prev]
      const current = next[index]
      if (!current) return prev
      const opUpper = String(current.operator || '').toUpperCase()
      const isMulti = opUpper === 'IN' || opUpper === 'NOT IN'
      const selectedValues = Array.isArray(current.value) ? current.value.map((value) => String(value)) : current.value ? [String(current.value)] : []
      if (selectedValues.includes(normalizedValue)) return prev
      next[index] = { ...current, value: isMulti ? [...selectedValues, normalizedValue] : normalizedValue }
      return next
    })
  }

  const removeFilterValue = (index, valueToRemove) => {
    const normalizedValue = String(valueToRemove)
    setFilters((prev) => {
      const next = [...prev]
      const current = next[index]
      if (!current) return prev
      const selectedValues = Array.isArray(current.value) ? current.value.map((value) => String(value)) : current.value ? [String(current.value)] : []
      const remainingValues = selectedValues.filter((value) => value !== normalizedValue)
      const opUpper = String(current.operator || '').toUpperCase()
      const isMulti = opUpper === 'IN' || opUpper === 'NOT IN'
      next[index] = { ...current, value: isMulti ? remainingValues : (remainingValues[0] || '') }
      return next
    })
  }

  const toPayload = () => ({
    date_range: dateRange.start && dateRange.end ? dateRange : null,
    filters: filters
      .filter((row) => row.enabled)
      .filter((row) => {
        if (!row.column || !row.operator) return false
        const opUpper = String(row.operator || '').toUpperCase()
        const isMulti = opUpper === 'IN' || opUpper === 'NOT IN'
        if (isMulti) return Array.isArray(row.value) && row.value.length > 0
        if (['BEFORE', 'AFTER', 'ON', 'BETWEEN'].includes(opUpper)) {
          if (opUpper === 'BETWEEN') return Boolean(row.value?.startDate && row.value?.endDate)
          return Boolean(row.value?.date)
        }
        return row.value !== '' && row.value !== undefined && row.value !== null
      })
      .map((row) => {
        const opUpper = String(row.operator || '').toUpperCase()
        const isMulti = opUpper === 'IN' || opUpper === 'NOT IN'
        const isStructuredTs = ['BEFORE', 'AFTER', 'ON', 'BETWEEN'].includes(opUpper)

        return {
          ...row,
          operator: opUpper,
          value: isMulti 
            ? (Array.isArray(row.value) ? row.value.map(v => String(v)) : [String(row.value)])
            : (isStructuredTs ? row.value : String(row.value || ''))
        }
      }),
  })

  const handleApply = async () => {
    setLoading(true)
    setError('')
    try {
      const payload = toPayload()
      const response = await applyFilters(payload)
      setSummary(response)
      onFiltersApplied?.(payload.filters || [])
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
      const [response, range] = await Promise.all([applyFilters({ date_range: null, filters: [] }), getDateRange()])
      setSummary(response)
      setDateRange({ start: range.min_date || '', end: range.max_date || '' })
      onFiltersApplied?.([])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const activeFilterCount = filters.filter((row) => row.enabled && row.column && (Array.isArray(row.value) ? row.value.length > 0 : row.value !== '')).length

  // No-op or logic removed for parent-derived filter count

  return (
    <section className="card">
      <p className="secondary-text">Filtered to {summary.filtered_rows} rows out of {summary.total_rows} rows ({summary.percentage.toFixed(2)}%)</p>
      <p className="secondary-text">Active Filters: {activeFilterCount}</p>

      <div className="grid">
        <label>
          Start Date
          <input type="text" readOnly value={dateRange.start ? formatPrettyDate(dateRange.start) : ''} placeholder="1st Jan 2026" onClick={() => startDateInputRef.current?.showPicker?.() || startDateInputRef.current?.click()} />
          <input ref={startDateInputRef} className="hidden-date-input" type="date" value={dateRange.start} onChange={(e) => setDateRange((prev) => ({ ...prev, start: e.target.value }))} />
        </label>
        <label>
          End Date
          <input type="text" readOnly value={dateRange.end ? formatPrettyDate(dateRange.end) : ''} placeholder="1st Jan 2026" onClick={() => endDateInputRef.current?.showPicker?.() || endDateInputRef.current?.click()} />
          <input ref={endDateInputRef} className="hidden-date-input" type="date" value={dateRange.end} onChange={(e) => setDateRange((prev) => ({ ...prev, end: e.target.value }))} />
        </label>
      </div>

      {filters.map((row, index) => {
        const fetchedValues = (valueCache[row.column]?.values || []).map((value) => String(value))
        const isTimestamp = normalizeColumnType(columnByName[row.column]?.data_type) === 'TIMESTAMP'
        const selectedValues = Array.isArray(row.value) ? row.value.map((value) => String(value)) : row.value ? [String(row.value)] : []
        const mergedValues = Array.from(new Set([...fetchedValues, ...selectedValues]))
        const availableValues = mergedValues.filter((value) => !selectedValues.includes(value))

        return (
          <div key={index} className={`filter-block ${row.enabled ? '' : 'filter-disabled'}`}>
            <h4>Filter #{index + 1}</h4>
            <div className="filter-header-row">
              <label className="inline-checkbox filter-enabled-toggle"><input type="checkbox" checked={row.enabled} onChange={() => updateFilter(index, 'enabled', !row.enabled)} /></label>
              <SearchableSelect
                options={columns.map((column) => {
                  const totalDistinct = Number(valueCache[column.name]?.total_distinct || 0)
                  const labelSuffix = totalDistinct > 0 ? ` (${totalDistinct} values)` : ''
                  const roleLabel = column.role ? ` (${column.role})` : ''
                  return { label: `${column.name}${roleLabel}${labelSuffix}`, value: column.name }
                })}
                value={row.column}
                onChange={(nextColumn) => {
                  updateFilter(index, 'column', nextColumn)
                  ensureColumnValuesLoaded(nextColumn)
                }}
                placeholder="Select column"
              />
              <select value={row.operator} onChange={(e) => updateFilter(index, 'operator', e.target.value)} disabled={!row.column}>
                {getAllowedOperators(row.column).map((operator) => <option key={operator} value={operator}>{operator}</option>)}
              </select>
              {filters.length > 1 && <button className="filter-remove" type="button" onClick={() => setFilters((prev) => prev.filter((_, i) => i !== index))}>X</button>}
            </div>

            <p className="tertiary-label">Values:</p>
            {isTimestamp && isTimestampOperator(row.operator) && !isMultiOperator(row.operator) ? (
              <div className="grid">
                {(row.operator === 'before' || row.operator === 'after') && (
                  <>
                    <input type="date" value={row.value?.date || ''} onChange={(e) => updateFilter(index, 'value', { ...(row.value || {}), date: e.target.value })} />
                    <input type="time" step="1" value={row.value?.time || ''} onChange={(e) => updateFilter(index, 'value', { ...(row.value || {}), time: e.target.value })} />
                  </>
                )}
                {row.operator === 'on' && (
                  <input type="date" value={row.value?.date || ''} onChange={(e) => updateFilter(index, 'value', { date: e.target.value })} />
                )}
                {row.operator === 'between' && (
                  <>
                    <input type="date" value={row.value?.startDate || ''} onChange={(e) => updateFilter(index, 'value', { ...(row.value || {}), startDate: e.target.value })} />
                    <input type="time" step="1" value={row.value?.startTime || ''} onChange={(e) => updateFilter(index, 'value', { ...(row.value || {}), startTime: e.target.value })} />
                    <input type="date" value={row.value?.endDate || ''} onChange={(e) => updateFilter(index, 'value', { ...(row.value || {}), endDate: e.target.value })} />
                    <input type="time" step="1" value={row.value?.endTime || ''} onChange={(e) => updateFilter(index, 'value', { ...(row.value || {}), endTime: e.target.value })} />
                  </>
                )}
              </div>
            ) : (
              <>
                <div className="filter-values">
                  {selectedValues.map((value) => (
                    <span className="filter-chip" key={`${row.column}-${value}`}>{value}<button className="chip-remove" type="button" onClick={() => removeFilterValue(index, value)}>×</button></span>
                  ))}
                </div>

                <div className="filter-selector-spacing">
                  <SearchableSelect
                    options={availableValues}
                    value=""
                    onChange={(selectedValue) => addFilterValue(index, selectedValue)}
                    placeholder={row.column ? 'Search values to add' : 'Select a column first'}
                    disabled={!row.column}
                    column={row.column}
                  />
                </div>
              </>
            )}
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
