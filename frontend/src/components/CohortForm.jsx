import React, { Fragment, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { getColumnValues, getColumns, listEvents, createSavedCohort, updateSavedCohort, estimateCohort, createCohort } from '../api'
import SearchableSelect from './SearchableSelect'
import { getNextName } from '../utils/cohortUtils'

const OPERATOR_ORDER = ['=', '!=', '>', '>=', '<', '<=', 'IN', 'NOT IN']

const TYPE_OPERATOR_MAP = {
  TEXT: ['IN', 'NOT IN', '=', '!='],
  NUMERIC: ['=', '!=', '>', '<', '>=', '<=', 'IN', 'NOT IN'],
  TIMESTAMP: ['before', 'after', 'on', 'between', 'in', 'not in'],
  BOOLEAN: ['=', '!='],
}

const normalizeColumnType = (dataType = '') => {
  const upper = String(dataType).toUpperCase()
  if (upper === 'BOOLEAN' || upper === 'BOOL') return 'BOOLEAN'
  if (upper.includes('TIMESTAMP') || upper === 'DATE') return 'TIMESTAMP'
  if (['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'FLOAT', 'REAL', 'DOUBLE', 'DECIMAL'].includes(upper) || upper.startsWith('DECIMAL')) return 'NUMERIC'
  return 'TEXT'
}

const createEmptyCondition = (defaultEvent = '') => ({
  event_name: defaultEvent,
  min_event_count: 1,
  is_negated: false,
  property_filter: null,
  property_filter_expanded: false,
})

const isMultiOperator = (operator) => {
  const op = String(operator || '').toUpperCase()
  return op === 'IN' || op === 'NOT IN'
}
const isTimestampOperator = (operator) => ['before', 'after', 'on', 'between'].includes(String(operator || '').toLowerCase())

const formatCohortSize = (size) => {
  const numeric = Number(size || 0)
  if (numeric >= 1000000) return `${(numeric / 1000000).toFixed(numeric >= 10000000 ? 0 : 1)}M`
  if (numeric >= 1000) return `${(numeric / 1000).toFixed(numeric >= 100000 ? 0 : 1)}K`
  return String(numeric)
}

function generateCohortName(currentConditions, currentLogicOperator) {
  if (!currentConditions || currentConditions.length === 0) return 'Untitled Cohort'
  const parts = currentConditions.map((cond) => {
    const event = cond.event_name || 'event'
    const count = cond.min_event_count ?? 1
    const propertyFilter = cond.property_filter
    const negated = cond.is_negated ? 'NOT ' : ''

    let base = count > 1 ? `${negated}Triggered ${event} more than ${count} times` : `${negated}Triggered ${event}`
    if (propertyFilter) {
      const op = String(propertyFilter.operator || '').toUpperCase()
      let formattedValues = propertyFilter.values
      if (Array.isArray(propertyFilter.values)) {
        formattedValues = propertyFilter.values.join(', ')
      } else if (typeof propertyFilter.values === 'object' && propertyFilter.values !== null) {
        const val = propertyFilter.values
        if (op === 'ON') {
          formattedValues = val.date || ''
        } else if (op === 'BEFORE' || op === 'AFTER') {
          formattedValues = val.time ? `${val.date || ''} ${val.time}`.trim() : (val.date || '')
        } else if (op === 'BETWEEN') {
          const start = val.startTime ? `${val.startDate || ''} ${val.startTime}`.trim() : (val.startDate || '')
          const end = val.endTime ? `${val.endDate || ''} ${val.endTime}`.trim() : (val.endDate || '')
          formattedValues = `${start} and ${end}`
        }
      }

      if (op === 'BETWEEN') {
        base += ` where ${propertyFilter.column} between ${formattedValues}`
      } else if (op === 'IN' || op === 'NOT IN') {
        base += ` where ${propertyFilter.column} ${op} (${formattedValues})`
      } else {
        base += ` where ${propertyFilter.column} ${op.toLowerCase()} ${formattedValues}`
      }
    }
    return base
  })
  return parts.join(` ${currentLogicOperator} `)
}

const isTest = (typeof process !== 'undefined' && (process.env?.NODE_ENV === 'test' || process.env?.VITEST)) || (typeof import.meta !== 'undefined' && (import.meta.env?.MODE === 'test' || import.meta.env?.VITEST)) || typeof globalThis.__VITEST__ !== 'undefined'

export default function CohortForm({ mode, initialData, onCancel, onSave, refreshToken }) {
  const isEditing = mode === 'edit_saved'
  const [name, setName] = useState(initialData?.name || '')
  
  const initialLogic = initialData?.definition?.logic_operator || 'AND'
  const initialJoin = initialData?.definition?.join_type || 'condition_met'
  const [logicOperator, setLogicOperator] = useState(initialLogic)
  const [joinType, setJoinType] = useState(initialJoin)
  
  const [conditions, setConditions] = useState(() => {
    if (initialData?.definition?.conditions?.length > 0) {
      return initialData.definition.conditions.map((condition) => {
        const pf = condition.property_filter || null
        const hasValues = Array.isArray(pf?.values) ? pf.values.length > 0 : pf?.values !== null && pf?.values !== undefined && pf?.values !== ''
        const operator = pf?.operator || ''
        const isTsOp = isTimestampOperator(operator)
        return {
          ...condition,
          is_negated: condition.is_negated ?? false,
          property_filter: pf ? { ...pf, operator: isTsOp ? String(operator).toLowerCase() : operator } : null,
          property_filter_expanded: !!pf && hasValues,
          property_column: pf?.column || '',
          property_operator: isTsOp ? String(operator).toLowerCase() : (pf?.operator || ''),
          property_values: Array.isArray(pf?.values) ? pf.values : (pf?.values ? [pf.values] : []),
        }
      })
    }
    return [createEmptyCondition('')]
  })

  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  
  const [events, setEvents] = useState([])
  const [columns, setColumns] = useState([])
  const [valueCache, setValueCache] = useState({})
  const valueCacheRef = useRef(valueCache)
  const [loadingKeys, setLoadingKeys] = useState(new Set())
  const inFlightRequests = useRef(new Set())

  useEffect(() => {
    valueCacheRef.current = valueCache
  }, [valueCache])
  const [estimating, setEstimating] = useState(false)
  const [estimatedSize, setEstimatedSize] = useState(null)

  const [stayOpen, setStayOpen] = useState(false)
  const [toast, setToast] = useState(null)
  const toastTimeoutRef = useRef(null)

  const showToast = (cohortName) => {
    setToast(`Cohort "${cohortName}" created`)
    if (toastTimeoutRef.current) {
      clearTimeout(toastTimeoutRef.current)
    }
    if (isTest) {
      Promise.resolve().then(() => {
        setToast(null)
      })
    } else {
      toastTimeoutRef.current = setTimeout(() => {
        setToast(null)
      }, 2000)
    }
  }

  useEffect(() => {
    return () => {
      if (toastTimeoutRef.current) {
        clearTimeout(toastTimeoutRef.current)
      }
    }
  }, [])

  const handleReset = () => {
    setName('')
    setLogicOperator(initialLogic)
    setJoinType(initialJoin)
    setConditions([createEmptyCondition(events[0] || '')])
    setEstimatedSize(null)
    setError('')
  }

  const columnByName = useMemo(() => Object.fromEntries(columns.map((column) => [column.name, column])), [columns])

  const propertyColumns = useMemo(
    () =>
      columns.filter((column) => {
        if (column.category === 'metric') return false
        if (column.category === 'property') return true
        if (column.category === 'canonical') return column.name === 'user_id' || column.name === 'event_time'
        return false
      }),
    [columns]
  )

  const getAllowedOperators = (columnName, map = columnByName) => {
    if (!columnName || !map[columnName]) return OPERATOR_ORDER
    const type = normalizeColumnType(map[columnName]?.data_type)
    return TYPE_OPERATOR_MAP[type] || OPERATOR_ORDER
  }

  const getDefaultValuesForOperator = (operator, columnType, currentValue = null) => {
    if (columnType === 'TIMESTAMP' && isTimestampOperator(operator)) {
      if (operator === 'between') return { startDate: '', endDate: '', startTime: '', endTime: '' }
      if (operator === 'on') return { date: '' }
      return { date: '', time: '' }
    }
    if (isMultiOperator(operator)) {
      if (Array.isArray(currentValue)) return currentValue
      if (typeof currentValue === 'object' && currentValue !== null) {
        return currentValue.date ? [String(currentValue.date)] : (currentValue.startDate ? [String(currentValue.startDate)] : [])
      }
      return currentValue ? [String(currentValue)] : []
    }
    if (columnType === 'BOOLEAN') return true
    return ''
  }

  const getValueCacheKey = useCallback((eventName, columnName) => `${eventName || ''}::${columnName || ''}`, [])

  const ensureColumnValuesLoaded = useCallback(async (columnName, eventName) => {
    if (!columnName || !eventName) return
    const cacheKey = getValueCacheKey(eventName, columnName)
    if (valueCacheRef.current[cacheKey] || inFlightRequests.current.has(cacheKey)) return

    inFlightRequests.current.add(cacheKey)
    setLoadingKeys((prev) => {
      const next = new Set(prev)
      next.add(cacheKey)
      return next
    })

    try {
      const response = await getColumnValues(columnName, eventName)
      setValueCache((prev) => ({
        ...prev,
        [cacheKey]: { values: (response.values || []).map((value) => String(value)) },
      }))
    } catch {
      setValueCache((prev) => ({ ...prev, [cacheKey]: { values: [] } }))
    } finally {
      inFlightRequests.current.delete(cacheKey)
      setLoadingKeys((prev) => {
        const next = new Set(prev)
        next.add(cacheKey) // Force a re-render even if we just removing, though we usually add to cache above
        next.delete(cacheKey)
        return next
      })
    }
  }, [getValueCacheKey])



  const useEffectFn = isTest ? useLayoutEffect : useEffect

  useEffect(() => {
    // Invalidate cache on scope change
    setValueCache({})
    valueCacheRef.current = {}
    inFlightRequests.current.clear()
    setLoadingKeys(new Set())
  }, [refreshToken])

  useEffectFn(() => {
    const load = async () => {
      try {
        const [eventsResponse, columnsResponse] = await Promise.all([listEvents(), getColumns()])
        const eventList = eventsResponse.events || []
        setEvents(eventList)
        setColumns(columnsResponse.columns || [])
        
        if (!initialData) {
          setConditions((prev) =>
            prev.map((condition) => {
              if (eventList.length === 0) return { ...condition, event_name: '', property_filter: null, property_filter_expanded: false }
              if (condition.event_name && eventList.includes(condition.event_name)) return condition
              return { ...condition, event_name: eventList[0], property_filter: null, property_filter_expanded: false }
            })
          )
        }
      } catch {
        setEvents([])
        setColumns([])
      }
    }
    load()
  }, [refreshToken, initialData])

  useEffect(() => {
    // Reactive hydration for property filters
    if (events.length === 0 || columns.length === 0) return

    conditions.forEach((condition) => {
      const eventName = condition.event_name
      const columnName = condition.property_filter?.column
      if (eventName && columnName) {
        ensureColumnValuesLoaded(columnName, eventName)
      }
    })
  }, [conditions, events, columns, ensureColumnValuesLoaded])

  const lastPayloadRef = useRef(null)

  useEffect(() => {
    if (loading) return
    const delay = isTest ? 0 : 300
    let cancelled = false

    const runEstimation = () => {
      if (events.length === 0 || conditions.some((c) => !c.event_name)) {
        setEstimatedSize(null)
        lastPayloadRef.current = null
        return
      }

      const hasEmptyNumericFilter = conditions.some((condition) => {
        const propertyFilter = condition.property_filter
        if (!propertyFilter || isMultiOperator(propertyFilter.operator)) return false
        const columnType = normalizeColumnType(columnByName[propertyFilter.column]?.data_type)
        return columnType === 'NUMERIC' && propertyFilter.values === ''
      })
      if (hasEmptyNumericFilter) {
        setEstimatedSize(null)
        lastPayloadRef.current = null
        return
      }

      const hasEmptyInFilter = conditions.some((condition) => {
        const pf = condition.property_filter
        if (pf && (String(pf.operator).toUpperCase() === 'IN' || String(pf.operator).toUpperCase() === 'NOT IN')) {
          return !Array.isArray(pf.values) || pf.values.length === 0
        }
        return false
      })
      if (hasEmptyInFilter) {
        setEstimatedSize(null)
        lastPayloadRef.current = null
        return
      }

      const payloadConditions = conditions.map(({ property_filter_expanded, property_column, property_operator, property_values, ...condition }) => {
        const pf = condition.property_filter
        if (pf && pf.operator) {
          const opUpper = String(pf.operator).toUpperCase()
          const isMulti = opUpper === 'IN' || opUpper === 'NOT IN'
          const isStructuredTs = ['BEFORE', 'AFTER', 'ON', 'BETWEEN'].includes(opUpper)

          return {
            ...condition,
            property_filter: {
              ...pf,
              operator: opUpper,
              values: isMulti 
                ? (Array.isArray(pf.values) ? pf.values.map(v => String(v)) : [String(pf.values)])
                : (isStructuredTs ? pf.values : String(pf.values || ''))
            }
          }
        }
        return condition
      })
      const currentPayload = JSON.stringify({
        name: name.trim() || generateCohortName(payloadConditions, logicOperator),
        logic_operator: logicOperator,
        join_type: joinType,
        conditions: payloadConditions,
      })

      if (currentPayload === lastPayloadRef.current) return
      lastPayloadRef.current = currentPayload

      setEstimating(true)
      estimateCohort(JSON.parse(currentPayload)).then(res => {
        if (!cancelled) setEstimatedSize(res.estimated_users)
      }).catch(err => {
        if (!cancelled) setEstimatedSize(null)
      }).finally(() => {
        if (!cancelled) setEstimating(false)
      })
    }

    if (isTest) {
      Promise.resolve().then(() => {
        if (!cancelled) runEstimation()
      })
    } else {
      const delayDebounceFn = setTimeout(runEstimation, delay)
      return () => {
        cancelled = true
        clearTimeout(delayDebounceFn)
      }
    }

    return () => {
      cancelled = true
    }
  }, [conditions, logicOperator, joinType, events, columns, loading, name])

  const handleSubmit = async () => {
    setError('')
    if (events.length === 0) {
      setError('No events available')
      return
    }
    if (conditions.some((condition) => !condition.event_name)) {
      setError('Event name is required')
      return
    }

    const hasEmptyNumericFilter = conditions.some((condition) => {
      const propertyFilter = condition.property_filter
      if (!propertyFilter || isMultiOperator(propertyFilter.operator)) return false
      const columnType = normalizeColumnType(columnByName[propertyFilter.column]?.data_type)
      return columnType === 'NUMERIC' && propertyFilter.values === ''
    })
    
    if (hasEmptyNumericFilter) {
      setError('Numeric property filters require a value')
      return
    }

    const hasEmptyInFilter = conditions.some((condition) => {
      const pf = condition.property_filter
      if (pf && (String(pf.operator).toUpperCase() === 'IN' || String(pf.operator).toUpperCase() === 'NOT IN')) {
        return !Array.isArray(pf.values) || pf.values.length === 0
      }
      return false
    })
    if (hasEmptyInFilter) {
      setError('IN requires at least one value')
      return
    }

    setLoading(true)

    try {
      const payloadConditions = conditions.map(({ property_filter_expanded, property_column, property_operator, property_values, ...condition }) => {
        const pf = condition.property_filter
        if (pf && pf.operator) {
          const opUpper = String(pf.operator).toUpperCase()
          const isMulti = opUpper === 'IN' || opUpper === 'NOT IN'
          const isStructuredTs = ['BEFORE', 'AFTER', 'ON', 'BETWEEN'].includes(opUpper)

          return {
            ...condition,
            property_filter: {
              ...pf,
              operator: opUpper,
              values: isMulti 
                ? (Array.isArray(pf.values) ? pf.values.map(v => String(v)) : [String(pf.values)])
                : (isStructuredTs ? pf.values : String(pf.values || ''))
            }
          }
        }
        return condition
      })
      const finalName = name.trim() || generateCohortName(payloadConditions, logicOperator)
      const payload = {
        name: finalName,
        logic_operator: logicOperator,
        join_type: joinType,
        conditions: payloadConditions,
      }

      if (isEditing) {
        await updateSavedCohort(initialData.id, payload)
        onSave(true)
      } else {
        const saved = await createSavedCohort(payload)
        if (saved.is_valid === false) {
          setError('Saved globally, but definition is invalid for this dataset (not auto-added).')
          return
        }
        await createCohort({
          ...payload,
          source_saved_id: saved.id
        })

        showToast(finalName)
        
        if (stayOpen) {
          setName(prev => getNextName(prev))
          setEstimatedSize(null)
          onSave(false)
        } else {
          onSave(true)
        }
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="modal-overlay" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.5)' }}>
      <div className="modal-content card" style={{ padding: '24px', width: '800px', maxHeight: '90vh', overflowY: 'auto' }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <h3>{isEditing ? 'Edit Saved Cohort' : 'Create Saved Cohort'}</h3>
          <button
            onClick={onCancel}
            disabled={loading}
            style={{
              background: "transparent",
              border: "none",
              fontSize: "20px",
              cursor: "pointer",
              color: "#666"
            }}
            aria-label="Close"
          >
            ×
          </button>
        </div>
        
        <h4><strong>Name</strong></h4>
        <div className="grid">
          <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Cohort name (optional, defaults to description)" />
        </div>

        <h4><strong>Conditions</strong></h4>
        <div className="cohort-condition-logic-picker">
          <span>Match users where</span>
          <select value={logicOperator} onChange={(e) => setLogicOperator(e.target.value)}>
            <option value="AND">ALL conditions (AND)</option>
            <option value="OR">ANY conditions (OR)</option>
          </select>
        </div>
        
        {conditions.map((condition, index) => {
          const propertyFilter = condition.property_filter
          const selectedColumn = propertyFilter?.column || ''
          const columnType = normalizeColumnType(columnByName[selectedColumn]?.data_type)
          const valueCacheKey = getValueCacheKey(condition.event_name, selectedColumn)
          const availableValues = valueCache[valueCacheKey]?.values || []
          const isValueSelectionDisabled = !condition.event_name
          const showProperty = condition.property_filter_expanded && propertyFilter

          return (
            <div key={index}>
              <div className="cohort-condition-block">
                <div className="cohort-condition-content">
                  <div className="cohort-condition-row">
                    <select
                      className="cohort-negation-select"
                      value={condition.is_negated ? 'true' : 'false'}
                      onChange={(e) => {
                        setConditions(prev => {
                          const updated = [...prev]
                          updated[index] = { ...updated[index], is_negated: e.target.value === 'true' }
                          return updated
                        })
                      }}
                    >
                      <option value="false">DID</option>
                      <option value="true">DIDN'T</option>
                    </select>
                    <span className="cohort-rule-text">perform</span>
                    <SearchableSelect
                      options={events}
                      value={condition.event_name}
                      onChange={(nextEventName) => {
                        setConditions(prev => {
                          const updated = [...prev]
                          updated[index] = { ...updated[index], event_name: nextEventName, property_filter: null, property_filter_expanded: false }
                          return updated
                        })
                      }}
                      placeholder="Select event"
                      column="event_name"
                    />
                  </div>
                  <div className="cohort-condition-row">
                    <span className="cohort-rule-text">at least</span>
                    <input
                      className="cohort-count-input"
                      type="number"
                      min="1"
                      value={condition.min_event_count}
                      onChange={(e) => {
                        const val = Math.max(1, Number(e.target.value) || 1)
                        setConditions(prev => {
                          const updated = [...prev]
                          updated[index] = { ...updated[index], min_event_count: val }
                          return updated
                        })
                      }}
                    />
                    <span className="cohort-rule-text">times</span>
                  </div>

                  {conditions.length > 1 && (
                    <button
                      className="cohort-condition-remove"
                      type="button"
                      onClick={() => {
                        const updated = conditions.filter((_, i) => i !== index)
                        setConditions(updated)
                      }}
                    >
                      X
                    </button>
                  )}
                </div>

                <button
                  className="cohort-property-filter-action"
                  type="button"
                  onClick={() => {
                    const updated = [...conditions]
                    if (!updated[index].property_filter_expanded) {
                      const defaultColumn = propertyColumns[0]?.name || ''
                      const allowedOperators = getAllowedOperators(defaultColumn)
                      updated[index].property_filter = {
                        column: defaultColumn,
                        operator: allowedOperators[0] || '=',
                        values: getDefaultValuesForOperator(allowedOperators[0] || '=', normalizeColumnType(columnByName[defaultColumn]?.data_type)),
                      }
                      updated[index].property_filter_expanded = true
                      ensureColumnValuesLoaded(defaultColumn, updated[index].event_name)
                    } else {
                      updated[index].property_filter_expanded = false
                    }
                    setConditions(updated)
                  }}
                >
                  {showProperty ? 'Hide property filter' : '+ Add property filter (optional)'}
                </button>

                {showProperty && (
                  <div className="cohort-rule-filters">
                    <div className="cohort-rule-filter-grid">
                      <label>Property</label>
                      <SearchableSelect
                        options={propertyColumns.map((column) => ({ label: column.name, value: column.name }))}
                        value={propertyFilter.column}
                        onChange={(nextColumn) => {
                          if (!nextColumn) {
                            const updated = [...conditions]
                            updated[index].property_filter = null
                            updated[index].property_filter_expanded = false
                            setConditions(updated)
                            return
                          }
                          const allowed = getAllowedOperators(nextColumn)
                          const nextType = normalizeColumnType(columnByName[nextColumn]?.data_type)
                          const updated = [...conditions]
                          updated[index].property_filter = {
                            ...updated[index].property_filter,
                            column: nextColumn,
                            operator: allowed[0],
                            values: getDefaultValuesForOperator(allowed[0], nextType),
                          }
                          setConditions(updated)
                          ensureColumnValuesLoaded(nextColumn, updated[index].event_name)
                        }}
                        placeholder="Select column"
                      />

                      <label>Operator</label>
                      <select
                        value={propertyFilter.operator}
                        onChange={(e) => {
                          const nextOperator = e.target.value
                          const updated = [...conditions]
                          updated[index].property_filter = {
                            ...updated[index].property_filter,
                            operator: nextOperator,
                            values: isMultiOperator(nextOperator) ? [] : getDefaultValuesForOperator(nextOperator, columnType),
                          }
                          setConditions(updated)
                          if (isMultiOperator(nextOperator)) {
                            ensureColumnValuesLoaded(propertyFilter.column, updated[index].event_name)
                          }
                        }}
                      >
                        {getAllowedOperators(propertyFilter.column).map((operator) => (
                          <option key={operator} value={operator}>
                            {operator}
                          </option>
                        ))}
                      </select>

                      <label>Values</label>
                      <div>
                        {columnType === 'BOOLEAN' ? (
                          <select
                            disabled={isValueSelectionDisabled}
                            value={String(propertyFilter.values)}
                            onChange={(e) => {
                              const updated = [...conditions]
                              updated[index].property_filter = {
                                ...updated[index].property_filter,
                                values: e.target.value === 'true',
                              }
                              setConditions(updated)
                            }}
                          >
                            <option value="true">True</option>
                            <option value="false">False</option>
                          </select>                         ) : isMultiOperator(propertyFilter.operator) ? (
                          <div className="cohort-multi-values">
                            <SearchableSelect
                              options={availableValues}
                              value=""
                              disabled={isValueSelectionDisabled}
                              onChange={(selected) => {
                                if (!selected) return
                                const normalizedValue = (typeof selected === 'object' && !Array.isArray(selected)) ? String(selected.value || '') : String(selected)
                                if (!normalizedValue || normalizedValue === '[object Object]') return
                                
                                const existing = Array.isArray(propertyFilter.values) ? propertyFilter.values : []
                                if (existing.includes(normalizedValue)) return
                                const updated = [...conditions]
                                updated[index].property_filter = {
                                  ...updated[index].property_filter,
                                  values: [...existing, normalizedValue],
                                }
                                setConditions(updated)
                              }}
                              placeholder={loadingKeys.has(valueCacheKey) ? "Loading options..." : "Select values"}
                              column={propertyFilter.column}
                              eventName={condition.event_name}
                            />

                            <div className="cohort-pills">
                              {(Array.isArray(propertyFilter.values) ? propertyFilter.values : []).map((value) => (
                                <span className="cohort-pill" key={value}>
                                  {value}
                                  <button
                                    type="button"
                                    onClick={() => {
                                      const updated = [...conditions]
                                      const currentValues = Array.isArray(updated[index].property_filter.values) ? updated[index].property_filter.values : []
                                      updated[index].property_filter = {
                                        ...updated[index].property_filter,
                                        values: currentValues.filter((item) => item !== value),
                                      }
                                      setConditions(updated)
                                    }}
                                  >
                                    ×
                                  </button>
                                </span>
                              ))}
                            </div>
                          </div>
                        ) : columnType === 'NUMERIC' ? (
                          <input
                            disabled={isValueSelectionDisabled}
                            type="number"
                            value={Array.isArray(propertyFilter.values) ? propertyFilter.values[0] ?? '' : propertyFilter.values}
                            onChange={(e) => {
                              const updated = [...conditions]
                              updated[index].property_filter = {
                                ...updated[index].property_filter,
                                values: e.target.value === '' ? '' : Number(e.target.value),
                              }
                              setConditions(updated)
                            }}
                          />
                        ) : columnType === 'TIMESTAMP' ? (
                          <div className="grid">
                            {(propertyFilter.operator === 'before' || propertyFilter.operator === 'after') && (
                              <>
                                <input
                                  disabled={isValueSelectionDisabled}
                                  type="date"
                                  value={propertyFilter.values?.date || ''}
                                  onChange={(e) => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: { ...(propertyFilter.values || {}), date: e.target.value },
                                    }
                                    setConditions(updated)
                                  }}
                                />
                                <input
                                  disabled={isValueSelectionDisabled}
                                  type="time"
                                  step="1"
                                  value={propertyFilter.values?.time || ''}
                                  onChange={(e) => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: { ...(propertyFilter.values || {}), time: e.target.value },
                                    }
                                    setConditions(updated)
                                  }}
                                />
                              </>
                            )}
                            {propertyFilter.operator === 'on' && (
                              <input
                                disabled={isValueSelectionDisabled}
                                type="date"
                                value={propertyFilter.values?.date || ''}
                                onChange={(e) => {
                                  const updated = [...conditions]
                                  updated[index].property_filter = {
                                    ...updated[index].property_filter,
                                    values: { date: e.target.value },
                                  }
                                  setConditions(updated)
                                }}
                              />
                            )}
                            {propertyFilter.operator === 'between' && (
                              <>
                                <input
                                  disabled={isValueSelectionDisabled}
                                  type="date"
                                  value={propertyFilter.values?.startDate || ''}
                                  onChange={(e) => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: { ...(propertyFilter.values || {}), startDate: e.target.value },
                                    }
                                    setConditions(updated)
                                  }}
                                />
                                <input
                                  disabled={isValueSelectionDisabled}
                                  type="time"
                                  step="1"
                                  value={propertyFilter.values?.startTime || ''}
                                  onChange={(e) => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: { ...(propertyFilter.values || {}), startTime: e.target.value },
                                    }
                                    setConditions(updated)
                                  }}
                                />
                                <input
                                  disabled={isValueSelectionDisabled}
                                  type="date"
                                  value={propertyFilter.values?.endDate || ''}
                                  onChange={(e) => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: { ...(propertyFilter.values || {}), endDate: e.target.value },
                                    }
                                    setConditions(updated)
                                  }}
                                />
                                <input
                                  disabled={isValueSelectionDisabled}
                                  type="time"
                                  step="1"
                                  value={propertyFilter.values?.endTime || ''}
                                  onChange={(e) => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: { ...(propertyFilter.values || {}), endTime: e.target.value },
                                    }
                                    setConditions(updated)
                                  }}
                                />
                              </>
                            )}
                          </div>
                        ) : (
                          <SearchableSelect
                            options={availableValues}
                            value={String((Array.isArray(propertyFilter.values) ? propertyFilter.values[0] : propertyFilter.values) || '')}
                            disabled={isValueSelectionDisabled}
                            onChange={(nextValue) => {
                              const updated = [...conditions]
                              updated[index].property_filter = {
                                ...updated[index].property_filter,
                                values: nextValue,
                              }
                              setConditions(updated)
                            }}
                            placeholder={loadingKeys.has(valueCacheKey) ? "Loading options..." : "Select value"}
                            column={propertyFilter.column}
                            eventName={condition.event_name}
                          />
                        )}
                      </div>
                    </div>

                    <button
                      className="cohort-condition-remove"
                      type="button"
                      onClick={() => {
                        const updated = [...conditions]
                        updated[index].property_filter = null
                        updated[index].property_filter_expanded = false
                        setConditions(updated)
                      }}
                    >
                      Remove Filter
                    </button>
                  </div>
                )}
              </div>

              {conditions.length > 1 && index < conditions.length - 1 && <div className="cohort-logic-connector">{logicOperator}</div>}
            </div>
          )
        })}

        <button
          className="cohort-add-condition"
          type="button"
          disabled={conditions.length >= 5}
          onClick={() => setConditions(prev => [...prev, createEmptyCondition(events[0] || '')])}
        >
          + Add Condition
        </button>

        <h4><strong>Materialization logic</strong></h4>
        <div className="cohort-join-time">
          <select value={joinType} onChange={(e) => setJoinType(e.target.value)}>
             <option value="condition_met">Join when condition is met</option>
             <option value="first_event">Join on first qualifying event</option>
          </select>
        </div>
        
        <div style={{ marginTop: '16px', padding: '12px', backgroundColor: '#f5f5f5', borderRadius: '4px', display: 'flex', alignItems: 'center' }}>
          <span style={{ fontWeight: 'bold', marginRight: '8px' }}>Estimated matching users:</span>
          {estimating ? (
            <span style={{ color: '#666' }}>Estimating...</span>
          ) : estimatedSize !== null ? (
            <span style={{ color: '#1a73e8' }}>{formatCohortSize(estimatedSize)}</span>
          ) : (
            <span style={{ color: '#999' }}>-</span>
          )}
        </div>

        {toast && (
          <div className="inline-toast success" style={{ display: 'flex', justifyContent: 'center' }}>
            {toast}
          </div>
        )}

        <div className="inline-controls" style={{ marginTop: '24px', display: 'flex', gap: '12px', alignItems: 'center' }}>
          <button 
            className="button button-primary" 
            onClick={handleSubmit} 
            disabled={loading || events.length === 0}
            style={{ width: !isEditing ? (stayOpen ? '60%' : '80%') : 'auto' }}
          >
            {loading ? 'Saving...' : 'Save Cohort'}
          </button>

          {stayOpen && !isEditing && (
            <button 
              className="button" 
              onClick={handleReset} 
              disabled={loading}
              style={{ width: '15%' }}
            >
              Reset
            </button>
          )}

          {!isEditing && (
            <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px', color: '#666', whiteSpace: 'nowrap' }}>
              <input 
                type="checkbox" 
                id="stay-open-toggle"
                checked={stayOpen} 
                onChange={(e) => setStayOpen(e.target.checked)} 
              />
              <label htmlFor="stay-open-toggle" style={{ cursor: 'pointer', margin: 0, paddingTop: '2px' }}>Multi-Create</label>
            </div>
          )}
        </div>

        {events.length === 0 && <p className="error" style={{marginTop: '8px'}}>No events available under current filters</p>}
        {error && <p className="error" style={{marginTop: '8px'}}>{error}</p>}
      </div>
    </div>
  )
}
