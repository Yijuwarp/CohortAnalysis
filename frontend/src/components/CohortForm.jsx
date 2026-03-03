import { useEffect, useMemo, useState } from 'react'
import { createCohort, deleteCohort, getColumnValues, getColumns, listCohorts, listEvents, updateCohort } from '../api'
import SearchableSelect from './SearchableSelect'

const OPERATOR_ORDER = ['=', '!=', '>', '>=', '<', '<=', 'IN', 'NOT IN']

const TYPE_OPERATOR_MAP = {
  TEXT: ['IN', 'NOT IN', '=', '!='],
  NUMERIC: ['=', '!=', '>', '<', '>=', '<=', 'IN', 'NOT IN'],
  TIMESTAMP: ['=', '!=', '>', '<', '>=', '<=', 'IN', 'NOT IN'],
  BOOLEAN: ['=', '!='],
}

const normalizeColumnType = (dataType = '') => {
  const upper = String(dataType).toUpperCase()
  if (upper === 'BOOLEAN' || upper === 'BOOL') {
    return 'BOOLEAN'
  }
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

const createEmptyCondition = (defaultEvent = '') => ({
  event_name: defaultEvent,
  min_event_count: 1,
  property_filter: null,
  property_filter_expanded: false,
})

const isMultiOperator = (operator) => operator === 'IN' || operator === 'NOT IN'

const formatPropertyFilter = (propertyFilter) => {
  if (!propertyFilter) {
    return ''
  }

  const formattedValues = Array.isArray(propertyFilter.values)
    ? propertyFilter.values.join(', ')
    : propertyFilter.values

  if (isMultiOperator(propertyFilter.operator)) {
    return ` WHERE ${propertyFilter.column} ${propertyFilter.operator} (${formattedValues})`
  }

  return ` WHERE ${propertyFilter.column} ${propertyFilter.operator} ${formattedValues}`
}

export default function CohortForm({ refreshToken, onCohortsChanged }) {
  const [name, setName] = useState('')
  const [conditions, setConditions] = useState([createEmptyCondition('')])
  const [logicOperator, setLogicOperator] = useState('AND')
  const [joinType, setJoinType] = useState('condition_met')
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [cohorts, setCohorts] = useState([])
  const [deletingId, setDeletingId] = useState(null)
  const [events, setEvents] = useState([])
  const [columns, setColumns] = useState([])
  const [valueCache, setValueCache] = useState({})
  const [infoCohortId, setInfoCohortId] = useState(null)
  const [editingCohortId, setEditingCohortId] = useState(null)

  const columnByName = useMemo(() => Object.fromEntries(columns.map((column) => [column.name, column])), [columns])

  const getAllowedOperators = (columnName, map = columnByName) => {
    if (!columnName || !map[columnName]) {
      return OPERATOR_ORDER
    }
    const type = normalizeColumnType(map[columnName]?.data_type)
    return TYPE_OPERATOR_MAP[type] || OPERATOR_ORDER
  }

  const getDefaultValuesForOperator = (operator, columnType) => {
    if (isMultiOperator(operator)) {
      return []
    }
    if (columnType === 'BOOLEAN') {
      return true
    }
    return ''
  }

  const getValueCacheKey = (eventName, columnName) => `${eventName || ''}__${columnName || ''}`

  const ensureColumnValuesLoaded = async (columnName, eventName) => {
    if (!columnName || !eventName) {
      return
    }

    const cacheKey = getValueCacheKey(eventName, columnName)
    if (valueCache[cacheKey]) {
      return
    }

    try {
      const response = await getColumnValues(columnName, eventName)
      setValueCache((prev) => ({
        ...prev,
        [cacheKey]: {
          values: (response.values || []).map((value) => String(value)),
        },
      }))
    } catch {
      setValueCache((prev) => ({
        ...prev,
        [cacheKey]: {
          values: [],
        },
      }))
    }
  }

  const resetForm = () => {
    setEditingCohortId(null)
    setName('')
    setConditions([createEmptyCondition(events[0] || '')])
    setLogicOperator('AND')
    setJoinType('condition_met')
  }

  const loadCohorts = async () => {
    try {
      const response = await listCohorts()
      setCohorts(response.cohorts || [])
    } catch {
      setCohorts([])
    }
  }

  useEffect(() => {
    loadCohorts()
  }, [refreshToken])

  useEffect(() => {
    const load = async () => {
      try {
        const [eventsResponse, columnsResponse] = await Promise.all([listEvents(), getColumns()])
        const eventList = eventsResponse.events || []
        setEvents(eventList)
        setColumns(columnsResponse.columns || [])
        setConditions((prev) =>
          prev.map((condition) => {
            if (eventList.length === 0) {
              return { ...condition, event_name: '', property_filter: null, property_filter_expanded: false }
            }

            if (condition.event_name && eventList.includes(condition.event_name)) {
              return condition
            }

            return { ...condition, event_name: eventList[0], property_filter: null, property_filter_expanded: false }
          })
        )
      } catch {
        setEvents([])
        setColumns([])
        setConditions((prev) =>
          prev.map((condition) => ({ ...condition, event_name: '', property_filter: null, property_filter_expanded: false }))
        )
      }
    }
    load()
  }, [refreshToken])

  const handleSubmit = async () => {
    setError('')
    setResult(null)

    if (!name.trim()) {
      setError('Cohort name is required')
      return
    }

    if (events.length === 0) {
      setError('No events available under current filters')
      return
    }

    if (conditions.some((condition) => !condition.event_name)) {
      setError('Event name is required')
      return
    }

    const hasEmptyNumericFilter = conditions.some((condition) => {
      const propertyFilter = condition.property_filter
      if (!propertyFilter || isMultiOperator(propertyFilter.operator)) {
        return false
      }
      const columnType = normalizeColumnType(columnByName[propertyFilter.column]?.data_type)
      return columnType === 'NUMERIC' && propertyFilter.values === ''
    })
    if (hasEmptyNumericFilter) {
      setError('Numeric property filters require a value')
      return
    }

    setLoading(true)

    try {
      const payloadConditions = conditions.map(({ property_filter_expanded, ...condition }) => condition)
      const payload = {
        name,
        logic_operator: logicOperator,
        join_type: joinType,
        conditions: payloadConditions,
      }

      const isEditing = Boolean(editingCohortId)
      const data = isEditing ? await updateCohort(editingCohortId, payload) : await createCohort(payload)
      setResult({ ...data, mode: isEditing ? 'updated' : 'created' })
      resetForm()
      await loadCohorts()
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const handleDelete = async (cohortId) => {
    setDeletingId(cohortId)
    setError('')
    setResult(null)

    try {
      await deleteCohort(cohortId)
      setCohorts((prev) => prev.filter((cohort) => cohort.cohort_id !== cohortId))
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    } finally {
      setDeletingId(null)
    }
  }

  const handleEdit = (cohort) => {
    setEditingCohortId(cohort.cohort_id)
    setName(cohort.cohort_name)
    setLogicOperator(cohort.logic_operator || 'AND')
    setJoinType(cohort.join_type || 'condition_met')
    setConditions(
      cohort.conditions?.length
        ? cohort.conditions.map((condition) => ({ ...condition, property_filter: condition.property_filter || null, property_filter_expanded: false }))
        : [createEmptyCondition(events[0] || '')]
    )
  }

  return (
    <section className="card">
      <h2>4. {editingCohortId ? 'Edit Cohort' : 'Create Cohort'}</h2>
      <div className="grid">
        <label>
          Cohort Name
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </label>
      </div>

      <h4>Rules</h4>
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
                <SearchableSelect
                  options={events}
                  value={condition.event_name}
                  onChange={(nextEventName) => {
                    const updated = [...conditions]
                    updated[index].event_name = nextEventName
                    updated[index].property_filter = null
                    updated[index].property_filter_expanded = false
                    setConditions(updated)
                  }}
                  placeholder="Select event"
                />

                <span className="cohort-operator-symbol">≥</span>

                <input
                  type="number"
                  min="1"
                  value={condition.min_event_count}
                  onChange={(e) => {
                    const updated = [...conditions]
                    updated[index].min_event_count = Number(e.target.value)
                    setConditions(updated)
                  }}
                />

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
                className="cohort-add-condition"
                type="button"
                onClick={() => {
                  const updated = [...conditions]
                  if (!updated[index].property_filter_expanded) {
                    const defaultColumn = columns[0]?.name || ''
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
                {showProperty ? '▼ Property Filter' : '▶ Add Property Filter (Optional)'}
              </button>

              {showProperty && (
                <div className="cohort-rule-filters">
                  <div className="cohort-rule-filter-grid">
                    <label>Property</label>
                    <SearchableSelect
                      options={columns.map((column) => ({ label: column.name, value: column.name }))}
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
                          values: getDefaultValuesForOperator(nextOperator, columnType),
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
                        </select>
                      ) : isMultiOperator(propertyFilter.operator) ? (
                        <div className="cohort-multi-values">
                          <SearchableSelect
                            options={availableValues}
                            value=""
                            disabled={isValueSelectionDisabled}
                            onChange={(selected) => {
                              const existing = Array.isArray(propertyFilter.values) ? propertyFilter.values : []
                              if (existing.includes(selected) || existing.length >= 100) {
                                return
                              }
                              const updated = [...conditions]
                              updated[index].property_filter = {
                                ...updated[index].property_filter,
                                values: [...existing, selected],
                              }
                              setConditions(updated)
                            }}
                            placeholder="Select values"
                          />
                          <div className="cohort-pills">
                            {(propertyFilter.values || []).map((value) => (
                              <span className="cohort-pill" key={value}>
                                {value}
                                <button
                                  type="button"
                                  onClick={() => {
                                    const updated = [...conditions]
                                    updated[index].property_filter = {
                                      ...updated[index].property_filter,
                                      values: (updated[index].property_filter.values || []).filter((item) => item !== value),
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
                          value={propertyFilter.values}
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
                        <input
                          disabled={isValueSelectionDisabled}
                          type="datetime-local"
                          value={String(propertyFilter.values || '')}
                          onChange={(e) => {
                            const updated = [...conditions]
                            updated[index].property_filter = {
                              ...updated[index].property_filter,
                              values: e.target.value,
                            }
                            setConditions(updated)
                          }}
                        />
                      ) : (
                        <SearchableSelect
                          options={availableValues}
                          value={String(propertyFilter.values || '')}
                          disabled={isValueSelectionDisabled}
                          onChange={(nextValue) => {
                            const updated = [...conditions]
                            updated[index].property_filter = {
                              ...updated[index].property_filter,
                              values: nextValue,
                            }
                            setConditions(updated)
                          }}
                          placeholder="Select value"
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

            {conditions.length > 1 && index < conditions.length - 1 && (
              <div className="cohort-logic-connector">
                <select value={logicOperator} onChange={(e) => setLogicOperator(e.target.value)}>
                  <option value="AND">AND</option>
                  <option value="OR">OR</option>
                </select>
              </div>
            )}
          </div>
        )
      })}

      <button
        className="cohort-add-condition"
        type="button"
        disabled={conditions.length >= 5}
        onClick={() => setConditions([...conditions, createEmptyCondition(events[0] || '')])}
      >
        + Add Condition
      </button>

      <h4>Cohort Join Time</h4>
      <div>
        <label>
          <input
            type="radio"
            name="join-type"
            value="condition_met"
            checked={joinType === 'condition_met'}
            onChange={(e) => setJoinType(e.target.value)}
          />{' '}
          When condition is met
        </label>
      </div>
      <div>
        <label>
          <input
            type="radio"
            name="join-type"
            value="first_event"
            checked={joinType === 'first_event'}
            onChange={(e) => setJoinType(e.target.value)}
          />{' '}
          On first event
        </label>
      </div>

      <div className="inline-controls">
        <button className="button button-primary" onClick={handleSubmit} disabled={loading || events.length === 0}>
          {loading ? (editingCohortId ? 'Updating...' : 'Creating...') : editingCohortId ? 'Update Cohort' : 'Create Cohort'}
        </button>

        {editingCohortId && (
          <button className="button button-secondary" type="button" onClick={resetForm} disabled={loading}>
            Cancel
          </button>
        )}
      </div>
      {events.length === 0 && <p className="error">No events available under current filters</p>}
      {error && <p className="error">{error}</p>}
      {result && (
        <p className="success">
          {result.mode === 'updated' ? 'Updated' : 'Created'} cohort #{result.cohort_id} with {result.users_joined} users joined.
        </p>
      )}

      <h3>Existing Cohorts</h3>
      {cohorts.length === 0 ? (
        <p className="secondary-text">No cohorts created yet.</p>
      ) : (
        <ul>
          {cohorts.map((cohort) => (
            <li key={cohort.cohort_id} title={cohort.is_active ? '' : 'No matching members under current filters'}>
              <div className="cohort-row">
                <div className="cohort-left">
                  <span>{cohort.cohort_name}</span>
                  {!cohort.is_active && <span className="badge-inactive">Inactive</span>}
                </div>

                <div className="cohort-actions">
                  <button
                    className="button button-secondary button-icon"
                    type="button"
                    onClick={() => setInfoCohortId(infoCohortId === cohort.cohort_id ? null : cohort.cohort_id)}
                  >
                    i
                  </button>

                  <button className="button button-secondary" type="button" onClick={() => handleEdit(cohort)}>
                    Edit
                  </button>

                  <button
                    className="button button-danger"
                    type="button"
                    onClick={() => handleDelete(cohort.cohort_id)}
                    disabled={deletingId === cohort.cohort_id}
                  >
                    {deletingId === cohort.cohort_id ? 'Deleting...' : 'Delete'}
                  </button>
                </div>
              </div>
              {infoCohortId === cohort.cohort_id && (
                <div className="cohort-info">
                  <div>
                    <strong>Logic:</strong> {cohort.logic_operator}
                  </div>
                  <div>
                    <strong>Join:</strong> {cohort.join_type || 'condition_met'}
                  </div>
                  {(cohort.conditions || []).map((c, infoIndex) => (
                    <div key={infoIndex}>
                      {c.event_name}
                      {c.property_filter
                        ? formatPropertyFilter(c.property_filter)
                        : ''}{' '}
                      ≥ {c.min_event_count}
                    </div>
                  ))}
                </div>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
