import { Fragment, useEffect, useMemo, useState } from 'react'
import { createCohort, deleteCohort, getColumnValues, getColumns, listCohorts, listEvents, randomSplitCohort, toggleCohortHide, updateCohort } from '../api'
import SearchableSelect from './SearchableSelect'

const OPERATOR_ORDER = ['=', '!=', '>', '>=', '<', '<=', 'IN', 'NOT IN']

const STRUCTURAL_COLUMNS = new Set(['user_id', 'event_name', 'event_time'])

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

const formatCohortSize = (size) => {
  const numeric = Number(size || 0)
  if (numeric >= 1000000) {
    return `${(numeric / 1000000).toFixed(numeric >= 10000000 ? 0 : 1)}M`
  }
  if (numeric >= 1000) {
    return `${(numeric / 1000).toFixed(numeric >= 100000 ? 0 : 1)}K`
  }
  return String(numeric)
}

const describeJoinType = (joinType) => (joinType === 'first_event' ? 'Join on first event' : 'Join when condition is met')

const buildCohortDefinition = (cohort) => {
  const logic = cohort.condition_logic || cohort.logic_operator || 'AND'
  const conditionLines = (cohort.conditions || []).map((condition) => {
    const property = condition.property_filter ? formatPropertyFilter(condition.property_filter) : ''
    return `${condition.event_name} ≥ ${condition.min_event_count}${property}`
  })
  return [`Logic: ${logic}`, ...conditionLines, describeJoinType(cohort.join_type)].join(' • ')
}

function generateCohortName(currentConditions, currentLogicOperator) {
  if (!currentConditions || currentConditions.length === 0) {
    return 'Untitled Cohort'
  }

  const parts = currentConditions.map((cond) => {
    const event = cond.event_name || 'event'
    const count = cond.min_event_count ?? 1
    const propertyFilter = cond.property_filter

    let base = ''

    if (count > 1) {
      base = `Triggered ${event} more than ${count} times`
    } else {
      base = `Triggered ${event}`
    }

    if (propertyFilter) {
      const values = Array.isArray(propertyFilter.values)
        ? propertyFilter.values.join(', ')
        : propertyFilter.values

      base += ` where ${propertyFilter.column} ${propertyFilter.operator} ${values}`
    }

    return base
  })

  return parts.join(` ${currentLogicOperator} `)
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
  const [editingCohortId, setEditingCohortId] = useState(null)
  const [splittingId, setSplittingId] = useState(null)

  const columnByName = useMemo(() => Object.fromEntries(columns.map((column) => [column.name, column])), [columns])

  const propertyColumns = useMemo(
    () => columns.filter((column) => !STRUCTURAL_COLUMNS.has(column.name)),
    [columns]
  )

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
      const finalName = name.trim() || generateCohortName(payloadConditions, logicOperator)
      const payload = {
        name: finalName,
        logic_operator: logicOperator,
        condition_logic: logicOperator,
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

  const handleToggleHide = async (cohortId) => {
    setError('')
    setResult(null)

    try {
      await toggleCohortHide(cohortId)
      await loadCohorts()
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
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

  const handleRandomSplit = async (cohort) => {
    setError('')
    setResult(null)
    setSplittingId(cohort.cohort_id)

    try {
      await randomSplitCohort(cohort.cohort_id)
      await loadCohorts()
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    } finally {
      setSplittingId(null)
    }
  }

  const parentCohorts = useMemo(
    () => cohorts.filter((cohort) => !cohort.split_parent_cohort_id),
    [cohorts]
  )

  const childCohortsByParent = useMemo(() => {
    const childrenMap = {}
    cohorts.forEach((cohort) => {
      if (!cohort.split_parent_cohort_id) {
        return
      }
      if (!childrenMap[cohort.split_parent_cohort_id]) {
        childrenMap[cohort.split_parent_cohort_id] = []
      }
      childrenMap[cohort.split_parent_cohort_id].push(cohort)
    })

    Object.keys(childrenMap).forEach((parentId) => {
      childrenMap[parentId].sort((a, b) => (a.split_group_index ?? 0) - (b.split_group_index ?? 0))
    })

    return childrenMap
  }, [cohorts])

  const parentDefinitionTooltips = useMemo(
    () => Object.fromEntries(parentCohorts.map((cohort) => [cohort.cohort_id, buildCohortDefinition(cohort)])),
    [parentCohorts]
  )

  const childDefinitionTooltips = useMemo(() => {
    const map = {}
    Object.values(childCohortsByParent).forEach((children) => {
      children.forEach((child) => {
        map[child.cohort_id] = buildCohortDefinition(child)
      })
    })
    return map
  }, [childCohortsByParent])


  const handleEdit = (cohort) => {
    setEditingCohortId(cohort.cohort_id)
    setName(cohort.cohort_name)
    setLogicOperator(cohort.condition_logic || cohort.logic_operator || 'AND')
    setJoinType(cohort.join_type || 'condition_met')
    setConditions(
      cohort.conditions?.length
        ? cohort.conditions.map((condition) => ({ ...condition, property_filter: condition.property_filter || null, property_filter_expanded: false }))
        : [createEmptyCondition(events[0] || '')]
    )
  }

  return (
    <section>
      <div className="cohorts-section-card create-cohorts-card">
        <h3>{editingCohortId ? 'Edit Cohort' : 'Create Cohort'}</h3>
      <h4><strong>Name</strong></h4>
      <div className="grid">
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Cohort name (optional)" />
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
                <p className="cohort-rule-text">Performed</p>
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
                </div>
                                           <div className="cohort-condition-row">
                <span className="cohort-rule-text">at least</span>

                <input
                  className="cohort-count-input"
                  type="number"
                  min="1"
                  value={condition.min_event_count}
                  onChange={(e) => {
                    const updated = [...conditions]
                    updated[index].min_event_count = Math.max(1, Number(e.target.value) || 1)
                    setConditions(updated)
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

            {conditions.length > 1 && index < conditions.length - 1 && <div className="cohort-logic-connector">{logicOperator}</div>}
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

      <h4><strong>Users join the cohort</strong></h4>
      <div className="cohort-join-time">
        <select value={joinType} onChange={(e) => setJoinType(e.target.value)}>
          <option value="condition_met">When condition is met</option>
          <option value="first_event">On first event</option>
        </select>
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

      </div>

      <div className="cohorts-section-card existing-cohorts-card">
        <h3>Existing Cohorts</h3>
        {cohorts.length === 0 ? (
          <p className="secondary-text">No cohorts created yet.</p>
        ) : (
          <div className="cohort-list-table">
            <div className="cohort-list-header cohort-list-row">
              <span>Name</span>
              <span>Size</span>
              <span>Actions</span>
            </div>
            {parentCohorts.map((cohort) => {
              const isSystemCohort = cohort.cohort_name === 'All Users'
              const childCohorts = cohort.hidden ? [] : (childCohortsByParent[cohort.cohort_id] || [])
              const minSizeForSplit = Number(cohort.size || 0) >= 8
              const definitionTooltip = parentDefinitionTooltips[cohort.cohort_id]

              return (
                <Fragment key={cohort.cohort_id}>
                  <div className="cohort-list-row cohort-row" title={cohort.is_active ? '' : 'No matching members under current filters'}>
                    <div className="cohort-list-name cohort-left">
                      <span>{cohort.cohort_name}</span>
                      {cohort.hidden && <span className="badge-hidden">Hidden</span>}
                      {!cohort.is_active && <span className="badge-inactive">Inactive</span>}
                    </div>

                    <span className="cohort-list-size">{formatCohortSize(cohort.size)}</span>

                    <div className="cohort-actions">
                      <button className="cohort-icon-button" type="button" aria-label="View cohort definition" title={definitionTooltip}>
                        ℹ
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleRandomSplit(cohort)}
                        disabled={!minSizeForSplit || splittingId === cohort.cohort_id}
                        title={minSizeForSplit ? 'Create random subsets from this cohort' : 'Minimum 8 users required'}
                      >
                        {splittingId === cohort.cohort_id ? '⏳' : '🎲'}
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleToggleHide(cohort.cohort_id)}
                        title={cohort.hidden ? 'Show cohort in charts' : 'Hide cohort from charts'}
                      >
                        👁
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleEdit(cohort)}
                        disabled={isSystemCohort}
                        title={isSystemCohort ? 'System cohort cannot be modified' : 'Edit cohort'}
                      >
                        ✏
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleDelete(cohort.cohort_id)}
                        disabled={deletingId === cohort.cohort_id || isSystemCohort}
                        title={isSystemCohort ? 'System cohort cannot be deleted' : 'Delete cohort'}
                      >
                        {deletingId === cohort.cohort_id ? '⏳' : '🗑'}
                      </button>
                    </div>
                  </div>

                  {childCohorts.map((child) => {
                    const isChildSystemCohort = child.cohort_name === 'All Users'
                    const childDefinitionTooltip = childDefinitionTooltips[child.cohort_id]
                    return (
                      <div key={child.cohort_id} className="cohort-list-row cohort-row child" title={child.is_active ? '' : 'No matching members under current filters'}>
                        <div className="cohort-list-name cohort-left">
                          <span>{child.cohort_name}</span>
                          {child.hidden && <span className="badge-hidden">Hidden</span>}
                          {!child.is_active && <span className="badge-inactive">Inactive</span>}
                        </div>

                        <span className="cohort-list-size">{formatCohortSize(child.size)}</span>

                        <div className="cohort-actions">
                          <button className="cohort-icon-button" type="button" aria-label="View cohort definition" title={childDefinitionTooltip}>
                            ℹ
                          </button>
                          <button
                            className="cohort-icon-button"
                            type="button"
                            onClick={() => handleDelete(child.cohort_id)}
                            disabled={deletingId === child.cohort_id || isChildSystemCohort}
                            title={isChildSystemCohort ? 'System cohort cannot be deleted' : 'Delete cohort'}
                          >
                            {deletingId === child.cohort_id ? '⏳' : '🗑'}
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </Fragment>
              )
            })}
          </div>
        )}
      </div>
    </section>
  )
}
