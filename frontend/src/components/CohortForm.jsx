import { useEffect, useMemo, useState } from 'react'
import { createCohort, deleteCohort, getColumnValues, getColumns, listCohorts, listEvents, updateCohort } from '../api'

const OPERATOR_ORDER = ['=', '!=', '>', '>=', '<', '<=']

const TYPE_OPERATOR_MAP = {
  TEXT: ['=', '!='],
  NUMERIC: ['=', '!=', '>', '>=', '<', '<='],
  TIMESTAMP: ['=', '!=', '>', '>=', '<', '<='],
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

export default function CohortForm({ refreshToken, onCohortsChanged }) {
  const [name, setName] = useState('')
  const [conditions, setConditions] = useState([{ event_name: '', min_event_count: 1, property_filter: null }])
  const [logicOperator, setLogicOperator] = useState('AND')
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
        },
      }))
    } catch {
      setValueCache((prev) => ({
        ...prev,
        [columnName]: {
          values: [],
        },
      }))
    }
  }

  const resetForm = () => {
    setEditingCohortId(null)
    setName('')
    setConditions([{ event_name: events[0] || '', min_event_count: 1, property_filter: null }])
    setLogicOperator('AND')
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
              return { ...condition, event_name: '', property_filter: null }
            }

            if (condition.event_name && eventList.includes(condition.event_name)) {
              return condition
            }

            return { ...condition, event_name: eventList[0], property_filter: null }
          })
        )
      } catch {
        setEvents([])
        setColumns([])
        setConditions((prev) => prev.map((condition) => ({ ...condition, event_name: '', property_filter: null })))
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

    setLoading(true)

    try {
      const payload = {
        name,
        logic_operator: logicOperator,
        conditions,
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
    setConditions(
      cohort.conditions?.length
        ? cohort.conditions.map((condition) => ({ ...condition, property_filter: condition.property_filter || null }))
        : [{ event_name: events[0] || '', min_event_count: 1, property_filter: null }]
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
      {conditions.map((condition, index) => (
        <div key={index}>
          <div className="cohort-condition-block">
            <div className="cohort-condition-content">
              <select
                value={condition.event_name}
                onChange={(e) => {
                  const updated = [...conditions]
                  updated[index].event_name = e.target.value
                  updated[index].property_filter = null
                  setConditions(updated)
                }}
              >
                {events.map((event) => (
                  <option key={event} value={event}>
                    {event}
                  </option>
                ))}
              </select>

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

            {!condition.property_filter && (
              <button
                className="cohort-add-condition"
                type="button"
                onClick={() => {
                  const updated = [...conditions]
                  const defaultColumn = columns[0]?.name || ''
                  updated[index].property_filter = {
                    column: defaultColumn,
                    operator: getAllowedOperators(defaultColumn)[0] || '=',
                    value: '',
                  }
                  setConditions(updated)
                  ensureColumnValuesLoaded(defaultColumn)
                }}
              >
                + Add property filter
              </button>
            )}

            {condition.property_filter && (
              <div className="cohort-rule-filters">
                <label>Where:</label>
                <div className="cohort-condition-content">
                  <select
                    value={condition.property_filter.column}
                    onChange={(e) => {
                      const nextColumn = e.target.value
                      const allowed = getAllowedOperators(nextColumn)
                      const updated = [...conditions]
                      updated[index].property_filter = {
                        ...updated[index].property_filter,
                        column: nextColumn,
                        operator: allowed[0],
                        value: '',
                      }
                      setConditions(updated)
                      ensureColumnValuesLoaded(nextColumn)
                    }}
                  >
                    <option value="">Select column</option>
                    {columns.map((column) => (
                      <option key={column.name} value={column.name}>
                        {column.name}
                      </option>
                    ))}
                  </select>

                  <select
                    value={condition.property_filter.operator}
                    onChange={(e) => {
                      const updated = [...conditions]
                      updated[index].property_filter = {
                        ...updated[index].property_filter,
                        operator: e.target.value,
                      }
                      setConditions(updated)
                    }}
                  >
                    {getAllowedOperators(condition.property_filter.column).map((operator) => (
                      <option key={operator} value={operator}>
                        {operator}
                      </option>
                    ))}
                  </select>

                  <input
                    value={condition.property_filter.value}
                    onChange={(e) => {
                      const updated = [...conditions]
                      updated[index].property_filter = {
                        ...updated[index].property_filter,
                        value: e.target.value,
                      }
                      setConditions(updated)
                    }}
                    list={`cohort-filter-values-${index}`}
                    onFocus={() => ensureColumnValuesLoaded(condition.property_filter.column)}
                  />
                  <datalist id={`cohort-filter-values-${index}`}>
                    {(valueCache[condition.property_filter.column]?.values || []).map((value) => (
                      <option key={value} value={value} />
                    ))}
                  </datalist>

                  <button
                    className="cohort-condition-remove"
                    type="button"
                    onClick={() => {
                      const updated = [...conditions]
                      updated[index].property_filter = null
                      setConditions(updated)
                    }}
                  >
                    Remove Filter
                  </button>
                </div>
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
      ))}

      <button
        className="cohort-add-condition"
        type="button"
        disabled={conditions.length >= 5}
        onClick={() => setConditions([...conditions, { event_name: events[0] || '', min_event_count: 1, property_filter: null }])}
      >
        + Add Condition
      </button>

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
                  {(cohort.conditions || []).map((c, index) => (
                    <div key={index}>
                      {c.event_name}
                      {c.property_filter
                        ? ` WHERE ${c.property_filter.column} ${c.property_filter.operator} ${c.property_filter.value}`
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
