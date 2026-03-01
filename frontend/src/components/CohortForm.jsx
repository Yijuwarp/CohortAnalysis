import { useEffect, useState } from 'react'
import { createCohort, deleteCohort, listCohorts, listEvents } from '../api'

export default function CohortForm({ refreshToken, onCohortsChanged }) {
  const [name, setName] = useState('')
  const [conditions, setConditions] = useState([{ event_name: '', min_event_count: 1 }])
  const [logicOperator, setLogicOperator] = useState('AND')
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [cohorts, setCohorts] = useState([])
  const [deletingId, setDeletingId] = useState(null)
  const [events, setEvents] = useState([])

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
        const response = await listEvents()
        const eventList = response.events || []
        setEvents(eventList)
        setConditions((prev) =>
          prev.map((condition) => {
            if (eventList.length === 0) {
              return { ...condition, event_name: '' }
            }

            if (condition.event_name && eventList.includes(condition.event_name)) {
              return condition
            }

            return { ...condition, event_name: eventList[0] }
          })
        )
      } catch {
        setEvents([])
        setConditions((prev) => prev.map((condition) => ({ ...condition, event_name: '' })))
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
      const data = await createCohort({
        name,
        logic_operator: logicOperator,
        conditions,
      })
      setResult(data)
      setName('')
      setConditions([{ event_name: events[0] || '', min_event_count: 1 }])
      setLogicOperator('AND')
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

  return (
    <section className="card">
      <h2>4. Create Cohort</h2>
      <div className="grid">
        <label>
          Cohort Name
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </label>
      </div>

      <h4>Conditions</h4>
      {conditions.map((condition, index) => (
        <div key={index} className="condition-row">
          {index > 0 && (
            <div className="operator-row">
              <select value={logicOperator} onChange={(e) => setLogicOperator(e.target.value)}>
                <option value="AND">AND</option>
                <option value="OR">OR</option>
              </select>
            </div>
          )}

          <div className="condition-layout">
            <div>
              <label className="tertiary-label">Event Name</label>
              <select
                value={condition.event_name}
                onChange={(e) => {
                  const updated = [...conditions]
                  updated[index].event_name = e.target.value
                  setConditions(updated)
                }}
              >
                {events.map((event) => (
                  <option key={event} value={event}>
                    {event}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="tertiary-label">Min Event Count</label>
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
            </div>

            {conditions.length > 1 && (
              <button
                className="button button-danger"
                type="button"
                onClick={() => {
                  const updated = conditions.filter((_, i) => i !== index)
                  setConditions(updated)
                }}
              >
                Remove
              </button>
            )}
          </div>
        </div>
      ))}

      <div className="inline-controls">
        <button
          className="button button-secondary"
          type="button"
          disabled={conditions.length >= 5}
          onClick={() => setConditions([...conditions, { event_name: events[0] || '', min_event_count: 1 }])}
        >
          + Add Condition
        </button>

        <button className="button button-primary" onClick={handleSubmit} disabled={loading || events.length === 0}>
          {loading ? 'Creating...' : 'Create Cohort'}
        </button>
      </div>
      {events.length === 0 && <p className="error">No events available under current filters</p>}
      {error && <p className="error">{error}</p>}
      {result && (
        <p className="success">
          Created cohort #{result.cohort_id} with {result.users_joined} users joined.
        </p>
      )}

      <h3>Existing Cohorts</h3>
      {cohorts.length === 0 ? (
        <p className="secondary-text">No cohorts created yet.</p>
      ) : (
        <ul>
          {cohorts.map((cohort) => (
            <li
              key={cohort.cohort_id}
              title={cohort.is_active ? '' : 'No matching members under current filters'}
              className="secondary-text" style={{ opacity: cohort.is_active ? 1 : 0.5 }}
            >
              {cohort.cohort_name}
              <button
                className="button button-danger"
                type="button"
                onClick={() => handleDelete(cohort.cohort_id)}
                disabled={deletingId === cohort.cohort_id}
              >
                {deletingId === cohort.cohort_id ? 'Deleting...' : 'Delete'}
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
