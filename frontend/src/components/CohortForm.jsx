import { useEffect, useState } from 'react'
import { createCohort, deleteCohort, getRetention, listEvents } from '../api'

export default function CohortForm({ onCohortsChanged }) {
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
      const response = await getRetention(0)
      setCohorts(response.retention_table.map((row) => ({ cohort_id: row.cohort_id, cohort_name: row.cohort_name })))
    } catch {
      setCohorts([])
    }
  }

  useEffect(() => {
    loadCohorts()
  }, [])

  useEffect(() => {
    const load = async () => {
      try {
        const response = await listEvents()
        const eventList = response.events || []
        setEvents(eventList)
        if (eventList.length > 0) {
          setConditions((prev) => prev.map((condition) => ({ ...condition, event_name: condition.event_name || eventList[0] })))
        }
      } catch {
        setEvents([])
      }
    }
    load()
  }, [])

  const handleSubmit = async () => {
    setError('')
    setResult(null)

    if (!name.trim()) {
      setError('Cohort name is required')
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
      <h2>3. Create Cohort</h2>
      <div className="grid">
        <label>
          Cohort Name
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </label>
      </div>

      <h4 style={{ marginTop: '1rem' }}>Conditions</h4>
      {conditions.map((condition, index) => (
        <div key={index} className="condition-row" style={{ marginBottom: '0.75rem' }}>
          {index > 0 && (
            <div style={{ marginBottom: '0.5rem' }}>
              <select value={logicOperator} onChange={(e) => setLogicOperator(e.target.value)}>
                <option value="AND">AND</option>
                <option value="OR">OR</option>
              </select>
            </div>
          )}

          <div style={{ display: 'flex', gap: '1rem', alignItems: 'flex-end' }}>
            <div>
              <label style={{ display: 'block', fontSize: '0.85rem' }}>Event Name</label>
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
              <label style={{ display: 'block', fontSize: '0.85rem' }}>Min Event Count</label>
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
                type="button"
                onClick={() => {
                  const updated = conditions.filter((_, i) => i !== index)
                  setConditions(updated)
                }}
                style={{ marginLeft: '0.5rem' }}
              >
                Remove
              </button>
            )}
          </div>
        </div>
      ))}

      <button
        type="button"
        disabled={conditions.length >= 5}
        onClick={() => setConditions([...conditions, { event_name: events[0] || '', min_event_count: 1 }])}
      >
        + Add Condition
      </button>

      <button onClick={handleSubmit} disabled={loading}>
        {loading ? 'Creating...' : 'Create Cohort'}
      </button>
      {error && <p className="error">{error}</p>}
      {result && (
        <p className="success">
          Created cohort #{result.cohort_id} with {result.users_joined} users joined.
        </p>
      )}

      <h3>Existing Cohorts</h3>
      {cohorts.length === 0 ? (
        <p>No cohorts created yet.</p>
      ) : (
        <ul>
          {cohorts.map((cohort) => (
            <li key={cohort.cohort_id}>
              {cohort.cohort_name}
              <button
                type="button"
                onClick={() => handleDelete(cohort.cohort_id)}
                disabled={deletingId === cohort.cohort_id}
                style={{ marginLeft: '0.5rem' }}
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
