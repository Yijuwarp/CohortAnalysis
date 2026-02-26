import { useEffect, useState } from 'react'
import { createCohort, deleteCohort, getRetention } from '../api'

export default function CohortForm({ onCohortsChanged }) {
  const [payload, setPayload] = useState({
    name: '',
    event_name: '',
    min_event_count: 1,
  })
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [cohorts, setCohorts] = useState([])
  const [deletingId, setDeletingId] = useState(null)

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

  const handleSubmit = async () => {
    setLoading(true)
    setError('')
    setResult(null)

    try {
      const data = await createCohort({
        ...payload,
        min_event_count: Number(payload.min_event_count),
      })
      setResult(data)
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
          <input value={payload.name} onChange={(e) => setPayload((prev) => ({ ...prev, name: e.target.value }))} />
        </label>
        <label>
          Event Name
          <input value={payload.event_name} onChange={(e) => setPayload((prev) => ({ ...prev, event_name: e.target.value }))} />
        </label>
        <label>
          Min Event Count
          <input
            type="number"
            min="1"
            value={payload.min_event_count}
            onChange={(e) => setPayload((prev) => ({ ...prev, min_event_count: e.target.value }))}
          />
        </label>
      </div>
      <button onClick={handleSubmit} disabled={loading}>{loading ? 'Creating...' : 'Create Cohort'}</button>
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
