import { useState } from 'react'
import { createCohort } from '../api'

export default function CohortForm() {
  const [payload, setPayload] = useState({
    name: '',
    event_name: '',
    min_event_count: 1,
  })
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

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
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
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
    </section>
  )
}
