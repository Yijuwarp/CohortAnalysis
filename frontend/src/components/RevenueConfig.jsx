import { useEffect, useMemo, useState } from 'react'
import { getRevenueConfigEvents, updateRevenueConfig } from '../api'

export default function RevenueConfig({ refreshToken, onUpdated }) {
  const [hasRevenueMapping, setHasRevenueMapping] = useState(false)
  const [availableRevenueEvents, setAvailableRevenueEvents] = useState([])
  const [pendingRevenueConfig, setPendingRevenueConfig] = useState({})
  const [pendingOverrideInputs, setPendingOverrideInputs] = useState({})
  const [eventToAdd, setEventToAdd] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')

  useEffect(() => {
    const run = async () => {
      try {
        const payload = await getRevenueConfigEvents()
        const events = payload.events || []
        setHasRevenueMapping(Boolean(payload.has_revenue_mapping))
        setAvailableRevenueEvents(events.map((event) => event.event_name))
        const config = events.reduce((acc, event) => ({
          ...acc,
          [event.event_name]: { included: Boolean(event.included), override: event.override ?? null },
        }), {})
        setPendingRevenueConfig(config)
        setPendingOverrideInputs(events.reduce((acc, event) => ({
          ...acc,
          [event.event_name]: event.override === null || event.override === undefined ? '' : String(event.override),
        }), {}))
      } catch {
        setHasRevenueMapping(false)
        setAvailableRevenueEvents([])
        setPendingRevenueConfig({})
        setPendingOverrideInputs({})
      }
    }

    run()
  }, [refreshToken])

  const invalidOverrideEvents = useMemo(
    () => Object.entries(pendingOverrideInputs)
      .filter(([, value]) => value !== '' && !Number.isFinite(Number(value)))
      .map(([eventName]) => eventName),
    [pendingOverrideInputs],
  )

  const addableRevenueEvents = useMemo(
    () => availableRevenueEvents.filter((eventName) => !pendingRevenueConfig[eventName]),
    [availableRevenueEvents, pendingRevenueConfig],
  )

  const canApplyRevenueChanges = invalidOverrideEvents.length === 0 && !saving

  if (!hasRevenueMapping) {
    return null
  }

  const handleOverrideChange = (eventName, value) => {
    setPendingOverrideInputs((previous) => ({ ...previous, [eventName]: value }))

    if (value === '') {
      setPendingRevenueConfig((previous) => ({
        ...previous,
        [eventName]: {
          ...previous[eventName],
          override: null,
        },
      }))
      return
    }

    const parsed = Number(value)
    if (!Number.isFinite(parsed)) {
      return
    }

    setPendingRevenueConfig((previous) => ({
      ...previous,
      [eventName]: {
        ...previous[eventName],
        override: parsed,
      },
    }))
  }

  const handleApply = async () => {
    if (!canApplyRevenueChanges) {
      return
    }

    setSaving(true)
    setError('')
    setMessage('')
    try {
      const payload = await updateRevenueConfig(pendingRevenueConfig)
      const events = payload.events || []
      setHasRevenueMapping(Boolean(payload.has_revenue_mapping))
      setAvailableRevenueEvents(events.map((event) => event.event_name))
      setPendingRevenueConfig(events.reduce((acc, event) => ({
        ...acc,
        [event.event_name]: { included: Boolean(event.included), override: event.override ?? null },
      }), {}))
      setPendingOverrideInputs(events.reduce((acc, event) => ({
        ...acc,
        [event.event_name]: event.override === null || event.override === undefined ? '' : String(event.override),
      }), {}))
      setMessage('Revenue config updated. Reloading monetization...')
      if (onUpdated) {
        onUpdated()
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <section className="card">
      <h2>2.5 Revenue Configuration</h2>
      <div className="revenue-config-panel">
        <div className="revenue-config-add">
          <label>
            Add Revenue Event
            <select value={eventToAdd} onChange={(e) => setEventToAdd(e.target.value)}>
              <option value="">Select event</option>
              {addableRevenueEvents.map((eventName) => (
                <option key={eventName} value={eventName}>{eventName}</option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className="button button-secondary"
            onClick={() => {
              if (!eventToAdd) return
              setPendingRevenueConfig((previous) => ({
                ...previous,
                [eventToAdd]: { included: true, override: null },
              }))
              setPendingOverrideInputs((previous) => ({ ...previous, [eventToAdd]: '' }))
              setEventToAdd('')
            }}
            disabled={!eventToAdd}
          >
            Add Revenue Event +
          </button>
        </div>

        <table className="revenue-config-table">
          <thead>
            <tr>
              <th>Event Name</th>
              <th>Include</th>
              <th>Override ($)</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(pendingRevenueConfig).map(([eventName, config]) => {
              const isInvalid = invalidOverrideEvents.includes(eventName)
              return (
                <tr key={eventName}>
                  <td>{eventName}</td>
                  <td>
                    <input
                      type="checkbox"
                      checked={config.included}
                      onChange={(e) => setPendingRevenueConfig((previous) => ({
                        ...previous,
                        [eventName]: {
                          ...previous[eventName],
                          included: e.target.checked,
                        },
                      }))}
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={pendingOverrideInputs[eventName] ?? ''}
                      onChange={(e) => handleOverrideChange(eventName, e.target.value)}
                      placeholder="Leave blank for original"
                      disabled={!config.included}
                      className={isInvalid ? 'invalid-number-input' : ''}
                    />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>

        <p className="muted-text">(Blank override sends null and reverts to original revenue)</p>
        <button type="button" className="button button-primary" onClick={handleApply} disabled={!canApplyRevenueChanges}>
          {saving ? 'Updating...' : 'Update Monetization'}
        </button>
        {message && <p className="success">{message}</p>}
        {error && <p className="error">{error}</p>}
      </div>
    </section>
  )
}
