import { useEffect, useMemo, useState } from 'react'
import { getUsage, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'

function formatRatioValue(value) {
  return Number(value).toFixed(2)
}

const MAX_DAY_DETECTION_WINDOW = 365

export default function UsageTable({ refreshToken, retentionEvent, maxDay }) {
  const [event, setEvent] = useState('')
  const [effectiveMaxDayVolume, setEffectiveMaxDayVolume] = useState(() => Number(maxDay))
  const [effectiveMaxDayUsers, setEffectiveMaxDayUsers] = useState(() => Number(maxDay))
  const [isPinned, setIsPinned] = useState(true)
  const [modeUsers, setModeUsers] = useState('count')
  const [metricType, setMetricType] = useState('count')
  const [events, setEvents] = useState([])
  const [volumeRows, setVolumeRows] = useState([])
  const [userRows, setUserRows] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')


  const loadUsage = async (selectedEvent = event, overrideMaxDay) => {
    if (!selectedEvent) {
      setVolumeRows([])
      setUserRows([])
      setRetainedRows([])
      return
    }

    if (retentionEvent === undefined || retentionEvent === null || retentionEvent === '') {
      setError('Retention event must be selected before loading usage metrics')
      setVolumeRows([])
      setUserRows([])
      setRetainedRows([])
      return
    }

    setLoading(true)
    setError('')
    try {
      const response = await getUsage(selectedEvent, Number(overrideMaxDay ?? maxDay), retentionEvent)
      setVolumeRows(response.usage_volume_table || [])
      setUserRows(response.usage_users_table || [])
      setRetainedRows(response.retained_users_table || [])
    } catch (err) {
      setError(err.message)
      setVolumeRows([])
      setUserRows([])
      setRetainedRows([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    const refresh = async () => {
      try {
        const response = await listEvents()
        const nextEvents = response.events || []
        setEvents(nextEvents)
        const initialEvent = nextEvents[0] || ''
        setEvent((current) => (current && nextEvents.includes(current) ? current : initialEvent))

        // Trigger an initial fetch with a larger window to detect the appropriate maxDay
        if (initialEvent && (retentionEvent !== undefined && retentionEvent !== null && retentionEvent !== '')) {
          loadUsage(initialEvent, MAX_DAY_DETECTION_WINDOW)
        }
      } catch (err) {
        setEvents([])
        setEvent('')
      }
    }
    refresh()
  }, [refreshToken, retentionEvent])

  useEffect(() => {
    if (event) {
      loadUsage(event)
    } else {
      setVolumeRows([])
      setUserRows([])
      setRetainedRows([])
    }
  }, [event, maxDay, retentionEvent])

  const dayColumns = useMemo(() => Array.from({ length: Number(maxDay) + 1 }, (_, index) => index), [maxDay])

  const userDisplayRows = useMemo(
    () =>
      userRows.map((row) => {
        if (modeUsers === 'count') {
          return row
        }

        const converted = {}
        for (const day of dayColumns) {
          const rawValue = Number(row.values?.[String(day)] ?? 0)
          const percent = row.size > 0 ? (rawValue / row.size) * 100 : 0
          converted[String(day)] = formatRatioValue(percent)
        }

        return { ...row, values: converted }
      }),
    [dayColumns, modeUsers, userRows]
  )

  const volumeDisplayRows = useMemo(() => {
    const usersByCohort = new Map(userRows.map((row) => [row.cohort_id, row.values || {}]))
    const retainedByCohort = new Map(retainedRows.map((row) => [row.cohort_id, row.values || {}]))

    return volumeRows.map((row) => {
      if (metricType === 'count') {
        return row
      }

      const converted = {}
      const usersByDay = usersByCohort.get(row.cohort_id) || {}
      const retainedByDay = retainedByCohort.get(row.cohort_id) || {}

      for (const day of dayColumns) {
        const totalEvents = Number(row.values?.[String(day)] ?? 0)
        const distinctUsers = Number(usersByDay[String(day)] ?? 0)
        const retainedUsers = Number(retainedByDay[String(day)] ?? 0)

        if (metricType === 'per_event_firer') {
          converted[String(day)] = formatRatioValue(distinctUsers > 0 ? totalEvents / distinctUsers : 0)
        } else {
          converted[String(day)] = formatRatioValue(retainedUsers > 0 ? totalEvents / retainedUsers : 0)
        }
      }

      return { ...row, values: converted }
    })
  }, [dayColumns, metricType, retainedRows, userRows, volumeRows])

  const volumeLabel =
    metricType === 'count'
      ? 'Event Count'
      : metricType === 'per_event_firer'
        ? 'Events per Event Firer'
        : 'Events per Active User'

  useEffect(() => {
    setEffectiveMaxDayVolume(Number(maxDay))
    setEffectiveMaxDayUsers(Number(maxDay))
  }, [maxDay])

  const dayColumnsVolume = useMemo(
    () => Array.from({ length: Number(effectiveMaxDayVolume) + 1 }, (_, index) => index),
    [effectiveMaxDayVolume]
  )
  const dayColumnsUsers = useMemo(
    () => Array.from({ length: Number(effectiveMaxDayUsers) + 1 }, (_, index) => index),
    [effectiveMaxDayUsers]
  )

  return (
    <section className="card">
      <h2>6. Usage Analytics</h2>
      <div className="inline-controls">
        <label>
          Usage Event
          <SearchableSelect
            options={events}
            value={event}
            onChange={setEvent}
            placeholder="Select an event"
          />
        </label>
        <label>
          Unique Users
          <select value={modeUsers} onChange={(e) => setModeUsers(e.target.value)}>
            <option value="count">Count</option>
            <option value="percent">%</option>
          </select>
        </label>
        <label>
          Metric
          <select value={metricType} onChange={(e) => setMetricType(e.target.value)}>
            <option value="count">Count</option>
            <option value="per_active_user">Per Active User</option>
            <option value="per_event_firer">Per Event Firer</option>
          </select>
        </label>
        <button
          className={`view-button ${isPinned ? 'active' : ''}`}
          onClick={() => setIsPinned((prev) => !prev)}
          title="Pin Cohort Columns"
        >
          {isPinned ? "📌" : "📍"}
        </button>
        <button className="button button-primary" onClick={() => loadUsage()} disabled={loading || !event || retentionEvent === undefined || retentionEvent === null || retentionEvent === ""}>
          {loading ? 'Loading...' : 'Load Usage'}
        </button>
      </div>

      {metricType === 'per_active_user' && (
        <p>Active users are calculated using the selected retention event.</p>
      )}

      {error && <p className="error">{error}</p>}

      <h3>Event Volume ({volumeLabel})</h3>
      {volumeDisplayRows.length > 0 && (
        <div className="analytics-table table-responsive">
          <table>
            <thead>
              <tr>
                <th className={isPinned ? 'sticky-col sticky-col-cohort' : ''}>Cohort</th>
                <th className={isPinned ? 'sticky-col sticky-col-size' : ''}>Size</th>
                {dayColumnsVolume.map((day) => (
                  <th key={day}>D{day}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {volumeDisplayRows.map((row) => (
                <tr key={row.cohort_id}>
                  <td
                    className={isPinned ? 'sticky-col sticky-col-cohort' : ''}
                    title={row.cohort_name}
                  >
                    {row.cohort_name}
                  </td>
                  <td className={isPinned ? 'sticky-col sticky-col-size' : ''}>{row.size}</td>
                  {dayColumnsVolume.map((day) => (
                    <td key={day}>{row.values?.[String(day)] ?? '—'}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <h3>Unique Users</h3>
      {userDisplayRows.length > 0 && (
        <div className="analytics-table table-responsive">
          <table>
            <thead>
              <tr>
                <th className={isPinned ? 'sticky-col sticky-col-cohort' : ''}>Cohort</th>
                <th className={isPinned ? 'sticky-col sticky-col-size' : ''}>Size</th>
                {dayColumnsUsers.map((day) => (
                  <th key={day}>D{day}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {userDisplayRows.map((row) => (
                <tr key={row.cohort_id}>
                  <td
                    className={isPinned ? 'sticky-col sticky-col-cohort' : ''}
                    title={row.cohort_name}
                  >
                    {row.cohort_name}
                  </td>
                  <td className={isPinned ? 'sticky-col sticky-col-size' : ''}>{row.size}</td>
                  {dayColumnsUsers.map((day) => {
                    const value = row.values?.[String(day)] ?? null
                    return <td key={day}>{value === null ? '—' : (modeUsers === 'percent' ? `${value}%` : value)}</td>
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
