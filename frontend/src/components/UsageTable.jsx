import { useEffect, useMemo, useState, useRef } from 'react'
import { getAvailabilityStyle } from '../utils/style_utils'
import { getEventProperties, getEventPropertyValues, getUsage, listEvents, getUsageFrequency } from '../api'
import { formatSplitValue } from '../utils/formatters'
import SearchableSelect from './SearchableSelect'
import UsageFrequencyHistogram from './UsageFrequencyHistogram'
import ComparePane from './ComparePane'

function computeCumulative(values) {
  let running = 0
  const result = {}

  Object.keys(values)
    .sort((a, b) => Number(a) - Number(b))
    .forEach((day) => {
      running += Number(values[day] || 0)
      result[day] = running
    })

  return result
}

function formatRatioValue(value) {
  return Number(value).toFixed(2)
}

function formatCountValue(value) {
  return Number(value).toLocaleString()
}

export default function UsageTable({ refreshToken, retentionEvent, maxDay, state, setState, scopeVersion, cohorts = [], appliedFilters = [], onAddToExport }) {
  const [event, setEvent] = useState(state?.event || '')
  const [effectiveMaxDayVolume, setEffectiveMaxDayVolume] = useState(() => Number(state?.effectiveMaxDayVolume || maxDay))
  const [effectiveMaxDayUsers, setEffectiveMaxDayUsers] = useState(() => Number(state?.effectiveMaxDayUsers || maxDay))
  const [isPinned, setIsPinned] = useState(state?.isPinned ?? true)
  const [modeUsers, setModeUsers] = useState(state?.modeUsers || 'count')
  const [metricType, setMetricType] = useState(state?.metricType || 'count')
  const [cumulativeMode, setCumulativeMode] = useState(state?.cumulativeMode ?? false)
  const [columnWidths, setColumnWidths] = useState(state?.columnWidths || {
    cohort: 160,
    split: 160,
    size: 80
  })
  const isResizingRef = useRef(false)
  const [frequencyData, setFrequencyData] = useState(null)
  const [frequencyLoading, setFrequencyLoading] = useState(false)
  const [localRefreshToken, setLocalRefreshToken] = useState(0)

  const getStickyLeft = (colKey) => {
    if (colKey === "cohort") return 0
    if (colKey === "split") return columnWidths.cohort
    if (colKey === "size") {
      return columnWidths.cohort + (showSplit ? columnWidths.split : 0)
    }
    return 0
  }

  const startResize = (colKey, e) => {
    e.preventDefault()
    e.stopPropagation()
    
    isResizingRef.current = true

    const startX = e.clientX
    const startWidth = columnWidths[colKey] || (colKey === 'size' ? 80 : 160)

    const onMouseMove = (moveEvent) => {
      const minWidth = colKey === "size" ? 80 : 120
      const newWidth = Math.max(minWidth, startWidth + (moveEvent.clientX - startX))
      setColumnWidths(prev => ({ ...prev, [colKey]: newWidth }))
    }

    const onMouseUp = () => {
      window.removeEventListener("mousemove", onMouseMove)
      window.removeEventListener("mouseup", onMouseUp)
      document.body.classList.remove("resizing")

      setTimeout(() => {
        isResizingRef.current = false
      }, 0)
    }

    window.addEventListener("mousemove", onMouseMove)
    window.addEventListener("mouseup", onMouseUp)
    document.body.classList.add("resizing")
  }
  const [events, setEvents] = useState([])

  const [eventProperties, setEventProperties] = useState([])
  const [eventProperty, setEventProperty] = useState(state?.eventProperty || '')
  const [propertyOperator, setPropertyOperator] = useState(state?.propertyOperator || '=')
  const [propertyValues, setPropertyValues] = useState([])
  const [propertyValue, setPropertyValue] = useState(state?.propertyValue || '')
  const [propertyLoading, setPropertyLoading] = useState(false)
  const [showPropertyFilter, setShowPropertyFilter] = useState(state?.showPropertyFilter ?? false)

  useEffect(() => {
    const nextState = {
      event,
      effectiveMaxDayVolume,
      effectiveMaxDayUsers,
      isPinned,
      modeUsers,
      metricType,
      cumulativeMode,
      eventProperty,
      propertyOperator,
      propertyValue,
      showPropertyFilter,
      columnWidths
    }
    setState(nextState)
  }, [event, effectiveMaxDayVolume, effectiveMaxDayUsers, isPinned, modeUsers, metricType, cumulativeMode, eventProperty, propertyOperator, propertyValue, showPropertyFilter, columnWidths])
  const [volumeRows, setVolumeRows] = useState([])
  const [userRows, setUserRows] = useState([])
  const [retainedRows, setRetainedRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [isComparePaneOpen, setIsComparePaneOpen] = useState(false)
  const [sortConfigVolume, setSortConfigVolume] = useState({ key: 'size', direction: 'desc' })
  const [sortConfigUsers, setSortConfigUsers] = useState({ key: 'size', direction: 'desc' })

  const cohortMetaMap = useMemo(() => {
    const map = {}
    ;(cohorts || []).forEach(c => {
      map[c.cohort_id] = c
    })
    return map
  }, [cohorts])

  const getSplitLabel = (row) => {
    const cohort = cohortMetaMap[row.cohort_id]
    if (!cohort?.split_type) return "NA"

    if (cohort.split_type === "random") {
      return `Group ${cohort.split_value}`
    }

    if (cohort.split_type === "property") {
      if (cohort.split_value === "__OTHER__") return `${cohort.split_property} = Other`
      return `${cohort.split_property} = ${formatSplitValue(cohort.split_value)}`
    }

    return "NA"
  }

  const getDisplayName = (row) => {
    const cohort = cohortMetaMap[row.cohort_id]
    if (cohort?.split_parent_cohort_id) {
      const parent = cohortMetaMap[cohort.split_parent_cohort_id]
      return parent?.cohort_name || parent?.name || row.cohort_name
    }
    return row.cohort_name
  }

  const getSortValue = (row, key) => {
    if (key === 'cohort_name') return row.cohort_name
    if (key === 'split') return getSplitLabel(row)
    if (key === 'size') return row.size || 0
    if (key.startsWith('D')) {
      const day = key.slice(1)
      return row.values?.[day] || 0
    }
    return 0
  }

  const handleSortVolume = (key) => {
    setSortConfigVolume(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }))
  }

  const handleSortUsers = (key) => {
    setSortConfigUsers(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }))
  }

  const sortRows = (rows, config) => {
    const { key, direction } = config
    const sorted = [...(rows || [])]
    return sorted.sort((a, b) => {
      const valA = getSortValue(a, key)
      const valB = getSortValue(b, key)

      if (key === 'split') {
        const isOtherA = valA.endsWith(' = Other') || valA === 'Other'
        const isOtherB = valB.endsWith(' = Other') || valB === 'Other'
        if (isOtherA && !isOtherB) return 1
        if (isOtherB && !isOtherA) return -1
        if (valA === "NA" && valB !== "NA") return 1
        if (valB === "NA" && valA !== "NA") return -1
      }

      const order = direction === 'asc' ? 1 : -1
      if (typeof valA === 'string') {
        return valA.localeCompare(valB) * order
      }
      if (valA < valB) return -1 * order
      if (valA > valB) return 1 * order
      return 0
    })
  }

  const propertyFilter = eventProperty ? { property: eventProperty, operator: propertyOperator, value: propertyValue } : null
  const propertyFilterRequiresValue = Boolean(eventProperty && !propertyValue)

  const clearPropertyFilter = () => {
    setEventProperty('')
    setPropertyOperator('=')
    setPropertyValue('')
    setPropertyValues([])
    setShowPropertyFilter(false)
  }

  const handleEventChange = (nextEvent) => {
    clearPropertyFilter()
    setEvent(nextEvent)
    setLocalRefreshToken((prev) => prev + 1)
  }

  useEffect(() => {
    if (!event) {
      setFrequencyData(null)
      return
    }
    setFrequencyLoading(true)
    getUsageFrequency(event, propertyFilter)
      .then(res => setFrequencyData(res))
      .catch(() => setFrequencyData(null))
      .finally(() => setFrequencyLoading(false))
  }, [event, propertyFilter?.property, propertyFilter?.operator, propertyFilter?.value, localRefreshToken])

  const loadUsage = async (selectedEvent = event) => {
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

    if (propertyFilterRequiresValue) {
      setError('Select a property value before loading usage metrics')
      return
    }

    setLoading(true)
    setError('')
    try {
      const response = await getUsage(selectedEvent, Number(maxDay), retentionEvent, propertyFilter)
      setVolumeRows(response.usage_volume_table || [])
      const adoptionRows = response.usage_adoption_table || []
      const adoptionByCohort = new Map(adoptionRows.map((row) => [row.cohort_id, row.values || {}]))
      setUserRows((response.usage_users_table || []).map((row) => ({
        ...row,
        adoption_values: adoptionByCohort.get(row.cohort_id) || {},
      })))
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
      } catch (err) {
        setEvents([])
        setEvent('')
      }
    }
    refresh()
  }, [refreshToken, retentionEvent])

  useEffect(() => {
    if (event && !propertyFilterRequiresValue) {
      loadUsage(event)
    } else if (!event) {
      setVolumeRows([])
      setUserRows([])
      setRetainedRows([])
    }
  }, [event, maxDay, retentionEvent, scopeVersion, eventProperty, propertyOperator, propertyValue])

  useEffect(() => {
    if (!event) {
      setEventProperties([])
      clearPropertyFilter()
      return
    }

    let isMounted = true
    setPropertyLoading(true)

    getEventProperties(event)
      .then((response) => {
        if (!isMounted) return
        const properties = response.properties || []
        setEventProperties(properties)
        setEventProperty((current) => (current && properties.includes(current) ? current : ''))
      })
      .catch(() => {
        if (!isMounted) return
        setEventProperties([])
        clearPropertyFilter()
      })
      .finally(() => {
        if (isMounted) setPropertyLoading(false)
      })

    return () => {
      isMounted = false
    }
  }, [event])

  useEffect(() => {
    if (eventProperty) {
      setShowPropertyFilter(true)
    }
  }, [eventProperty])

  useEffect(() => {
    if (!event || !eventProperty) {
      setPropertyValues([])
      setPropertyValue('')
      return
    }

    let isMounted = true
    getEventPropertyValues(event, eventProperty)
      .then((response) => {
        if (!isMounted) return
        const nextValues = response.values || []
        setPropertyValues(nextValues)
        setPropertyValue((current) => (current && nextValues.includes(current) ? current : ''))
      })
      .catch(() => {
        if (!isMounted) return
        setPropertyValues([])
        setPropertyValue('')
      })

    return () => {
      isMounted = false
    }
  }, [event, eventProperty])

  const dayColumns = useMemo(() => Array.from({ length: Number(maxDay) + 1 }, (_, index) => index), [maxDay])

  const userDisplayRows = useMemo(
    () =>
      userRows.map((row) => {
        let baseValues = row.values || {}

        if (modeUsers === 'adoption_count' || modeUsers === 'adoption_percent') {
          baseValues = row.adoption_values || {}
        }

        if (modeUsers === 'count' || modeUsers === 'adoption_count') {
          return { ...row, values: baseValues }
        }

        const converted = {}
        for (const day of dayColumns) {
          const rawValue = Number(baseValues[String(day)] ?? 0)
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
      const converted = {}
      const usersByDay = usersByCohort.get(row.cohort_id) || {}
      const retainedByDay = retainedByCohort.get(row.cohort_id) || {}
      const eventValues = cumulativeMode ? computeCumulative(row.values || {}) : row.values || {}

      for (const day of dayColumns) {
        const totalEvents = Number(eventValues[String(day)] ?? 0)
        const distinctUsers = Number(usersByDay[String(day)] ?? 0)
        const retainedUsers = Number(retainedByDay[String(day)] ?? 0)

        if (metricType === 'count') {
          converted[String(day)] = totalEvents
          continue
        }

        if (metricType === 'per_event_firer') {
          converted[String(day)] = formatRatioValue(distinctUsers > 0 ? totalEvents / distinctUsers : 0)
          continue
        }

        if (metricType === 'per_installed_user') {
          converted[String(day)] = formatRatioValue(row.size > 0 ? totalEvents / row.size : 0)
          continue
        }

        if (metricType === 'per_active_user') {
          converted[String(day)] = formatRatioValue(retainedUsers > 0 ? totalEvents / retainedUsers : 0)
          continue
        }

        converted[String(day)] = '0.00'
      }

      return { ...row, values: converted }
    })
  }, [cumulativeMode, dayColumns, metricType, retainedRows, userRows, volumeRows])

  const showSplit = useMemo(() => {
    return (cohorts || []).some(c => c.split_type != null)
  }, [cohorts])

  const sortedVolumeRows = useMemo(() => sortRows(volumeDisplayRows, sortConfigVolume), [volumeDisplayRows, sortConfigVolume, cohorts])
  const sortedUserRows = useMemo(() => sortRows(userDisplayRows, sortConfigUsers), [userDisplayRows, sortConfigUsers, cohorts])

  const cumulativeSupported = metricType === 'count' || metricType === 'per_installed_user'

  useEffect(() => {
    if (!cumulativeSupported) {
      setCumulativeMode(false)
    }
  }, [cumulativeSupported])

  const volumeLabel =
    metricType === 'count'
      ? 'Event Count'
      : metricType === 'per_installed_user'
        ? 'Events per Installed User'
      : metricType === 'per_event_firer'
        ? 'Events per Event Firer'
        : 'Events per Retained User'

  const uniqueUsersLabel =
    modeUsers === 'count'
      ? 'Daily Users (Count)'
      : modeUsers === 'percent'
        ? 'Daily Users (%)'
        : modeUsers === 'adoption_count'
          ? 'Cumulative Adoption (Count)'
          : 'Cumulative Adoption (%)'

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

  const handleAddToExport = () => {
    // 1. Volume Table
    const volumeTable = {
      title: `Event Volume (${volumeLabel}${cumulativeMode ? ' - Cumulative' : ''})`,
      columns: [
        { key: 'cohort', label: 'Cohort', type: 'string' },
        { key: 'split', label: 'Split', type: 'string' },
        { key: 'size', label: 'Size', type: 'number' },
        ...dayColumnsVolume.map(d => ({ key: `D${d}`, label: `D${d}`, type: metricType === 'count' ? 'number' : 'string' }))
      ],
      data: sortedVolumeRows.map(row => {
        const rowObj = {
          cohort: getDisplayName(row),
          split: getSplitLabel(row),
          size: row.size
        }
        dayColumnsVolume.forEach(d => {
          const val = row.values?.[String(d)]
          rowObj[`D${d}`] = val !== null && val !== undefined ? Number(val) : null
        })
        return rowObj
      })
    }

    // 2. Selected Unique Users Table
    let usersTable = null
    const userLabelPrefix = modeUsers.includes('adoption') ? 'Cumulative Adoption' : 'Daily Users'
    const userLabelSuffix = modeUsers.includes('percent') ? '(%)' : '(Count)'
    const isPercent = modeUsers.includes('percent')

    usersTable = {
      title: `Unique Users (${userLabelPrefix} ${userLabelSuffix})`,
      columns: [
        { key: 'cohort', label: 'Cohort', type: 'string' },
        { key: 'split', label: 'Split', type: 'string' },
        { key: 'size', label: 'Size', type: 'number' },
        ...dayColumnsUsers.map(d => ({ 
          key: `D${d}`, 
          label: `D${d}`, 
          type: isPercent ? 'percentage' : 'number' 
        }))
      ],
      data: sortRows(userRows, sortConfigUsers).map(row => {
        const rowObj = {
          cohort: getDisplayName(row),
          split: getSplitLabel(row),
          size: row.size
        }
        dayColumnsUsers.forEach(d => {
          const val = row.values?.[String(d)]
          const adoptionVal = row.adoption_values?.[String(d)]
          const raw = modeUsers.includes('adoption') ? adoptionVal : val
          rowObj[`D${d}`] = raw !== null && raw !== undefined 
            ? (isPercent ? Number(raw) / row.size : Number(raw)) 
            : null
        })
        return rowObj
      })
    }

    // 3. Frequency Distribution Table
    let frequencyTable = null
    if (frequencyData && frequencyData.buckets && frequencyData.buckets.length > 0) {
      const cohortMeta = frequencyData.cohort_sizes.map(c => ({
        id: c.cohort_id,
        name: c.name || `Cohort ${c.cohort_id}`,
        size: Number(c.size) || 0
      }))

      frequencyTable = {
        title: 'Event Frequency Distribution',
        columns: [
          { key: 'bucket', label: 'Bucket', type: 'string' },
          ...cohortMeta.map(c => ({ 
            key: `cohort_${c.id}`, 
            label: `${c.name} (Users)`, 
            type: 'number' 
          })),
          ...cohortMeta.map(c => ({ 
            key: `cohort_${c.id}_pct`, 
            label: `${c.name} (%)`, 
            type: 'percentage' 
          }))
        ],
        data: frequencyData.buckets.map(bucketObj => {
          const rowObj = { bucket: bucketObj.bucket }
          const bucketByCohortId = new Map(bucketObj.cohorts.map(c => [c.cohort_id, Number(c.users) || 0]))
          
          cohortMeta.forEach(c => {
            const users = bucketByCohortId.get(c.id) || 0
            rowObj[`cohort_${c.id}`] = users
            rowObj[`cohort_${c.id}_pct`] = c.size > 0 ? users / c.size : 0
          })
          return rowObj
        })
      }
    }

    const payload = {
      id: crypto.randomUUID(),
      version: 2,
      type: 'usage',
      title: `Usage — ${event}`,
      summary: `Usage — ${event} • ${modeUsers.includes('adoption') ? 'Adoption' : 'Retained'} ${modeUsers.includes('percent') ? 'Percent' : 'Count'}`,
      tables: [volumeTable, usersTable, frequencyTable].filter(Boolean),
      meta: {
        filters: appliedFilters,
        cohorts: cohorts.filter(c => volumeRows.some(row => row.cohort_id === c.cohort_id)),
        settings: {
          'Event': event,
          'Metric': metricType,
          'Cumulative': cumulativeMode ? 'Enabled' : 'Disabled',
          'Max Day': maxDay,
          'Property Filter': eventProperty ? `${eventProperty} ${propertyOperator} ${propertyValue}` : 'None'
        }
      }
    }
    onAddToExport(payload)
  }

  return (
    <section className="card">
      <h2>Usage Analytics</h2>
      <div className="inline-controls">
        <label>
          Usage Event
          <SearchableSelect
            options={events}
            value={event}
            onChange={handleEventChange}
            placeholder="Select an event"
            className="searchable-select-prominent"
          />
        </label>
        <label>
          Unique Users
          <select value={modeUsers} onChange={(e) => setModeUsers(e.target.value)}>
            <option value="count">Daily Users (Count)</option>
            <option value="percent">Daily Users (%)</option>
            <option value="adoption_count">Cumulative Adoption (Count)</option>
            <option value="adoption_percent">Cumulative Adoption (%)</option>
          </select>
        </label>
        <label>
          Metric
          <select value={metricType} onChange={(e) => setMetricType(e.target.value)}>
            <option value="count">Count</option>
            <option value="per_active_user">Per Retained User</option>
            <option value="per_installed_user">Per Installed User</option>
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
        <button className="button button-primary" onClick={() => loadUsage()} disabled={loading || !event || propertyFilterRequiresValue || retentionEvent === undefined || retentionEvent === null || retentionEvent === ""}>
          {loading ? 'Loading...' : 'Load Usage'}
        </button>
        <button
          type="button"
          className={`compare-open-button ${isComparePaneOpen ? 'active' : ''}`}
          onClick={() => setIsComparePaneOpen(prev => !prev)}
          title="Compare two cohorts statistically"
          data-testid="open-compare-pane"
          disabled={!event}
        >
          🔬 Compare
        </button>

        <button
          type="button"
          className="button button-secondary"
          onClick={handleAddToExport}
          title="Add current view to global export buffer"
          disabled={!event || volumeRows.length === 0}
        >
          📸 Add to Export
        </button>
      </div>

      <div className="usage-property-filter">
        <div className="usage-property-filter-header">
          <span className="usage-property-filter-label">Where</span>
          {!showPropertyFilter && (
            <button
              type="button"
              className="button button-secondary button-small"
              onClick={() => setShowPropertyFilter(true)}
              disabled={propertyLoading || eventProperties.length === 0}
            >
              + Add Property
            </button>
          )}
        </div>
        {propertyLoading ? (
          <small className="secondary-text">Loading event properties...</small>
        ) : eventProperties.length === 0 ? (
          <small className="secondary-text">(No event properties available)</small>
        ) : showPropertyFilter ? (
          <div className="inline-controls">
            <SearchableSelect
              options={eventProperties}
              value={eventProperty}
              onChange={(value) => {
                setEventProperty(value)
                setPropertyValue('')
                setShowPropertyFilter(true)
              }}
              placeholder="Select property"
            />
            <select value={propertyOperator} onChange={(e) => setPropertyOperator(e.target.value)} disabled={!eventProperty}>
              <option value="=">=</option>
              <option value="!=">!=</option>
            </select>
            <SearchableSelect
              options={propertyValues}
              value={propertyValue}
              onChange={setPropertyValue}
              placeholder="Select value"
              disabled={!eventProperty}
            />
            <button type="button" className="filter-remove" onClick={clearPropertyFilter} title="Clear property filter">✕</button>
          </div>
        ) : null}
      </div>

      {metricType === 'per_active_user' && (
        <p>Retained users are calculated using the selected retention event.</p>
      )}

      {error && <p className="error">{error}</p>}

      <h3 className="section-header-inline">
        Event Volume ({volumeLabel})
        {cumulativeSupported && (
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={cumulativeMode}
              onChange={(e) => setCumulativeMode(e.target.checked)}
            />
            Cumulative
          </label>
        )}
      </h3>
      {volumeDisplayRows.length > 0 && (
        <div className={`analytics-table table-responsive ${showSplit ? 'has-split' : ''}`}>
          <table>
            <thead>
              <tr>
                <th
                  className={`${isPinned ? 'sticky-col sticky-col-cohort' : ''} sortable-header`}
                  style={{ 
                    width: columnWidths.cohort,
                    minWidth: columnWidths.cohort,
                    maxWidth: columnWidths.cohort,
                    left: isPinned ? getStickyLeft("cohort") : undefined
                  }}
                  onClick={() => {
                    if (isResizingRef.current) return
                    handleSortVolume('cohort_name')
                  }}
                >
                  Cohort {sortConfigVolume.key === 'cohort_name' && (sortConfigVolume.direction === 'asc' ? '↑' : '↓')}
                  <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('cohort', e); }} />
                </th>
                {showSplit && (
                  <th
                    className={`${isPinned ? 'sticky-col sticky-col-split' : ''} sortable-header`}
                    style={{ 
                      width: columnWidths.split, 
                      minWidth: columnWidths.split,
                      maxWidth: columnWidths.split,
                      left: isPinned ? getStickyLeft("split") : undefined 
                    }}
                    onClick={() => {
                      if (isResizingRef.current) return
                      handleSortVolume('split')
                    }}
                  >
                    Split {sortConfigVolume.key === 'split' && (sortConfigVolume.direction === 'asc' ? '↑' : '↓')}
                    <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('split', e); }} />
                  </th>
                )}
                <th
                  className={`${isPinned ? 'sticky-col sticky-col-size' : ''} sortable-header`}
                  style={{ 
                    width: columnWidths.size, 
                    minWidth: columnWidths.size,
                    maxWidth: columnWidths.size,
                    left: isPinned ? getStickyLeft("size") : undefined 
                  }}
                  onClick={() => {
                    if (isResizingRef.current) return
                    handleSortVolume('size')
                  }}
                >
                  Size {sortConfigVolume.key === 'size' && (sortConfigVolume.direction === 'asc' ? '↑' : '↓')}
                  <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('size', e); }} />
                </th>
                {dayColumnsVolume.map((day) => (
                  <th
                    key={day}
                    className="sortable-header"
                    onClick={() => {
                      if (isResizingRef.current) return
                      handleSortVolume(`D${day}`)
                    }}
                  >
                    D{day} {sortConfigVolume.key === `D${day}` && (sortConfigVolume.direction === 'asc' ? '↑' : '↓')}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedVolumeRows.map((row) => (
                <tr key={row.cohort_id}>
                  <td
                    className={isPinned ? 'sticky-col sticky-col-cohort' : ''}
                    style={{ 
                      width: columnWidths.cohort,
                      minWidth: columnWidths.cohort,
                      maxWidth: columnWidths.cohort,
                      left: isPinned ? getStickyLeft("cohort") : undefined
                    }}
                    title={row.cohort_name}
                  >
                    {getDisplayName(row)}
                  </td>
                  {showSplit && (
                    <td 
                      className={isPinned ? 'sticky-col sticky-col-split' : ''}
                      style={{ 
                        width: columnWidths.split, 
                        minWidth: columnWidths.split,
                        maxWidth: columnWidths.split,
                        left: isPinned ? getStickyLeft("split") : undefined 
                      }}
                    >
                      {getSplitLabel(row)}
                    </td>
                  )}
                  <td 
                    className={isPinned ? 'sticky-col sticky-col-size' : ''}
                    style={{ 
                      width: columnWidths.size, 
                      minWidth: columnWidths.size,
                      maxWidth: columnWidths.size,
                      left: isPinned ? getStickyLeft("size") : undefined 
                    }}
                  >
                    {formatCountValue(row.size)}
                  </td>
                  {dayColumnsVolume.map((day) => {
                    const rawValue = row.values?.[String(day)] ?? null
                    const hasValue = rawValue !== null && rawValue !== undefined
                    const value = hasValue ? (metricType === 'count' ? formatCountValue(rawValue) : rawValue) : null

                    const availability = row.availability?.[String(day)] || {}
                    const {
                      eligible_users = row.size,
                      cohort_size = row.size
                    } = availability

                    const ratio = cohort_size > 0 ? (eligible_users / cohort_size) : 1
                    const cellStyle = getAvailabilityStyle(ratio)

                    const tooltip = `Day ${day}\n\nValue: ${hasValue ? value : '—'}\nAvailable for ${eligible_users} / ${cohort_size} users`

                    return (
                      <td
                        key={day}
                        style={cellStyle}
                        title={tooltip}
                      >
                        {hasValue ? value : '—'}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <h3>Unique Users ({uniqueUsersLabel})</h3>
      {userDisplayRows.length > 0 && (
        <div className={`analytics-table table-responsive ${showSplit ? 'has-split' : ''}`}>
          <table>
            <thead>
              <tr>
                <th 
                  className={`${isPinned ? 'sticky-col sticky-col-cohort' : ''} sortable-header`} 
                  style={{ 
                    width: columnWidths.cohort,
                    minWidth: columnWidths.cohort,
                    maxWidth: columnWidths.cohort,
                    left: isPinned ? getStickyLeft("cohort") : undefined
                  }}
                  onClick={() => {
                    if (isResizingRef.current) return
                    handleSortUsers('cohort_name')
                  }}
                >
                  Cohort {sortConfigUsers.key === 'cohort_name' ? (sortConfigUsers.direction === 'asc' ? '↑' : '↓') : ''}
                  <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('cohort', e); }} />
                </th>
                {showSplit && (
                  <th 
                    className={`${isPinned ? 'sticky-col sticky-col-split' : ''} sortable-header`} 
                    style={{ 
                      width: columnWidths.split, 
                      minWidth: columnWidths.split,
                      maxWidth: columnWidths.split,
                      left: isPinned ? getStickyLeft("split") : undefined 
                    }}
                    onClick={() => {
                      if (isResizingRef.current) return
                      handleSortUsers('split')
                    }}
                  >
                    Split {sortConfigUsers.key === 'split' ? (sortConfigUsers.direction === 'asc' ? '↑' : '↓') : ''}
                    <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('split', e); }} />
                  </th>
                )}
                <th
                  className={`${isPinned ? 'sticky-col sticky-col-size' : ''} sortable-header`}
                  style={{ 
                    width: columnWidths.size, 
                    minWidth: columnWidths.size,
                    maxWidth: columnWidths.size,
                    left: isPinned ? getStickyLeft("size") : undefined 
                  }}
                  onClick={() => {
                    if (isResizingRef.current) return
                    handleSortUsers('size')
                  }}
                >
                  Size {sortConfigUsers.key === 'size' && (sortConfigUsers.direction === 'asc' ? '↑' : '↓')}
                  <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('size', e); }} />
                </th>
                {dayColumnsUsers.map((day) => (
                  <th
                    key={day}
                    className="sortable-header"
                    onClick={() => {
                      if (isResizingRef.current) return
                      handleSortUsers(`D${day}`)
                    }}
                  >
                    D{day} {sortConfigUsers.key === `D${day}` && (sortConfigUsers.direction === 'asc' ? '↑' : '↓')}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedUserRows.map((row) => (
                <tr key={row.cohort_id}>
                  <td
                    className={isPinned ? 'sticky-col sticky-col-cohort' : ''}
                    style={{ 
                      width: columnWidths.cohort,
                      minWidth: columnWidths.cohort,
                      maxWidth: columnWidths.cohort,
                      left: isPinned ? getStickyLeft("cohort") : undefined
                    }}
                    title={row.cohort_name}
                  >
                    {getDisplayName(row)}
                  </td>
                  {showSplit && (
                    <td 
                      className={isPinned ? 'sticky-col sticky-col-split' : ''}
                      style={{ 
                        width: columnWidths.split, 
                        minWidth: columnWidths.split,
                        maxWidth: columnWidths.split,
                        left: isPinned ? getStickyLeft("split") : undefined 
                      }}
                    >
                      {getSplitLabel(row)}
                    </td>
                  )}
                  <td 
                    className={isPinned ? 'sticky-col sticky-col-size' : ''}
                    style={{ 
                      width: columnWidths.size, 
                      minWidth: columnWidths.size,
                      maxWidth: columnWidths.size,
                      left: isPinned ? getStickyLeft("size") : undefined 
                    }}
                  >
                    {formatCountValue(row.size)}
                  </td>
                  {dayColumnsUsers.map((day) => {
                    const rawValue = row.values?.[String(day)] ?? null
                    const hasValue = rawValue !== null && rawValue !== undefined
                    const value = hasValue 
                      ? (modeUsers === 'percent' || modeUsers === 'adoption_percent' ? `${rawValue}%` : formatCountValue(rawValue))
                      : null

                    const availability = row.availability?.[String(day)] || {}
                    const {
                      eligible_users = row.size,
                      cohort_size = row.size
                    } = availability

                    const ratio = cohort_size > 0 ? (eligible_users / cohort_size) : 1
                    const cellStyle = getAvailabilityStyle(ratio)

                    const tooltip = `Day ${day}\n\nValue: ${hasValue ? value : '—'}\nAvailable for ${eligible_users} / ${cohort_size} users`

                    return (
                      <td
                        key={day}
                        style={cellStyle}
                        title={tooltip}
                      >
                        {hasValue ? value : '—'}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {event && (
        <UsageFrequencyHistogram
          event={event}
          refreshToken={refreshToken}
          propertyFilter={propertyFilter}
          prefetchedData={frequencyData}
          loadingState={frequencyLoading}
        />
      )}

      <ComparePane
        isOpen={isComparePaneOpen}
        onClose={() => setIsComparePaneOpen(false)}
        tab="usage"
        maxDay={maxDay}
        currentEvent={event}
        retentionEvent={retentionEvent}
        defaultMetric={metricType === 'per_active_user' ? 'per_retained_user' : metricType !== 'count' ? metricType : 'per_installed_user'}
        propertyFilter={propertyFilter}
        appliedFilters={appliedFilters}
        onAddToExport={onAddToExport}
      />
    </section>
  )
}
