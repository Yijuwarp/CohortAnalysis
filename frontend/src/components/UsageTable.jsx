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

const isMultiOperator = (op) => op?.toLowerCase().includes('in')

// ---------------------------------------------------------------------------
// Action Icons
// ---------------------------------------------------------------------------

function PlayIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="5 3 19 12 5 21 5 3"></polygon>
    </svg>
  )
}

function ReloadIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8" />
      <polyline points="21 3 21 8 16 8" />
    </svg>
  )
}

function MicroscopeIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M6 18h8" />
      <path d="M3 22h18" />
      <path d="M14 22a7 7 0 1 0 0-14h-1" />
      <path d="M9 14l2 2" />
      <path d="M9 12a2 2 0 1 1-2-2V6h6v4a2 2 0 1 1-2 2z" />
      <path d="M12 6V3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3" />
    </svg>
  )
}

function ExportIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"></path>
      <circle cx="12" cy="13" r="4"></circle>
    </svg>
  )
}

function FunnelIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 5H17L11 11V15L9 14V11L3 5Z" />
    </svg>
  )
}

function PinIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="17" x2="12" y2="22" />
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6a3 3 0 0 0-3-3 3 3 0 0 0-3 3v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
    </svg>
  )
}

function PinOffIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.5 }}>
      <line x1="12" y1="17" x2="12" y2="22" />
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6a3 3 0 0 0-3-3 3 3 0 0 0-3 3v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
      <line x1="2" y1="2" x2="22" y2="22" />
    </svg>
  )
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
  const [isPinnedVolume, setIsPinnedVolume] = useState(state?.isPinnedVolume ?? true)
  const [isPinnedUsers, setIsPinnedUsers] = useState(state?.isPinnedUsers ?? true)
  const [modeUsers, setModeUsers] = useState(state?.modeUsers || 'count')
  const [metricType, setMetricType] = useState(state?.metricType || 'count')
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
  const [multiPropertyValues, setMultiPropertyValues] = useState(Array.isArray(state?.propertyValue) ? state.propertyValue : [])
  const [propertyLoading, setPropertyLoading] = useState(false)
  const [showPropertyFilter, setShowPropertyFilter] = useState(state?.showPropertyFilter ?? false)
  const filterRowRef = useRef(null)

  useEffect(() => {
    const nextState = {
      event,
      effectiveMaxDayVolume,
      effectiveMaxDayUsers,
      isPinnedVolume,
      isPinnedUsers,
      modeUsers,
      metricType,
      eventProperty,
      propertyOperator,
      propertyValue: isMultiOperator(propertyOperator) ? multiPropertyValues : propertyValue,
      showPropertyFilter,
      columnWidths
    }
    setState(nextState)
  }, [event, effectiveMaxDayVolume, effectiveMaxDayUsers, isPinnedVolume, isPinnedUsers, modeUsers, metricType, eventProperty, propertyOperator, propertyValue, multiPropertyValues.join(','), showPropertyFilter, columnWidths])
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

  const multiValuesKey = multiPropertyValues.join(',')

  const propertyValuesToPass = isMultiOperator(propertyOperator) ? multiPropertyValues : (propertyValue ? [propertyValue] : [])
  const propertyFilter = useMemo(
    () => eventProperty ? { property: eventProperty, operator: propertyOperator, values: propertyValuesToPass } : null,
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [eventProperty, propertyOperator, propertyValue, multiValuesKey]
  )
  const propertyFilterRequiresValue = useMemo(
    () => Boolean(eventProperty && (isMultiOperator(propertyOperator) ? multiPropertyValues.length === 0 : !propertyValue)),
    [eventProperty, propertyOperator, propertyValue, multiValuesKey]
  )

  const clearPropertyFilter = () => {
    setEventProperty('')
    setPropertyOperator('=')
    setPropertyValue('')
    setMultiPropertyValues([])
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
  }, [event, propertyFilter, localRefreshToken, refreshToken])

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
  }, [event, maxDay, retentionEvent, scopeVersion, propertyFilter, propertyFilterRequiresValue, refreshToken])

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
      setMultiPropertyValues([])
      return
    }

    let isMounted = true
    getEventPropertyValues(event, eventProperty)
      .then((response) => {
        if (!isMounted) return
        const nextValues = response.values || []
        setPropertyValues(nextValues)
        setPropertyValue((current) => (current && nextValues.includes(current) ? current : ''))
        setMultiPropertyValues((current) => current.filter(v => nextValues.includes(v)))
      })
      .catch(() => {
        if (!isMounted) return
        setPropertyValues([])
        setPropertyValue('')
        setMultiPropertyValues([])
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

    const isCumulative = metricType === 'cumulative_count' || metricType === 'cumulative_per_installed_user'

    return volumeRows.map((row) => {
      const converted = {}
      const usersByDay = usersByCohort.get(row.cohort_id) || {}
      const retainedByDay = retainedByCohort.get(row.cohort_id) || {}
      const eventValues = isCumulative ? computeCumulative(row.values || {}) : row.values || {}

      for (const day of dayColumns) {
        const totalEvents = Number(eventValues[String(day)] ?? 0)
        const distinctUsers = Number(usersByDay[String(day)] ?? 0)
        const retainedUsers = Number(retainedByDay[String(day)] ?? 0)

        if (metricType === 'count' || metricType === 'cumulative_count') {
          converted[String(day)] = totalEvents
          continue
        }

        if (metricType === 'per_event_firer') {
          converted[String(day)] = formatRatioValue(distinctUsers > 0 ? totalEvents / distinctUsers : 0)
          continue
        }

        if (metricType === 'per_installed_user' || metricType === 'cumulative_per_installed_user') {
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
  }, [dayColumns, metricType, retainedRows, userRows, volumeRows])

  const showSplit = useMemo(() => {
    return (cohorts || []).some(c => c.split_type != null)
  }, [cohorts])

  const sortedVolumeRows = useMemo(() => sortRows(volumeDisplayRows, sortConfigVolume), [volumeDisplayRows, sortConfigVolume, cohorts])
  const sortedUserRows = useMemo(() => sortRows(userDisplayRows, sortConfigUsers), [userDisplayRows, sortConfigUsers, cohorts])

  const volumeLabel =
    metricType === 'count'
      ? 'Event Count'
      : metricType === 'cumulative_count'
        ? 'Cumulative Event Count'
      : metricType === 'per_installed_user'
        ? 'Events per Installed User'
      : metricType === 'cumulative_per_installed_user'
        ? 'Cumulative Events per Installed User'
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
      title: `Event Volume (${volumeLabel})`,
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
          'Metric': volumeLabel,
          'Max Day': maxDay,
          'Property Filter': eventProperty ? `${eventProperty} ${propertyOperator} ${isMultiOperator(propertyOperator) ? multiPropertyValues.join(', ') : propertyValue}` : 'None'
        }
      }
    }
    onAddToExport(payload)
  }

  return (
    <section className="card usage-analytics-card">
      <div className="usage-query-container">
        {/* Row 1: Primary Query & Global Actions */}
        <div className="usage-query-row">
          <div className="query-main">
            <span className="query-label">Event</span>
            <SearchableSelect
              options={events}
              value={event}
              onChange={handleEventChange}
              placeholder="Select an event"
              style={{ width: '240px' }}
              column="event_name"
            />
            <button
              type="button"
              className={`action-icon-button funnel-toggle ${eventProperty ? 'active' : ''} ${showPropertyFilter ? 'row-open' : ''}`}
              onClick={() => {
                if (eventProperty) {
                  clearPropertyFilter()
                } else {
                  setShowPropertyFilter(prev => !prev)
                }
              }}
              title={eventProperty ? "Edit filter" : "Add filter"}
              aria-label="Toggle filter"
              disabled={!event}
            >
              <FunnelIcon />
            </button>
          </div>
          
          <div className="query-actions">
            <button
               type="button"
               className={`action-icon-button ${loading ? 'loading' : ''}`}
               onClick={() => loadUsage()}
               disabled={loading || !event || propertyFilterRequiresValue || retentionEvent === undefined || retentionEvent === null || retentionEvent === ""}
               title="Reload analysis"
               aria-label="Reload analysis"
            >
              <ReloadIcon />
            </button>

            <button
              type="button"
              className={`action-icon-button ${isComparePaneOpen ? 'compare-toggle-active' : ''}`}
              onClick={() => setIsComparePaneOpen(prev => !prev)}
              title="Statistical analysis"
              aria-label="Statistical analysis"
              data-testid="open-compare-pane"
              disabled={!event}
            >
              <MicroscopeIcon />
            </button>

            <button
              type="button"
              className="action-icon-button"
              onClick={handleAddToExport}
              title="Snapshot (Add to Export)"
              aria-label="Snapshot"
              disabled={!event || volumeRows.length === 0}
            >
              <ExportIcon />
            </button>
          </div>
        </div>

        {/* Row 2: Filters (Animated Wrapper) */}
        <div className={`usage-filter-row-wrapper ${showPropertyFilter ? 'open' : ''}`}>
          <div className="usage-filter-row" style={{ flexDirection: 'column', alignItems: 'flex-start' }} ref={filterRowRef}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', width: '100%' }}>
              <span className="filter-label">where</span>
              {propertyLoading ? (
                <small className="secondary-text">Loading properties...</small>
              ) : (
                <div className="filter-triplet">
                  <SearchableSelect
                    options={eventProperties}
                    value={eventProperty}
                    onChange={(value) => {
                      setEventProperty(value)
                      setPropertyValue('')
                    }}
                    placeholder="Property"
                    style={{ width: '180px' }}
                  />
                  <select 
                    className="operator-select"
                    value={propertyOperator} 
                    onChange={(e) => {
                      const nextOp = e.target.value
                      setPropertyOperator(nextOp)
                    }} 
                    disabled={!eventProperty}
                  >
                    <option value="=">=</option>
                    <option value="!=">!=</option>
                    <option value="in">in</option>
                    <option value="not in">not in</option>
                  </select>
                  {isMultiOperator(propertyOperator) ? (
                    <SearchableSelect
                      options={propertyValues}
                      value=""
                      onChange={(val) => {
                        if (!val) return
                        const normalized = String(val)
                        if (!multiPropertyValues.includes(normalized)) {
                          setMultiPropertyValues(prev => [...prev, normalized])
                        }
                      }}
                      placeholder="Add value..."
                      disabled={!eventProperty}
                      column={eventProperty}
                      eventName={event}
                      excludeValues={multiPropertyValues}
                      style={{ width: '180px' }}
                    />
                  ) : (
                    <SearchableSelect
                      options={propertyValues}
                      value={propertyValue}
                      onChange={setPropertyValue}
                      placeholder="Value"
                      disabled={!eventProperty}
                      column={eventProperty}
                      eventName={event}
                      style={{ width: '180px' }}
                    />
                  )}
                  <button 
                    type="button" 
                    className="filter-remove-btn" 
                    onClick={clearPropertyFilter} 
                    title="Remove filter"
                  >
                    ✕
                  </button>
                </div>
              )}
            </div>
            {isMultiOperator(propertyOperator) && multiPropertyValues.length > 0 && (
              <div className="cohort-pills" style={{ paddingLeft: '52px', paddingTop: '6px' }}>
                {multiPropertyValues.map(val => (
                  <span key={val} className="cohort-pill">
                    {val}
                    <button 
                      type="button" 
                      onClick={() => setMultiPropertyValues(prev => prev.filter(v => v !== val))}
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
      <div className="table-header-scoped">
        <h3 className="section-header-inline">
          Event Volume (
          <select value={metricType} onChange={(e) => setMetricType(e.target.value)} className="inline-header-select">
            <option value="count">Event Count</option>
            <option value="cumulative_count">Cumulative Event Count</option>
            <option value="per_active_user">Events per Retained User</option>
            <option value="per_installed_user">Events per Installed User</option>
            <option value="cumulative_per_installed_user">Cumulative Events per Installed User</option>
            <option value="per_event_firer">Events per Event Firer</option>
          </select>
          )
          <button
            type="button"
            className={`view-button table-pin ${isPinnedVolume ? 'active' : ''}`}
            onClick={() => setIsPinnedVolume(prev => !prev)}
            title={isPinnedVolume ? "Unpin this table" : "Pin this table"}
            style={{ marginLeft: '12px' }}
          >
            {isPinnedVolume ? <PinIcon /> : <PinOffIcon />}
          </button>
        </h3>
      </div>
      {volumeDisplayRows.length > 0 && (
        <div className={`analytics-table table-responsive ${showSplit ? 'has-split' : ''}`}>
          <table>
            <thead>
              <tr>
                <th
                  className={`${isPinnedVolume ? 'sticky-col sticky-col-cohort' : ''} sticky-col-top sortable-header`}
                  style={{ 
                    width: columnWidths.cohort,
                    minWidth: columnWidths.cohort,
                    maxWidth: columnWidths.cohort,
                    left: isPinnedVolume ? getStickyLeft("cohort") : undefined
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
                    className={`${isPinnedVolume ? 'sticky-col sticky-col-split' : ''} sticky-col-top sortable-header`}
                    style={{ 
                      width: columnWidths.split, 
                      minWidth: columnWidths.split,
                      maxWidth: columnWidths.split,
                      left: isPinnedVolume ? getStickyLeft("split") : undefined 
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
                  className={`${isPinnedVolume ? 'sticky-col sticky-col-size' : ''} sticky-col-top sortable-header`}
                  style={{ 
                    width: columnWidths.size, 
                    minWidth: columnWidths.size,
                    maxWidth: columnWidths.size,
                    left: isPinnedVolume ? getStickyLeft("size") : undefined 
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
                    className="sticky-col-top sticky-col sortable-header"
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
                    className={isPinnedVolume ? 'sticky-col sticky-col-cohort' : ''}
                    style={{ 
                      width: columnWidths.cohort,
                      minWidth: columnWidths.cohort,
                      maxWidth: columnWidths.cohort,
                      left: isPinnedVolume ? getStickyLeft("cohort") : undefined
                    }}
                    title={row.cohort_name}
                  >
                    {getDisplayName(row)}
                  </td>
                  {showSplit && (
                    <td 
                      className={isPinnedVolume ? 'sticky-col sticky-col-split' : ''}
                      style={{ 
                        width: columnWidths.split, 
                        minWidth: columnWidths.split,
                        maxWidth: columnWidths.split,
                        left: isPinnedVolume ? getStickyLeft("split") : undefined 
                      }}
                    >
                      {getSplitLabel(row)}
                    </td>
                  )}
                  <td 
                    className={isPinnedVolume ? 'sticky-col sticky-col-size' : ''}
                    style={{ 
                      width: columnWidths.size, 
                      minWidth: columnWidths.size,
                      maxWidth: columnWidths.size,
                      left: isPinnedVolume ? getStickyLeft("size") : undefined 
                    }}
                  >
                    {formatCountValue(row.size)}
                  </td>
                  {dayColumnsVolume.map((day) => {
                    const rawValue = row.values?.[String(day)] ?? null
                    const hasValue = rawValue !== null && rawValue !== undefined
                    const isCount = metricType === 'count' || metricType === 'cumulative_count'
                    const value = hasValue ? (isCount ? formatCountValue(rawValue) : rawValue) : null

                    const availability = row.availability?.[String(day)] || {}
                    const {
                      eligible_users = row.size,
                      cohort_size = row.size
                    } = availability

                    const ratio = cohort_size > 0 ? (eligible_users / cohort_size) : 1
                    const cellStyle = getAvailabilityStyle(ratio)

                    const availabilityPct = Math.round((eligible_users / cohort_size) * 100)
                    const tooltip = `Day ${day}\n\nValue: ${hasValue ? value : '—'}\nAvailability: ${availabilityPct}% (${eligible_users} / ${cohort_size} users)`

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

      <div className="table-header-scoped" style={{ marginTop: '32px' }}>
        <h3 className="section-header-inline">
          Unique Users (
          <select value={modeUsers} onChange={(e) => setModeUsers(e.target.value)} className="inline-header-select">
            <option value="count">Daily Users (Count)</option>
            <option value="percent">Daily Users (%)</option>
            <option value="adoption_count">Cumulative Adoption (Count)</option>
            <option value="adoption_percent">Cumulative Adoption (%)</option>
          </select>
          )
          <button
            type="button"
            className={`view-button table-pin ${isPinnedUsers ? 'active' : ''}`}
            onClick={() => setIsPinnedUsers(prev => !prev)}
            title={isPinnedUsers ? "Unpin this table" : "Pin this table"}
            style={{ marginLeft: '12px' }}
          >
            {isPinnedUsers ? <PinIcon /> : <PinOffIcon />}
          </button>
        </h3>
      </div>
      {userDisplayRows.length > 0 && (
        <div className={`analytics-table table-responsive ${showSplit ? 'has-split' : ''}`}>
          <table>
            <thead>
              <tr>
                <th 
                  className={`${isPinnedUsers ? 'sticky-col sticky-col-cohort' : ''} sticky-col-top sortable-header`} 
                  style={{ 
                    width: columnWidths.cohort,
                    minWidth: columnWidths.cohort,
                    maxWidth: columnWidths.cohort,
                    left: isPinnedUsers ? getStickyLeft("cohort") : undefined
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
                    className={`${isPinnedUsers ? 'sticky-col sticky-col-split' : ''} sticky-col-top sortable-header`} 
                    style={{ 
                      width: columnWidths.split, 
                      minWidth: columnWidths.split,
                      maxWidth: columnWidths.split,
                      left: isPinnedUsers ? getStickyLeft("split") : undefined 
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
                  className={`${isPinnedUsers ? 'sticky-col sticky-col-size' : ''} sticky-col-top sortable-header`}
                  style={{ 
                    width: columnWidths.size, 
                    minWidth: columnWidths.size,
                    maxWidth: columnWidths.size,
                    left: isPinnedUsers ? getStickyLeft("size") : undefined 
                  }}
                  onClick={() => {
                    if (isResizingRef.current) return
                    handleSortUsers('size')
                  }}
                >
                  Size {sortConfigUsers.key === 'size' ? (sortConfigUsers.direction === 'asc' ? '↑' : '↓') : ''}
                  <div className="column-resizer" onMouseDown={(e) => { e.stopPropagation(); startResize('size', e); }} />
                </th>
                {dayColumnsUsers.map((day) => (
                  <th
                    key={day}
                    className="sticky-col-top sticky-col sortable-header"
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
                    className={isPinnedUsers ? 'sticky-col sticky-col-cohort' : ''}
                    style={{ 
                      width: columnWidths.cohort,
                      minWidth: columnWidths.cohort,
                      maxWidth: columnWidths.cohort,
                      left: isPinnedUsers ? getStickyLeft("cohort") : undefined
                    }}
                    title={row.cohort_name}
                  >
                    {getDisplayName(row)}
                  </td>
                  {showSplit && (
                    <td 
                      className={isPinnedUsers ? 'sticky-col sticky-col-split' : ''}
                      style={{ 
                        width: columnWidths.split, 
                        minWidth: columnWidths.split,
                        maxWidth: columnWidths.split,
                        left: isPinnedUsers ? getStickyLeft("split") : undefined 
                      }}
                    >
                      {getSplitLabel(row)}
                    </td>
                  )}
                  <td 
                    className={isPinnedUsers ? 'sticky-col sticky-col-size' : ''}
                    style={{ 
                      width: columnWidths.size, 
                      minWidth: columnWidths.size,
                      maxWidth: columnWidths.size,
                      left: isPinnedUsers ? getStickyLeft("size") : undefined 
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

                    const availabilityPct = Math.round((eligible_users / cohort_size) * 100)
                    const tooltip = `Day ${day}\n\nValue: ${hasValue ? value : '—'}\nAvailability: ${availabilityPct}% (${eligible_users} / ${cohort_size} users)`

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
