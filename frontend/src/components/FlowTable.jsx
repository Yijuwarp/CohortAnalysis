import { useEffect, useMemo, useState, useRef } from 'react'
import { getFlowL1, getFlowL2, listCohorts, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'

export default function FlowTable({ refreshToken }) {
  const [events, setEvents] = useState([])
  const [startEvent, setStartEvent] = useState(null)
  const [direction, setDirection] = useState('forward')
  
  const [cohortMap, setCohortMap] = useState({})
  const [l1Data, setL1Data] = useState([])
  const [loadingL1, setLoadingL1] = useState(false)
  const [error, setError] = useState('')
  
  const [expandedRows, setExpandedRows] = useState(new Set())
  const [l2Data, setL2Data] = useState({})
  const [loadingL2, setLoadingL2] = useState({})

  // Load cohort names to display in table headers
  useEffect(() => {
    async function loadCohorts() {
      try {
        const resp = await listCohorts()
        const map = {}
        resp.cohorts?.forEach(c => {
          map[String(c.cohort_id)] = { name: c.cohort_name, size: c.size || 0 }
        })
        setCohortMap(map)
      } catch (err) {
        console.error('Failed to load cohorts', err)
      }
    }
    loadCohorts()
  }, [refreshToken])

  // Load events for the event picker
  useEffect(() => {
    async function fetchEvents() {
      try {
        const resp = await listEvents()
        const fetchedEvents = resp.events || []
        setEvents(fetchedEvents)
        if (fetchedEvents.length > 0 && !startEvent) {
          setStartEvent(fetchedEvents[0])
        } else if (fetchedEvents.length === 0) {
          setStartEvent(null)
        }
      } catch (err) {
        console.error('Failed to load events', err)
        setEvents([])
        setStartEvent(null)
      }
    }
    fetchEvents()
  }, [refreshToken, startEvent])

  const requestIdRef = useRef(0)

  // Fetch L1 Flow Data
  useEffect(() => {
    if (!startEvent) {
      setL1Data([])
      setExpandedRows(new Set())
      return
    }

    setL1Data(null)
    setExpandedRows(new Set())

    const requestId = ++requestIdRef.current


    async function loadL1() {
      setLoadingL1(true)
      setError('')
      try {
        const resp = await getFlowL1(startEvent, direction)
        if (requestId !== requestIdRef.current) return
        setL1Data(resp.rows || [])
      } catch (err) {
        if (requestId !== requestIdRef.current) return
        setError(err.message || 'Failed to load flow data')
        setL1Data([])
      } finally {
        if (requestId === requestIdRef.current) {
          setLoadingL1(false)
        }
      }
    }
    loadL1()
  }, [refreshToken, startEvent, direction])

  const toggleExpand = async (parentEvent) => {
    const nextExpanded = new Set(expandedRows)
    
    if (nextExpanded.has(parentEvent)) {
      nextExpanded.delete(parentEvent)
      setExpandedRows(nextExpanded)
      return
    }

    // Expand row
    nextExpanded.add(parentEvent)
    setExpandedRows(nextExpanded)

    const cacheKey = `${startEvent}_${direction}_${parentEvent}`

    // Load L2 if not in cache
    if (!l2Data[cacheKey]) {
      setLoadingL2(prev => ({ ...prev, [cacheKey]: true }))
      try {
        const resp = await getFlowL2(startEvent, parentEvent, direction)
        setL2Data(prev => ({ ...prev, [cacheKey]: resp.rows || [] }))
      } catch (err) {
        console.error('Failed to load L2 flows for', parentEvent, err)
      } finally {
        setLoadingL2(prev => ({ ...prev, [cacheKey]: false }))
      }
    }
  }

  const cohorts = useMemo(() => {
    if (!l1Data || l1Data.length === 0) return []
    const firstRowValues = l1Data[0].values || {}
    return Object.keys(firstRowValues)
  }, [l1Data])

  const renderRow = (row, isL2 = false) => {
    const nodeEvent = row.path[row.path.length - 1]
    let parentEvent = null
    
    if (!isL2) {
      parentEvent = nodeEvent
    }
    
    const isExpanded = parentEvent && expandedRows.has(parentEvent)
    const isExpandable = row.expandable === true
    const isOther = nodeEvent === 'Other'
    
    const indent = isL2 ? 24 : 0
    
    return (
      <tr 
        key={row.path.join('-')}
        className={isOther ? 'flow-row-other' : ''}
        style={{ 
          backgroundColor: isExpanded && !isL2 ? '#f9fafb' : undefined
        }}
        data-testid={isL2 ? 'flow-row-l2' : 'flow-row-l1'}
      >
        <td 
          className="sticky-col flow-path-col" 
          title={row.path.join(' → ')}
          onClick={() => {
            if (isExpandable) {
              toggleExpand(parentEvent)
            }
          }}
          style={{ cursor: isExpandable ? 'pointer' : 'default' }}
        >
          <div className={isL2 ? "flow-l2" : ""} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span>
              {isL2 ? '↳ ' : <span style={{opacity: 0.6}}>{startEvent} → </span>}
              <strong style={{ fontWeight: isOther ? 'normal' : '600' }}>{nodeEvent}</strong>
            </span>
            {isExpandable && (
              <span className="flow-expand-icon">
                {isExpanded ? '▼' : '▶'}
              </span>
            )}
          </div>
        </td>
        {cohorts.map(cid => {
          const val = row.values[cid]
          if (!val) return <td key={cid} style={{textAlign: 'right'}}>—</td>
          const pctText = (val.pct * 100).toFixed(1) + '%'
          return (
            <td key={cid} style={{textAlign: 'right'}} title={`${val.count.toLocaleString()} users`}>
              {pctText}
            </td>
          )
        })}
      </tr>
    )
  }

  const renderTable = () => {
    if (events.length === 0) return <p style={{ marginTop: '16px' }}>No events available</p>
    if (!startEvent) return <p style={{ marginTop: '16px' }}>Select an event to view flows</p>
    if (loadingL1) return <div className="table-loading" style={{ padding: '24px 0' }}>Loading flows...</div>
    if (error) return <p className="error">{error}</p>
    if (!l1Data || l1Data.length === 0) return <p style={{ marginTop: '16px' }}>No flow data available</p>

    return (
      <div className="analytics-table table-responsive" style={{ marginTop: '16px' }}>
        <table>
          <thead>
            <tr>
              <th className="sticky-col flow-path-col">Path</th>
              {cohorts.map(cid => {
                const c = cohortMap[cid] || { name: `Cohort ${cid}`, size: 0 }
                return (
                  <th key={cid} style={{textAlign: 'right'}} title={c.name}>
                    <div>{c.name}</div>
                    <div className="cohort-size">
                      ({c.size.toLocaleString()} users)
                    </div>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {l1Data.map(l1Row => {
              const rows = [renderRow(l1Row, false)]
              const parentEvent = l1Row.path[1]
              
              if (expandedRows.has(parentEvent)) {
                const cacheKey = `${startEvent}_${direction}_${parentEvent}`
                if (loadingL2[cacheKey]) {
                  rows.push(
                    <tr key={`loading-${parentEvent}`}>
                      <td className="sticky-col flow-path-col">
                        <div className="flow-l2" style={{ opacity: 0.6 }}>
                          ↳ ⏳ Loading...
                        </div>
                      </td>
                      {cohorts.map(cid => <td key={cid}></td>)}
                    </tr>
                  )
                } else if (l2Data[cacheKey]) {
                  l2Data[cacheKey].forEach(l2Row => {
                    rows.push(renderRow(l2Row, true))
                  })
                }
              }
              return rows
            })}
          </tbody>
        </table>
      </div>
    )
  }

  return (
    <section className="card">
      <h2>Flow Explorer</h2>
      
      <div className="inline-controls" style={{ marginBottom: '16px' }}>
        <label>
          Event
          <SearchableSelect
            options={events}
            value={startEvent}
            onChange={setStartEvent}
            placeholder="Select an event"
          />
        </label>
        
        <label>
          Direction
          <div className="view-toggle" style={{ marginTop: '4px' }}>
            <button
              type="button"
              className={`view-button ${direction === 'forward' ? 'active' : ''}`}
              onClick={() => setDirection('forward')}
            >
              Forward
            </button>
            <button
              type="button"
              className={`view-button ${direction === 'reverse' ? 'active' : ''}`}
              onClick={() => setDirection('reverse')}
            >
              Reverse
            </button>
          </div>
        </label>
      </div>

      {renderTable()}
    </section>
  )
}
