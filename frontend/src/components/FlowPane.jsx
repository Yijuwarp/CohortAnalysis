import { useEffect, useMemo, useRef, useState } from 'react'
import { getEventProperties, getEventPropertyValues, getFlowL1, getFlowL2, listCohorts, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'
import FlowTable, { nodeKey } from './FlowTable'

const TABLE_MAX_DEPTH = 5

function makeCacheKey(path, controls, propertyFilter, depth) {
  return JSON.stringify({
    start_event: controls.event,
    direction: controls.direction,
    property: propertyFilter?.column || null,
    property_value: propertyFilter?.values?.[0] || null,
    depth,
    parent_path: path,
  })
}



export default function FlowPane({ refreshToken, state, setState }) {
  const [events, setEvents] = useState([])
  const [controls, setControls] = useState(state?.controls || { event: null, property: null, direction: 'forward' })
  const [propertyValues, setPropertyValues] = useState([])
  const [propertyFilterValue, setPropertyFilterValue] = useState(state?.propertyFilterValue || '')
  const [properties, setProperties] = useState([])

  const [cohortMap, setCohortMap] = useState({})
  const [flowTree, setFlowTree] = useState([])
  const [expandedNodes, setExpandedNodes] = useState(new Set())
  const [cache, setCache] = useState({})
  const [loadingNodes, setLoadingNodes] = useState({})
  const [loadingRoot, setLoadingRoot] = useState(false)

  useEffect(() => {
    const nextState = {
      controls,
      propertyFilterValue,
    }
    setState(nextState)
  }, [controls, propertyFilterValue])

  const reqIdRef = useRef(0)

  useEffect(() => {
    listCohorts().then(resp => {
      const map = {}
      ;(resp.cohorts || []).forEach(c => (map[String(c.cohort_id)] = { name: c.cohort_name, size: c.size || 0 }))
      setCohortMap(map)
    }).catch(() => setCohortMap({}))
  }, [refreshToken])

  useEffect(() => {
    listEvents().then(resp => {
      const ev = resp.events || []
      setEvents(ev)
      setControls(prev => ({ ...prev, event: prev.event || ev[0] || null }))
    }).catch(() => setEvents([]))
  }, [refreshToken])

  useEffect(() => {
    if (!controls.event) {
      setProperties([])
      return
    }
    getEventProperties(controls.event).then(resp => {
      setProperties(resp.properties || [])
    }).catch(() => setProperties([]))
  }, [controls.event])

  useEffect(() => {
    if (!controls.event || !controls.property) {
      setPropertyValues([])
      setPropertyFilterValue('')
      return
    }
    getEventPropertyValues(controls.event, controls.property).then(resp => {
      setPropertyValues(resp.values || [])
    }).catch(() => setPropertyValues([]))
  }, [controls.event, controls.property])

  const propertyFilter = useMemo(() => {
    if (!controls.property || !propertyFilterValue) return null
    return { column: controls.property, operator: '=', values: [propertyFilterValue] }
  }, [controls.property, propertyFilterValue])

  useEffect(() => {
    if (!controls.event) {
      setFlowTree([])
      return
    }
    const rid = ++reqIdRef.current
    setLoadingRoot(true)
    setExpandedNodes(new Set())
    setCache({})

    getFlowL1(controls.event, controls.direction, TABLE_MAX_DEPTH, propertyFilter)
      .then(resp => {
        if (rid !== reqIdRef.current) return
        setFlowTree(resp.rows || [])
      })
      .catch(() => {
        if (rid !== reqIdRef.current) return
        setFlowTree([])
      })
      .finally(() => {
        if (rid === reqIdRef.current) setLoadingRoot(false)
      })
  }, [refreshToken, controls.event, controls.direction, propertyFilter])

  const cohorts = useMemo(() => {
    const sourceRows = flowTree
    const first = sourceRows?.[0]
    return first ? Object.keys(first.values || {}) : Object.keys(cohortMap)
  }, [flowTree, cohortMap])

  const getChildren = (path) => cache[makeCacheKey(path, controls, propertyFilter, TABLE_MAX_DEPTH)] || []

  const onToggle = async (path) => {
    const key = nodeKey(path)
    const cacheKey = makeCacheKey(path, controls, propertyFilter, TABLE_MAX_DEPTH)
    const depth = path.length - 1
    if (depth >= TABLE_MAX_DEPTH) return

    setExpandedNodes(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })

    if (cache[cacheKey]) return

    setLoadingNodes(prev => ({ ...prev, [key]: true }))
    try {
      const resp = await getFlowL2(controls.event, path, controls.direction, TABLE_MAX_DEPTH, propertyFilter)
      setCache(prev => ({ ...prev, [cacheKey]: resp.rows || [] }))
    } finally {
      setLoadingNodes(prev => ({ ...prev, [key]: false }))
    }
  }


  return (
    <section className="card">
      <h2>Flow Explorer</h2>
      <div className="inline-controls" style={{ marginBottom: 16 }}>
        <label>
          Event
          <SearchableSelect options={events} value={controls.event} onChange={(event) => setControls(prev => ({ ...prev, event }))} placeholder="Select event" />
        </label>
        <label>
          Property
          <select
            value={controls.property || ''}
            disabled={!controls.event}
            onChange={(e) => {
              const property = e.target.value || null
              setControls(prev => ({ ...prev, property }))
              setPropertyFilterValue('')
              setFlowTree([])
              setExpandedNodes(new Set())
              setCache({})
            }}
          >
            <option value="">None</option>
            {properties.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </label>
        <label>
          Property Value
          <select value={propertyFilterValue} disabled={!controls.property} onChange={(e) => setPropertyFilterValue(e.target.value)}>
            <option value="">All</option>
            {propertyValues.map(v => <option key={v} value={v}>{v}</option>)}
          </select>
        </label>
        <label>
          Direction
          <div className="view-toggle" style={{ marginTop: 4 }}>
            <button type="button" className={`view-button ${controls.direction === 'forward' ? 'active' : ''}`} onClick={() => setControls(prev => ({ ...prev, direction: 'forward' }))}>Forward</button>
            <button type="button" className={`view-button ${controls.direction === 'reverse' ? 'active' : ''}`} onClick={() => setControls(prev => ({ ...prev, direction: 'reverse' }))}>Reverse</button>
          </div>
        </label>
      </div>

      {loadingRoot ? (
        <div className="table-loading" style={{ padding: '24px 0' }}>Loading flows...</div>
      ) : (
        <FlowTable
          rootRows={flowTree}
          cohorts={cohorts}
          cohortMap={cohortMap}
          expandedNodes={expandedNodes}
          loadingNodes={loadingNodes}
          getChildren={getChildren}
          onToggle={onToggle}
          maxDepth={TABLE_MAX_DEPTH}
        />
      )}
    </section>
  )
}
