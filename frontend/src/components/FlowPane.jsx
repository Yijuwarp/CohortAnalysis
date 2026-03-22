import { useEffect, useMemo, useRef, useState } from 'react'
import { getEventProperties, getEventPropertyValues, getFlowL1, getFlowL2, listCohorts, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'
import FlowTable, { nodeKey } from './FlowTable'
import FlowGraph from './FlowGraph'

export default function FlowPane({ refreshToken }) {
  const [events, setEvents] = useState([])
  const [controls, setControls] = useState({ event: null, property: null, direction: 'forward', depth: 2 })
  const [propertyValues, setPropertyValues] = useState([])
  const [propertyFilterValue, setPropertyFilterValue] = useState('')
  const [properties, setProperties] = useState([])

  const [cohortMap, setCohortMap] = useState({})
  const [flowTree, setFlowTree] = useState([])
  const [expandedNodes, setExpandedNodes] = useState(new Set())
  const [cache, setCache] = useState({})
  const [loadingNodes, setLoadingNodes] = useState({})
  const [viewMode, setViewMode] = useState('table')
  const [loadingRoot, setLoadingRoot] = useState(false)

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

    getFlowL1(controls.event, controls.direction, controls.depth, propertyFilter)
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
  }, [refreshToken, controls.event, controls.direction, controls.depth, propertyFilter])

  const cohorts = useMemo(() => {
    const first = flowTree[0]
    return first ? Object.keys(first.values || {}) : Object.keys(cohortMap)
  }, [flowTree, cohortMap])

  const getChildren = (path) => cache[nodeKey(path)] || []

  const onToggle = async (path) => {
    const key = nodeKey(path)
    const depth = path.length - 1
    if (depth >= controls.depth) return

    setExpandedNodes(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })

    if (cache[key]) return

    setLoadingNodes(prev => ({ ...prev, [key]: true }))
    try {
      const resp = await getFlowL2(controls.event, path, controls.direction, controls.depth, propertyFilter)
      setCache(prev => ({ ...prev, [key]: resp.rows || [] }))
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
          Depth
          <select value={controls.depth} onChange={(e) => setControls(prev => ({ ...prev, depth: Number(e.target.value) }))}>
            {[2, 3, 4, 5, 6, 7].map(d => <option key={d} value={d}>{d}</option>)}
          </select>
        </label>
        <label>
          Direction
          <div className="view-toggle" style={{ marginTop: 4 }}>
            <button type="button" className={`view-button ${controls.direction === 'forward' ? 'active' : ''}`} onClick={() => setControls(prev => ({ ...prev, direction: 'forward' }))}>Forward</button>
            <button type="button" className={`view-button ${controls.direction === 'reverse' ? 'active' : ''}`} onClick={() => setControls(prev => ({ ...prev, direction: 'reverse' }))}>Reverse</button>
          </div>
        </label>
        <label>
          View
          <div className="view-toggle" style={{ marginTop: 4 }}>
            <button type="button" className={`view-button ${viewMode === 'table' ? 'active' : ''}`} onClick={() => setViewMode('table')}>Table</button>
            <button type="button" className={`view-button ${viewMode === 'graph' ? 'active' : ''}`} onClick={() => setViewMode('graph')}>Graph</button>
          </div>
        </label>
      </div>

      {loadingRoot ? (
        <div className="table-loading" style={{ padding: '24px 0' }}>Loading flows...</div>
      ) : viewMode === 'table' ? (
        <FlowTable
          rootRows={flowTree}
          cohorts={cohorts}
          cohortMap={cohortMap}
          expandedNodes={expandedNodes}
          loadingNodes={loadingNodes}
          getChildren={getChildren}
          onToggle={onToggle}
          maxDepth={controls.depth}
        />
      ) : (
        <FlowGraph
          rootRows={flowTree}
          cohorts={cohorts}
          expandedNodes={expandedNodes}
          loadingNodes={loadingNodes}
          getChildren={getChildren}
          onToggle={onToggle}
          maxDepth={controls.depth}
        />
      )}
    </section>
  )
}
