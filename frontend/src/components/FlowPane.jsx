import { useEffect, useMemo, useRef, useState } from 'react'
import { getEventProperties, getEventPropertyValues, getFlowL1, getFlowL2, listCohorts, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'
import FlowTable, { nodeKey } from './FlowTable'

const TABLE_MAX_DEPTH = 5
const QUERY_VERSION = 2
const DEFAULT_QUERY = {
  version: QUERY_VERSION,
  event: null,
  property: null,
  value: '',
  direction: 'forward'
}

function validateQuery(q) {
  if (!q || q.version !== QUERY_VERSION) return DEFAULT_QUERY
  if (!q.event) return { ...DEFAULT_QUERY }
  return {
    ...q,
    // Property and value are cleared if event is missing (handled by if above)
    // and value is cleared if property is missing
    value: q.property ? (q.value || '') : ''
  }
}

function getNormalizedQuery(q) {
  if (!q || !q.event) return null
  return {
    event: q.event,
    property: q.property || null,
    value: q.property ? (q.value || '') : '',
    direction: q.direction || 'forward'
  }
}

function getPropertyFilter(normalized) {
  if (!normalized || !normalized.property || !normalized.value) return null
  return { column: normalized.property, operator: '=', values: [normalized.value] }
}

function makeCacheKey(path, normalized, refreshToken, depth, limit) {
  if (!normalized) return null
  return JSON.stringify({
    ...normalized,
    refreshToken,
    depth,
    parent_path: path || [],
    limit: limit || 3
  })
}



export default function FlowPane({ refreshToken, state, setState, appliedFilters = [], onAddToExport }) {
  const [events, setEvents] = useState([])
  const [flowQuery, setFlowQuery] = useState(() => validateQuery(state?.flowQuery))
  const [propertyValues, setPropertyValues] = useState([])
  const [properties, setProperties] = useState([])
  const [error, setError] = useState(null)

  const [cohortMap, setCohortMap] = useState({})
  const [flowTree, setFlowTree] = useState([])
  const [expandedNodes, setExpandedNodes] = useState(new Set())
  const [cache, setCache] = useState({})
  const [loadingNodes, setLoadingNodes] = useState({})
  const [loadingRoot, setLoadingRoot] = useState(false)
  const [nodeExpansion, setNodeExpansion] = useState({})

  useEffect(() => {
    setState({ flowQuery })
  }, [flowQuery, setState])

  const reqIdRef = useRef(0)
  const inFlightRef = useRef(new Map())

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
      setFlowQuery(prev => {
        if (prev.event) return prev
        return { ...prev, event: ev[0] || null }
      })
    }).catch(() => setEvents([]))
  }, [refreshToken])

  useEffect(() => {
    if (!flowQuery.event) {
      setProperties([])
      return
    }
    getEventProperties(flowQuery.event).then(resp => {
      setProperties(resp.properties || [])
    }).catch(() => setProperties([]))
  }, [flowQuery.event])

  useEffect(() => {
    if (!flowQuery.event || !flowQuery.property) {
      setPropertyValues([])
      return
    }
    getEventPropertyValues(flowQuery.event, flowQuery.property).then(resp => {
      setPropertyValues(resp.values || [])
    }).catch(() => setPropertyValues([]))
  }, [flowQuery.event, flowQuery.property])

  useEffect(() => {
    const normalized = getNormalizedQuery(flowQuery)
    if (!normalized) {
      setFlowTree([])
      setLoadingRoot(false)
      return
    }

    const controller = new AbortController()
    let isLatest = true

    setLoadingRoot(true)
    setError(null)

    const filter = getPropertyFilter(normalized)

    getFlowL1(normalized.event, normalized.direction, TABLE_MAX_DEPTH, filter, 3, { signal: controller.signal })
      .then(resp => {
        if (!isLatest) return
        setFlowTree(resp.rows || [])
        // Always reset expansions when the root query changes
        setExpandedNodes(new Set())
        setCache({})
        setNodeExpansion({})
      })
      .catch(err => {
        if (!isLatest || err.name === 'AbortError') return
        console.error('Flow fetch error:', err)
        setError(err.message || 'Failed to load flows')
      })
      .finally(() => {
        if (isLatest) setLoadingRoot(false)
      })

    return () => {
      isLatest = false
      controller.abort()
    }
  }, [refreshToken, flowQuery])

  const cohorts = useMemo(() => {
    const sourceRows = flowTree
    const first = sourceRows?.[0]
    return first ? Object.keys(first.values || {}) : Object.keys(cohortMap)
  }, [flowTree, cohortMap])

  const getNormalizedActive = () => getNormalizedQuery(flowQuery)

  const getChildren = (path) => {
    const normalized = getNormalizedActive()
    const depth = path.length - 1
    const key = nodeKey(path)
    const limit = nodeExpansion[`${depth}_${key}`] || 3
    return cache[makeCacheKey(path, normalized, refreshToken, TABLE_MAX_DEPTH, limit)] || []
  }

  const onToggle = async (path) => {
    const normalized = getNormalizedActive()
    if (!normalized) return

    const key = nodeKey(path)
    const depth = path.length - 1
    const limit = nodeExpansion[`${depth}_${key}`] || 3
    const cacheKey = makeCacheKey(path, normalized, refreshToken, TABLE_MAX_DEPTH, limit)
    
    if (depth >= TABLE_MAX_DEPTH) return

    setExpandedNodes(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })

    if (cache[cacheKey] || inFlightRef.current.has(cacheKey)) return

    setLoadingNodes(prev => ({ ...prev, [key]: true }))
    setError(null)

    const filter = getPropertyFilter(normalized)
    const promise = getFlowL2(normalized.event, path, normalized.direction, TABLE_MAX_DEPTH, filter, limit)
    inFlightRef.current.set(cacheKey, promise)

    try {
      const resp = await promise
      setCache(prev => ({ ...prev, [cacheKey]: resp.rows || [] }))
    } catch (err) {
      if (err.name !== 'AbortError') {
        console.error('L2 toggle error:', err)
        setError('Failed to load child transitions')
      }
    } finally {
      inFlightRef.current.delete(cacheKey)
      setLoadingNodes(prev => ({ ...prev, [key]: false }))
    }
  }

  const onExpandOther = async (path) => {
    const normalized = getNormalizedActive()
    if (!normalized) return

    const key = nodeKey(path)
    const depth = path.length - 1
    const nextLimit = (nodeExpansion[`${depth}_${key}`] || 3) + 3
    const stateKey = `${depth}_${key}`
    const cacheKey = makeCacheKey(path, normalized, refreshToken, TABLE_MAX_DEPTH, nextLimit)

    if (inFlightRef.current.has(cacheKey)) return

    setNodeExpansion(prev => ({ ...prev, [stateKey]: nextLimit }))
    
    if (depth === 0) {
      setLoadingRoot(true)
      setError(null)
      const filter = getPropertyFilter(normalized)
      const promise = getFlowL1(normalized.event, normalized.direction, TABLE_MAX_DEPTH, filter, nextLimit)
      inFlightRef.current.set(cacheKey, promise)
      try {
        const resp = await promise
        setFlowTree(resp.rows || [])
      } catch (err) {
        if (err.name !== 'AbortError') setError('Failed to expand root transitions')
      } finally {
        inFlightRef.current.delete(cacheKey)
        setLoadingRoot(false)
      }
    } else {
      setLoadingNodes(prev => ({ ...prev, [key]: true }))
      setError(null)
      const filter = getPropertyFilter(normalized)
      const promise = getFlowL2(normalized.event, path, normalized.direction, TABLE_MAX_DEPTH, filter, nextLimit)
      inFlightRef.current.set(cacheKey, promise)
      try {
        const resp = await promise
        setCache(prev => ({ ...prev, [cacheKey]: resp.rows || [] }))
      } catch (err) {
        if (err.name !== 'AbortError') setError('Failed to expand child transitions')
      } finally {
        inFlightRef.current.delete(cacheKey)
        setLoadingNodes(prev => ({ ...prev, [key]: false }))
      }
    }
  }

  const handleAddToExport = () => {
    if (!flowTree || flowTree.length === 0) return

    const edges = []
    const cohortsToExport = cohorts // from useMemo

    // Recursive function to build edge list from expanded nodes
    const walk = (nodes, currentPath = []) => {
      nodes.forEach(node => {
        const eventName = node.path[node.path.length - 1]
        const key = nodeKey(node.path)
        
        if (expandedNodes.has(key)) {
          const children = getChildren(node.path)
          if (children && children.length > 0) {
            children.forEach(child => {
              const childEvent = child.path[child.path.length - 1]
              cohortsToExport.forEach(cId => {
                const val = child.values?.[cId]
                if (!val) return

                const sourceUsers = Number(val.parent_users || 0)
                const targetUsers = Number(val.user_count || 0)

                edges.push({
                  cohort: cohortMap[cId]?.name || `Cohort ${cId}`,
                  cohort_size: cohortMap[cId]?.size || 0,
                  source_step: node.path.length - 1,
                  source_event: eventName,
                  source_users: sourceUsers,
                  target_step: node.path.length,
                  target_event: childEvent,
                  target_users: targetUsers,
                  transition_pct: sourceUsers > 0 ? targetUsers / sourceUsers : 0
                })
              })
            })
            walk(children, node.path)
          }
        }
      })
    }

    // Add initial edges from the Start Event (Step 0) to Step 1 nodes
    flowTree.forEach(node => {
      const eventName = node.path[node.path.length - 1]
      cohortsToExport.forEach(cId => {
        const val = node.values?.[cId]
        if (!val) return

        const sourceUsers = Number(val.parent_users || 0)
        const targetUsers = Number(val.user_count || 0)

        edges.push({
          cohort: cohortMap[cId]?.name || `Cohort ${cId}`,
          cohort_size: cohortMap[cId]?.size || 0,
          source_step: 0,
          source_event: flowQuery.event,
          source_users: sourceUsers,
          target_step: 1,
          target_event: eventName,
          target_users: targetUsers,
          transition_pct: sourceUsers > 0 ? targetUsers / sourceUsers : 0
        })
      })
    })

    walk(flowTree)

    const payload = {
      id: crypto.randomUUID(),
      version: 2,
      type: 'flow',
      title: `Flow — ${flowQuery.event}`,
      summary: `Flow analysis for ${flowQuery.event} (${flowQuery.direction})`,
      tables: [{
        title: `Flow Edge List (${flowQuery.direction === 'forward' ? 'Steps After' : 'Steps Before'} ${flowQuery.event})`,
        columns: [
          { key: 'cohort', label: 'Cohort', type: 'string' },
          { key: 'cohort_size', label: 'Cohort Size', type: 'number' },
          { key: 'source_step', label: 'Source Step', type: 'number' },
          { key: 'source_event', label: 'Source Event', type: 'string' },
          { key: 'source_users', label: 'Source Users', type: 'number' },
          { key: 'target_step', label: 'Target Step', type: 'number' },
          { key: 'target_event', label: 'Target Event', type: 'string' },
          { key: 'target_users', label: 'Target Users', type: 'number' },
          { key: 'transition_pct', label: 'Transition %', type: 'percentage' }
        ],
        data: edges
      }],
      meta: {
        filters: appliedFilters,
        cohorts: cohortsToExport.map(id => ({ cohort_id: id, name: cohortMap[id]?.name })),
        settings: {
          'Start Event': flowQuery.event,
          'Direction': flowQuery.direction,
          'Property Filter': flowQuery.property ? `${flowQuery.property} = ${flowQuery.value || 'All'}` : 'None'
        }
      }
    }
    onAddToExport(payload)
  }

  const onEventChange = (event) => setFlowQuery(prev => ({ ...prev, event, property: null, value: '' }))
  const onPropertyChange = (property) => setFlowQuery(prev => ({ ...prev, property: property || null, value: '' }))
  const onValueChange = (value) => setFlowQuery(prev => ({ ...prev, value }))
  const onDirectionToggle = (direction) => setFlowQuery(prev => ({ ...prev, direction }))


  return (
    <section className="card" style={{ position: 'relative' }}>
      <h2>Flow Explorer</h2>
      <div className="inline-controls" style={{ marginBottom: 16 }}>
        <label>
          Event
          <SearchableSelect options={events} value={flowQuery.event} onChange={onEventChange} placeholder="Select event" />
        </label>
        <label>
          Property
          <select
            value={flowQuery.property || ''}
            disabled={!flowQuery.event}
            onChange={(e) => onPropertyChange(e.target.value)}
          >
            <option value="">None</option>
            {properties.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </label>
        <label>
          Property Value
          <select value={flowQuery.value} disabled={!flowQuery.property} onChange={(e) => onValueChange(e.target.value)}>
            <option value="">All</option>
            {propertyValues.map(v => <option key={v} value={v}>{v}</option>)}
          </select>
        </label>
        <label>
          Direction
          <div className="view-toggle" style={{ marginTop: 4 }}>
            <button type="button" className={`view-button ${flowQuery.direction === 'forward' ? 'active' : ''}`} onClick={() => onDirectionToggle('forward')}>Forward</button>
            <button type="button" className={`view-button ${flowQuery.direction === 'reverse' ? 'active' : ''}`} onClick={() => onDirectionToggle('reverse')}>Reverse</button>
          </div>
        </label>
        <button
          type="button"
          className="button button-secondary"
          onClick={handleAddToExport}
          disabled={loadingRoot || flowTree.length === 0}
          title="Add edge list of expanded nodes to global export buffer"
          style={{ height: 36, marginTop: 'auto' }}
        >
          📸 Add to Export
        </button>
      </div>

      {error && (
        <div style={{ background: '#fee2e2', color: '#b91c1c', padding: 12, borderRadius: 6, marginBottom: 16 }}>
          {error}
        </div>
      )}

      <div style={{ position: 'relative', minHeight: 200 }}>
        {loadingRoot && (
           <div style={{
             position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
             background: 'rgba(255,255,255,0.6)', zIndex: 10,
             display: 'flex', alignItems: 'center', justifyContent: 'center',
             borderRadius: 8
           }}>
             <div className="table-loading">Refreshing flows...</div>
           </div>
        )}
        <FlowTable
          rootRows={flowTree}
          cohorts={cohorts}
          cohortMap={cohortMap}
          expandedNodes={expandedNodes}
          loadingNodes={loadingNodes}
          getChildren={getChildren}
          onToggle={onToggle}
          onExpandOther={onExpandOther}
          nodeExpansion={nodeExpansion}
          maxDepth={TABLE_MAX_DEPTH}
        />
      </div>
    </section>
  )
}
