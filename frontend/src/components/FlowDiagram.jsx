import { useEffect, useState } from 'react'
import ReactFlow, { Background, Controls, MarkerType } from 'reactflow'
import 'reactflow/dist/style.css'

function formatPct(v) {
  return `${((v || 0) * 100).toFixed(1)}%`
}

function formatTime(sec) {
  if (sec === null || sec === undefined) return '—'
  const s = Math.max(0, Math.round(sec))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
}

let dagreLib = null

async function getDagre() {
  if (dagreLib) return dagreLib

  try {
    dagreLib = await import('dagre')
  } catch {
    dagreLib = await import('../shims/dagre')
  }

  return dagreLib.default || dagreLib
}

function isRealDagre(dagre) {
  if (dagre.__isShim) return false

  try {
    const g = new dagre.graphlib.Graph()
    g.setGraph({})
    g.setNode('a', { width: 100, height: 50 })
    dagre.layout(g)
    const pos = g.node('a')
    return pos && pos.x !== 0
  } catch {
    return false
  }
}

function fallbackLayout(nodes) {
  const levels = {}

  nodes.forEach((node) => {
    const level = node.rank || 1
    if (!levels[level]) levels[level] = []
    levels[level].push(node)
  })

  const result = []

  Object.entries(levels).forEach(([level, levelNodes]) => {
    levelNodes.forEach((node, i) => {
      result.push({
        ...node,
        position: {
          x: (Number(level) - 1) * 320,
          y: i * 120,
        },
      })
    })
  })

  return result
}

function compactVertical(nodes) {
  const levels = {}

  nodes.forEach((n) => {
    const level = n.rank || 1
    if (!levels[level]) levels[level] = []
    levels[level].push(n)
  })

  Object.values(levels).forEach((levelNodes) => {
    levelNodes.sort((a, b) => (b.data.users || 0) - (a.data.users || 0))

    levelNodes.forEach((node, i) => {
      node.position.y = i * 150
    })
  })

  return nodes
}

async function buildLayout(nodes, edges) {
  const dagre = await getDagre()

  if (!isRealDagre(dagre)) {
    console.warn('Using fallback layout (dagre unavailable)')
    return fallbackLayout(nodes)
  }

  const dagreGraph = new dagre.graphlib.Graph()
  dagreGraph.setDefaultEdgeLabel(() => ({}))
  dagreGraph.setGraph({
    rankdir: 'LR',
    nodesep: 100,
    ranksep: 180,
    marginx: 40,
    marginy: 40,
  })

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, {
      width: 220,
      height: 80,
    })
  })

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target)
  })

  dagre.layout(dagreGraph)

  return nodes.map((node) => {
    const pos = dagreGraph.node(node.id)

    if (!pos || !Number.isFinite(pos.x) || !Number.isFinite(pos.y)) {
      return {
        ...node,
        position: { x: 0, y: 0 },
      }
    }

    return {
      ...node,
      position: {
        x: pos.x - 110,
        y: pos.y - 40,
      },
    }
  })
}

export function buildGraphFromTree(flowTree, rootEvent, direction, options = {}) {
  const { cohortId, graphDepth = 7, treeMap = {} } = options

  const edgeAgg = new Map()
  const nodeUsage = new Map()
  const nodeLoop = new Map()
  const rootNodeId = `${rootEvent}__1`

  const walk = (rows) => {
    ;(rows || []).forEach((row) => {
      const event = row.path[row.path.length - 1]
      const childDepth = row.path.length
      if (childDepth - 1 > graphDepth) return
      if (event === 'Other') return

      const parentEvent = row.path[row.path.length - 2]
      const childEvent = row.path[row.path.length - 1]

      const parentDepth = row.path.length - 1
      const source = `${parentEvent}__${parentDepth}`
      const target = `${childEvent}__${childDepth}`
      const isLoop = row.path.slice(0, -1).includes(event)
      if (isLoop) {
        nodeLoop.set(target, true)
      }

      const val = row.values?.[cohortId]
      const users = Number(val?.user_count ?? val?.count ?? 0)
      if (users <= 0) return
      const pctOfParent = Number(val?.pct_of_parent ?? val?.pct ?? 0)
      const parentUsers = users / (pctOfParent || 1)

      const key = `${source}→${target}`
      const existing = edgeAgg.get(key)
      if (existing) {
        existing.users += users
        existing.parentUsers += parentUsers
      } else {
        edgeAgg.set(key, {
          id: key,
          source,
          target,
          users,
          parentUsers,
          continue_pct: Number(val?.continue_pct ?? 0),
          dropoff_pct: Number(val?.dropoff_pct ?? 0),
          median_time_sec: val?.median_time_sec ?? null,
          p90_time_sec: val?.p90_time_sec ?? null,
        })
      }

      nodeUsage.set(source, Math.max(nodeUsage.get(source) || 0, Math.round(parentUsers)))
      nodeUsage.set(target, Math.max(nodeUsage.get(target) || 0, users))

      const children = treeMap[row.path.join('||')] || []
      walk(children)
    })
  }

  walk(flowTree)

  const edgesBySource = {}
  Array.from(edgeAgg.values()).forEach((edge) => {
    const pct = Math.min(1, edge.parentUsers > 0 ? edge.users / edge.parentUsers : 0)
    if (!edgesBySource[edge.source]) edgesBySource[edge.source] = []
    edgesBySource[edge.source].push({ ...edge, pct })
  })

  const filteredEdges = []
  Object.values(edgesBySource).forEach((sourceEdges) => {
    sourceEdges
      .sort((a, b) => b.pct - a.pct)
      .slice(0, 3)
      .filter((edge) => edge.pct >= 0.01)
      .forEach((edge) => filteredEdges.push(edge))
  })

  const activeNodes = new Set([rootNodeId])
  filteredEdges.forEach((edge) => {
    activeNodes.add(edge.source)
    activeNodes.add(edge.target)
  })

  const nodes = Array.from(activeNodes).map((nodeId) => {
    const [event, depthStr] = nodeId.split('__')
    const depth = Number(depthStr)
    const isLoop = nodeLoop.get(nodeId) || false

    return {
      id: nodeId,
    data: {
      label: `${event}${isLoop ? ' ↺' : ''}`,
      users: nodeUsage.get(nodeId) || 0,
      isRoot: nodeId === rootNodeId,
      isLoop,
    },
    position: { x: 0, y: 0 },
    style: {
      minWidth: 180,
      maxWidth: 260,
      minHeight: 70,
      border: isLoop ? '2px dashed #f59e0b' : (nodeId === rootNodeId ? '2px solid #2563eb' : '1px solid #e5e7eb'),
      borderRadius: 12,
      background: isLoop ? '#fffbeb' : '#fff',
      boxShadow: '0 1px 2px rgba(0,0,0,0.06)',
      padding: 10,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      textAlign: 'center',
      whiteSpace: 'pre-line',
    },
    title: isLoop ? 'Users returned to this event (loop)' : '',
    sourcePosition: 'right',
    targetPosition: 'left',
      rank: depth,
    }
  })

  const edges = filteredEdges.map((edge) => ({
    ...(edge.pct > 0.3 ? { isPrimary: true } : {}),
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: 'smoothstep',
    markerEnd: { type: MarkerType.ArrowClosed },
    label: formatPct(edge.pct),
    title: `${Math.round(edge.users).toLocaleString()} users`,
    style: {
      stroke: edge.pct > 0.3 ? '#2563eb' : '#94a3b8',
      strokeWidth: edge.pct > 0.3 ? 4 : Math.max(1.5, edge.pct * 8),
      opacity: edge.pct >= 0.01 ? 1 : 0.4,
    },
    data: edge,
  }))

  return { nodes, edges, rootEvent, direction }
}

export default function FlowDiagram({ data }) {
  const [graph, setGraph] = useState({ nodes: [], edges: [] })

  useEffect(() => {
    let cancelled = false

    async function compute() {
      if (!data?.nodes?.length) {
        setGraph({ nodes: [], edges: [] })
        return
      }

      const nodes = data.nodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          label: `${node.data.label}${node.data.isRoot ? ' (Start)' : ''}\n${Number(node.data.users || 0).toLocaleString()} users`,
        },
      }))

      const edges = data.edges.map((edge) => ({
        ...edge,
        title: `Continue ${formatPct(edge.data?.continue_pct)} | Drop-off ${formatPct(edge.data?.dropoff_pct)} | Median ${formatTime(edge.data?.median_time_sec)} | P90 ${formatTime(edge.data?.p90_time_sec)}`,
      }))

      const layoutedNodes = await buildLayout(nodes, edges)
      const LEVEL_GAP_X = 320

      layoutedNodes.forEach((node) => {
        const level = node.rank || 1
        node.position.x = (level - 1) * LEVEL_GAP_X
      })

      const compactedNodes = compactVertical(layoutedNodes)
      const minY = Math.min(...compactedNodes.map((n) => n.position.y))
      compactedNodes.forEach((n) => {
        n.position.y -= minY
      })

      if (!cancelled) {
        setGraph({ nodes: compactedNodes, edges })
      }
    }

    compute()

    return () => {
      cancelled = true
    }
  }, [data])

  if (!graph.nodes.length || !graph.edges.length) return <p>No transitions found</p>

  return (
    <div style={{ height: 520, width: '100%', overflow: 'auto', border: '1px solid #e5e7eb', borderRadius: 8 }}>
      <ReactFlow
        nodes={graph.nodes}
        edges={graph.edges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        panOnScroll
        panOnDrag
      >
        <Background gap={20} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  )
}
