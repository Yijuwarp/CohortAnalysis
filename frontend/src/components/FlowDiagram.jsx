import { useMemo } from 'react'
import dagre from 'dagre'
import ReactFlow, { Background, Controls, MarkerType } from 'reactflow'

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

function buildLayout(nodes, edges) {
  const dagreGraph = new dagre.graphlib.Graph()
  dagreGraph.setDefaultEdgeLabel(() => ({}))
  dagreGraph.setGraph({
    rankdir: 'LR',
    align: 'UL',
    nodesep: 80,
    ranksep: 140,
    marginx: 20,
    marginy: 20,
  })

  nodes.forEach((node) => dagreGraph.setNode(node.id, { width: 220, height: 80, rank: node.rank || 1 }))
  edges.forEach((edge) => dagreGraph.setEdge(edge.source, edge.target))
  dagre.layout(dagreGraph)

  return nodes.map((node) => {
    const pos = dagreGraph.node(node.id) || { x: 0, y: 0 }
    return { ...node, position: { x: pos.x, y: pos.y } }
  })
}

export function buildGraphFromTree(flowTree, rootEvent, direction, options = {}) {
  const { cohortId, graphDepth = 7, treeMap = {} } = options

  const edgeAgg = new Map()
  const nodeUsage = new Map()
  const nodeDepth = new Map([[rootEvent, 1]])

  const walk = (rows) => {
    ;(rows || []).forEach((row) => {
      const level = row.path.length - 1
      if (level > graphDepth) return

      const source = row.path[row.path.length - 2]
      const target = row.path[row.path.length - 1]
      if (!source || !target || target === 'Other') return

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
      nodeDepth.set(source, Math.min(nodeDepth.get(source) || level, level))
      nodeDepth.set(target, Math.min(nodeDepth.get(target) || (level + 1), level + 1))

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

  const activeNodes = new Set([rootEvent])
  filteredEdges.forEach((edge) => {
    activeNodes.add(edge.source)
    activeNodes.add(edge.target)
  })

  const nodes = Array.from(activeNodes).map((event) => ({
    id: event,
    data: {
      label: event,
      users: nodeUsage.get(event) || 0,
      isRoot: event === rootEvent,
    },
    position: { x: 0, y: 0 },
    style: {
      width: 220,
      height: 80,
      border: event === rootEvent ? '2px solid #2563eb' : '1px solid #cbd5e1',
      borderRadius: 8,
      background: '#fff',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      textAlign: 'center',
      whiteSpace: 'pre-line',
      padding: 8,
      fontWeight: 600,
      fontSize: 14,
      color: '#111827',
    },
    sourcePosition: 'right',
    targetPosition: 'left',
    rank: nodeDepth.get(event) || 1,
  }))

  const edges = filteredEdges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: 'smoothstep',
    markerEnd: { type: MarkerType.ArrowClosed },
    label: formatPct(edge.pct),
    style: {
      strokeWidth: Math.max(1, edge.pct * 8),
      strokeDasharray: edge.source === edge.target ? '5 4' : undefined,
    },
    data: edge,
  }))

  return { nodes, edges, rootEvent, direction }
}

export default function FlowDiagram({ data }) {
  const graph = useMemo(() => {
    if (!data || !data.nodes?.length) return { nodes: [], edges: [] }

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

    return { nodes: buildLayout(nodes, edges), edges }
  }, [data])

  if (!graph.nodes.length) return <p>No transitions found</p>

  return (
    <div style={{ height: 520, width: '100%', overflow: 'auto', border: '1px solid #e5e7eb', borderRadius: 8 }}>
      <ReactFlow
        nodes={graph.nodes}
        edges={graph.edges}
        fitView
        panOnScroll
        panOnDrag
      >
        <Background gap={20} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  )
}
