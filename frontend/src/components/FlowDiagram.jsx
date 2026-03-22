import { useMemo } from 'react'
import dagre from 'dagre'
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

function layout(nodes, edges) {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 30, ranksep: 80 })
  g.setDefaultEdgeLabel(() => ({}))

  nodes.forEach(n => g.setNode(n.id, { width: 170, height: 60 }))
  edges.forEach(e => g.setEdge(e.source, e.target))
  dagre.layout(g)

  return nodes.map(n => {
    const pos = g.node(n.id) || { x: 0, y: 0 }
    return { ...n, position: { x: pos.x, y: pos.y } }
  })
}

export function buildGraphFromTree(rootRows, treeMap, depth, cohortId) {
  const edgesMap = new Map()
  const nodeUsers = new Map()

  const walk = (rows) => {
    ;(rows || []).forEach(row => {
      if (row.path.length - 1 > depth) return
      const source = row.path[row.path.length - 2]
      const target = row.path[row.path.length - 1]
      if (!source || !target || target === 'Other') return

      const val = row.values?.[cohortId]
      const users = val?.user_count ?? val?.count ?? 0
      const key = `${source}→${target}`
      const existing = edgesMap.get(key)
      if (existing) {
        existing.users += users
      } else {
        edgesMap.set(key, {
          id: key,
          source,
          target,
          users,
          continue_pct: val?.continue_pct ?? 0,
          dropoff_pct: val?.dropoff_pct ?? 0,
          median_time_sec: val?.median_time_sec ?? null,
          p90_time_sec: val?.p90_time_sec ?? null,
          selfLoop: source === target,
        })
      }

      nodeUsers.set(target, Math.max(nodeUsers.get(target) || 0, users))
      nodeUsers.set(source, Math.max(nodeUsers.get(source) || 0, Math.round(users / Math.max(val?.pct_of_parent || val?.pct || 0.000001, 0.000001))))

      const children = treeMap?.[row.path.join('||')] || []
      walk(children)
    })
  }

  walk(rootRows)

  const nodes = Array.from(nodeUsers.entries()).map(([event, users]) => ({
    id: event,
    data: { label: `${event}\n${users.toLocaleString()} users` },
    position: { x: 0, y: 0 },
    style: { border: '1px solid #cbd5e1', borderRadius: 6, padding: 8, width: 170, background: 'white', whiteSpace: 'pre-line' },
  }))

  const totalBySource = {}
  Array.from(edgesMap.values()).forEach(e => {
    totalBySource[e.source] = (totalBySource[e.source] || 0) + e.users
  })

  const edges = Array.from(edgesMap.values()).map(e => {
    const sourceUsers = nodeUsers.get(e.source) || totalBySource[e.source] || 1
    const pct = sourceUsers > 0 ? e.users / sourceUsers : 0
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      type: e.selfLoop ? 'smoothstep' : 'default',
      markerEnd: { type: MarkerType.ArrowClosed },
      label: `→ ${formatPct(pct)}\n↓ ${formatPct(e.dropoff_pct)}\n${formatTime(e.median_time_sec)} / ${formatTime(e.p90_time_sec)}`,
      style: { strokeWidth: Math.max(1, Math.min(10, pct * 12)) },
      data: { ...e, pct },
      title: `Continue ${formatPct(e.continue_pct)} | Drop-off ${formatPct(e.dropoff_pct)} | Median ${formatTime(e.median_time_sec)} | P90 ${formatTime(e.p90_time_sec)}`,
    }
  })

  return { nodes, edges }
}

export default function FlowDiagram({ data }) {
  const graph = useMemo(() => {
    if (!data || !data.nodes?.length) return { nodes: [], edges: [] }
    return { ...data, nodes: layout(data.nodes, data.edges) }
  }, [data])

  if (!graph.nodes.length) return <p>No transitions found</p>

  return (
    <div style={{ height: 420, width: '100%', overflow: 'auto', border: '1px solid #e5e7eb', borderRadius: 8 }}>
      <ReactFlow
        nodes={graph.nodes}
        edges={graph.edges}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={true}
      >
        <Background gap={16} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  )
}
