import React from 'react'

export const MarkerType = { ArrowClosed: 'arrowclosed' }

export function Background() {
  return null
}

export function Controls() {
  return null
}

export default function ReactFlow({ nodes, edges }) {
  return (
    <div style={{ minWidth: 900, minHeight: 480, position: 'relative', padding: 16 }}>
      <div style={{ marginBottom: 12, color: '#6b7280' }}>{edges?.length || 0} edges</div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
        {(nodes || []).map((node) => (
          <div key={node.id} style={node.style} title={node.id}>{node.data?.label}</div>
        ))}
      </div>
      <ul style={{ marginTop: 12, fontSize: 12, color: '#4b5563' }}>
        {(edges || []).map((edge) => (
          <li key={edge.id} title={edge.title}>{edge.source} → {edge.target} ({edge.label})</li>
        ))}
      </ul>
    </div>
  )
}
