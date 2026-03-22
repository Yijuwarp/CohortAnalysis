class SimpleGraph {
  constructor() {
    this.nodes = new Map()
    this.edges = []
    this.config = { ranksep: 140, nodesep: 80, marginx: 20, marginy: 20 }
  }

  setDefaultEdgeLabel() {}

  setGraph(cfg) {
    this.config = { ...this.config, ...(cfg || {}) }
  }

  setNode(id, data) {
    this.nodes.set(id, { ...(data || {}), id })
  }

  setEdge(source, target) {
    this.edges.push({ source, target })
  }

  node(id) {
    return this.nodes.get(id)
  }
}

function layout(graph) {
  let index = 0
  for (const [id, node] of graph.nodes.entries()) {
    const x = graph.config.marginx + (index % 4) * graph.config.ranksep
    const y = graph.config.marginy + Math.floor(index / 4) * graph.config.nodesep
    graph.nodes.set(id, { ...node, x, y })
    index += 1
  }
}

export default {
  graphlib: { Graph: SimpleGraph },
  layout,
}
