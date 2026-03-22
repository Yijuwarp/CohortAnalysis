class Graph {
  constructor() {
    this._nodes = new Map()
  }

  setGraph() {}
  setDefaultEdgeLabel() {}

  setNode(id, value = {}) {
    this._nodes.set(id, value)
  }

  setEdge() {}

  node() {
    return { x: 0, y: 0 }
  }
}

function layout() {}

export default {
  __isShim: true,
  graphlib: { Graph },
  layout,
}
