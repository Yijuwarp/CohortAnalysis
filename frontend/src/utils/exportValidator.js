export function validateExportSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    throw new Error('Export snapshot must be an object')
  }

  const { id, version, type, title, summary, tables, meta } = snapshot

  if (!id) throw new Error('Missing snapshot id')
  if (version !== 2) throw new Error('Invalid schema version, expected 2')
  if (!['retention', 'usage', 'monetization', 'paths', 'flow', 'user-explorer'].includes(type)) {
    throw new Error(`Invalid type: ${type}`)
  }
  if (typeof title !== 'string' || !title) throw new Error('Missing or invalid title')
  if (typeof summary !== 'string' || !summary) throw new Error('Missing or invalid summary')

  if (!Array.isArray(tables) || tables.length === 0) {
    throw new Error('Tables must be a non-empty array in version 2')
  }

  let totalRows = 0
  tables.forEach((table, tIdx) => {
    if (!table.title || typeof table.title !== 'string') {
      throw new Error(`Table at index ${tIdx} missing title`)
    }
    if (!Array.isArray(table.columns) || table.columns.length === 0) {
      throw new Error(`Table ${table.title} has no columns`)
    }
    if (!Array.isArray(table.data)) {
      throw new Error(`Table ${table.title} data must be an array`)
    }

    table.columns.forEach((c, i) => {
      if (!c.key || typeof c.key !== 'string') throw new Error(`Table ${table.title}, column ${i} missing key`)
      if (!c.label || typeof c.label !== 'string') throw new Error(`Table ${table.title}, column ${i} missing label`)
      if (!['string', 'number', 'percentage', 'currency'].includes(c.type)) {
        throw new Error(`Table ${table.title}, column ${i} has invalid type: ${c.type}`)
      }
    })

    totalRows += table.data.length
  })

  if (totalRows > 15000) {
    throw new Error(`Total data rows (${totalRows}) exceeds limits (15,000)`)
  }

  if (!meta || typeof meta !== 'object') {
    throw new Error('Meta must be an object')
  }

  if (!Array.isArray(meta.filters)) throw new Error('Meta filters must be an array')
  if (!Array.isArray(meta.cohorts)) throw new Error('Meta cohorts must be an array')
  if (typeof meta.settings !== 'object') throw new Error('Meta settings must be an object')

  return true
}
