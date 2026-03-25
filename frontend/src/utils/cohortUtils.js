const isMultiOperator = (operator) => operator === 'IN' || operator === 'NOT IN'

const formatPropertyFilter = (propertyFilter) => {
  if (!propertyFilter) {
    return ''
  }

  const formattedValues = Array.isArray(propertyFilter.values)
    ? propertyFilter.values.join(', ')
    : propertyFilter.values

  if (isMultiOperator(propertyFilter.operator)) {
    return ` WHERE ${propertyFilter.column} ${propertyFilter.operator} (${formattedValues})`
  }

  return ` WHERE ${propertyFilter.column} ${propertyFilter.operator} ${formattedValues}`
}

const describeJoinType = (joinType) => (joinType === 'first_event' ? 'Join on first event' : 'Join when condition is met')

export const formatCohortLogic = (cohort) => {
  const data = cohort.definition || cohort
  const logic = data.condition_logic || data.logic_operator || 'AND'
  const conditionLines = (data.conditions || []).map((condition) => {
    const property = condition.property_filter ? formatPropertyFilter(condition.property_filter) : ''
    return `${condition.event_name} ≥ ${condition.min_event_count}${property}`
  })
  return [`Logic: ${logic}`, ...conditionLines, describeJoinType(data.join_type)].join(' • ')
}

/**
 * Returns tooltip text for a child (split) cohort.
 *   For property splits: "Parent: X\nFilter: col = val"
 *   For _other:          "Parent: X\nFilter: col NOT IN [v1, v2]"
 *   For random splits:   "Parent: X\nGroup: N of total"
 *   Fallback: same as formatCohortLogic
 */
export function formatChildCohortTooltip(child, parentName, siblings) {
  const { split_type, split_property, split_value, split_group_index, split_group_total } = child

  if (split_type === 'property') {
    if (split_value === '__OTHER__') {
      // Collect sibling non-other values for the NOT IN list
      const selectedVals = (siblings || [])
        .filter(s => s.split_type === 'property' && s.split_value !== '__OTHER__')
        .map(s => s.split_value)
      const valList = selectedVals.length ? selectedVals.join(', ') : '…'
      return `Parent: ${parentName}\nFilter: ${split_property} NOT IN [${valList}]`
    }
    return `Parent: ${parentName}\nFilter: ${split_property} = ${split_value}`
  }

  if (split_type === 'random') {
    const groupNum = split_value || (split_group_index != null ? split_group_index + 1 : '?')
    const total = split_group_total || '?'
    return `Parent: ${parentName}\nGroup: ${groupNum} of ${total} (random)`
  }

  // Fallback for legacy or unknown split types
  return formatCohortLogic(child)
}

export function getNextName(name) {
  if (!name || name.trim() === '') return name
  const match = name.match(/\((\d+)\)$/)
  if (match) {
    const num = parseInt(match[1], 10)
    return name.replace(/\(\d+\)$/, `(${num + 1})`)
  }
  return `${name} (1)`
}
