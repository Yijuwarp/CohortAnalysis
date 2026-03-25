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

export function getNextName(name) {
  if (!name || name.trim() === '') return name
  const match = name.match(/\((\d+)\)$/)
  if (match) {
    const num = parseInt(match[1], 10)
    return name.replace(/\(\d+\)$/, `(${num + 1})`)
  }
  return `${name} (1)`
}
