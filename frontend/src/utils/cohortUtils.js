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

export const buildCohortDefinition = (cohort) => {
  const logic = cohort.condition_logic || cohort.logic_operator || 'AND'
  const conditionLines = (cohort.conditions || []).map((condition) => {
    const property = condition.property_filter ? formatPropertyFilter(condition.property_filter) : ''
    return `${condition.event_name} ≥ ${condition.min_event_count}${property}`
  })
  return [`Logic: ${logic}`, ...conditionLines, describeJoinType(cohort.join_type)].join(' • ')
}
