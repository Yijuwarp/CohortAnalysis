import { formatCurrency } from './utils/formatters'

export function buildMonetizationRows({ cohortSizes, retainedRows, revenueRows, eligibilityRows, dayColumns, metricType }) {
  const availabilityByKey = new Map(eligibilityRows.map((row) => [`${row.cohort_id}:${row.day_number}`, { eligible_users: row.eligible_users, cohort_size: (cohortSizes.find(c => c.cohort_id === row.cohort_id)?.size || 0) }]))
  const sizeByCohort = new Map(cohortSizes.map((row) => [row.cohort_id, row]))
  const retainedByKey = new Map(retainedRows.map((row) => [`${row.cohort_id}:${row.day_number}`, Number(row.retained_users)]))
  const revenueByKey = new Map(revenueRows.map((row) => [`${row.cohort_id}:${row.day_number}`, Number(row.revenue)]))

  return cohortSizes.map((cohort) => {
    const numericValues = {}
    const displayValues = {}
    const availabilityValues = {}
    let running = 0

    for (const day of dayColumns) {
      const key = `${cohort.cohort_id}:${day}`
      const dailyRevenue = revenueByKey.get(key) ?? 0
      running += dailyRevenue
      const acquiredSize = Number(sizeByCohort.get(cohort.cohort_id)?.size ?? 0)
      const retained = retainedByKey.get(key) ?? 0
      const availability = availabilityByKey.get(key) ?? { eligible_users: 0, cohort_size: (sizeByCohort.get(cohort.cohort_id)?.size || 0) }

      let numericValue = null

      if (metricType === 'total_revenue') {
        numericValue = dailyRevenue
      } else if (metricType === 'cumulative_revenue') {
        numericValue = running
      } else if (metricType === 'revenue_per_acquired_user') {
        numericValue = acquiredSize > 0 ? dailyRevenue / acquiredSize : null
      } else if (metricType === 'cumulative_revenue_per_acquired_user') {
        numericValue = acquiredSize > 0 ? running / acquiredSize : null
      } else {
        numericValue = retained > 0 ? dailyRevenue / retained : null
      }

      numericValues[String(day)] = numericValue
      displayValues[String(day)] = formatCurrency(numericValue)
      availabilityValues[String(day)] = availability
    }

    return {
      cohort_id: cohort.cohort_id,
      cohort_name: cohort.cohort_name,
      size: cohort.size,
      numericValues,
      displayValues,
      availabilityValues
    }
  })
}
