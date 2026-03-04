import { formatCurrency } from './utils/formatters'

export function buildMonetizationRows({ cohortSizes, retainedRows, revenueRows, dayColumns, metricType }) {
  const sizeByCohort = new Map(cohortSizes.map((row) => [row.cohort_id, row]))
  const retainedByKey = new Map(retainedRows.map((row) => [`${row.cohort_id}:${row.day_number}`, Number(row.retained_users)]))
  const revenueByKey = new Map(revenueRows.map((row) => [`${row.cohort_id}:${row.day_number}`, Number(row.revenue)]))

  return cohortSizes.map((cohort) => {
    const numericValues = {}
    const displayValues = {}
    let running = 0

    for (const day of dayColumns) {
      const key = `${cohort.cohort_id}:${day}`
      const dailyRevenue = revenueByKey.get(key) ?? 0
      running += dailyRevenue
      const acquiredSize = Number(sizeByCohort.get(cohort.cohort_id)?.size ?? 0)
      const retained = retainedByKey.get(key) ?? 0

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
    }

    return {
      cohort_id: cohort.cohort_id,
      cohort_name: cohort.cohort_name,
      size: cohort.size,
      numericValues,
      displayValues,
    }
  })
}
