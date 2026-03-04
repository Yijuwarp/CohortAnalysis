export function buildMonetizationRows({ cohortSizes, retainedRows, revenueRows, dayColumns, metricType, formatCurrency }) {
  const sizeByCohort = new Map(cohortSizes.map((row) => [row.cohort_id, row]))
  const retainedByKey = new Map(retainedRows.map((row) => [`${row.cohort_id}:${row.day_number}`, Number(row.retained_users)]))
  const revenueByKey = new Map(revenueRows.map((row) => [`${row.cohort_id}:${row.day_number}`, Number(row.revenue)]))

  return cohortSizes.map((cohort) => {
    const values = {}
    let running = 0
    for (const day of dayColumns) {
      const key = `${cohort.cohort_id}:${day}`
      const dailyRevenue = revenueByKey.get(key) ?? 0
      running += dailyRevenue
      const acquiredSize = Number(sizeByCohort.get(cohort.cohort_id)?.size ?? 0)
      const retained = retainedByKey.get(key) ?? 0

      if (metricType === 'total_revenue') {
        values[String(day)] = formatCurrency(dailyRevenue)
      } else if (metricType === 'cumulative_revenue') {
        values[String(day)] = formatCurrency(running)
      } else if (metricType === 'revenue_per_acquired_user') {
        values[String(day)] = acquiredSize > 0 ? formatCurrency(dailyRevenue / acquiredSize) : '-'
      } else if (metricType === 'cumulative_revenue_per_acquired_user') {
        values[String(day)] = acquiredSize > 0 ? formatCurrency(running / acquiredSize) : '-'
      } else {
        values[String(day)] = retained > 0 ? formatCurrency(dailyRevenue / retained) : '-'
      }
    }

    return {
      cohort_id: cohort.cohort_id,
      cohort_name: cohort.cohort_name,
      size: cohort.size,
      values,
    }
  })
}
