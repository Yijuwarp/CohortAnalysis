import test from 'node:test'
import assert from 'node:assert/strict'
import { buildMonetizationRows } from '../src/monetization.js'

const identityCurrency = (value) => Number(value).toFixed(2)

test('buildMonetizationRows computes cumulative and denominator metrics', () => {
  const rows = buildMonetizationRows({
    cohortSizes: [{ cohort_id: 1, cohort_name: 'All Users', size: 4 }],
    retainedRows: [
      { cohort_id: 1, day_number: 0, retained_users: 2 },
      { cohort_id: 1, day_number: 1, retained_users: 1 },
    ],
    revenueRows: [
      { cohort_id: 1, day_number: 0, revenue: 8 },
      { cohort_id: 1, day_number: 1, revenue: 4 },
    ],
    dayColumns: [0, 1],
    metricType: 'cumulative_revenue_per_acquired_user',
    formatCurrency: identityCurrency,
  })

  assert.equal(rows[0].values['0'], '2.00')
  assert.equal(rows[0].values['1'], '3.00')

  const perRetained = buildMonetizationRows({
    cohortSizes: [{ cohort_id: 1, cohort_name: 'All Users', size: 4 }],
    retainedRows: [{ cohort_id: 1, day_number: 0, retained_users: 0 }],
    revenueRows: [{ cohort_id: 1, day_number: 0, revenue: 5 }],
    dayColumns: [0],
    metricType: 'revenue_per_retained_user',
    formatCurrency: identityCurrency,
  })

  assert.equal(perRetained[0].values['0'], '-')
})
