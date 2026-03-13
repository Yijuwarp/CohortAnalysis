import { describe, test, expect } from "vitest"
import { buildMonetizationRows } from "../src/monetization.js"

describe("buildMonetizationRows", () => {

  test("computes cumulative revenue per acquired user", () => {
    const rows = buildMonetizationRows({
      cohortSizes: [
        { cohort_id: 1, cohort_name: "All Users", size: 4 }
      ],
      retainedRows: [
        { cohort_id: 1, day_number: 0, retained_users: 2 },
        { cohort_id: 1, day_number: 1, retained_users: 1 }
      ],
      revenueRows: [
        { cohort_id: 1, day_number: 0, revenue: 8 },
        { cohort_id: 1, day_number: 1, revenue: 4 }
      ],
      dayColumns: [0, 1],
      metricType: "cumulative_revenue_per_acquired_user"
    })

    expect(rows.length).toBe(1)

    const row = rows[0]

    expect(row.numericValues["0"]).toBe(2) // 8 / 4
    expect(row.numericValues["1"]).toBe(3) // (8+4) / 4
  })


  test("handles divide-by-zero retained users", () => {
    const rows = buildMonetizationRows({
      cohortSizes: [
        { cohort_id: 1, cohort_name: "All Users", size: 4 }
      ],
      retainedRows: [
        { cohort_id: 1, day_number: 0, retained_users: 0 }
      ],
      revenueRows: [
        { cohort_id: 1, day_number: 0, revenue: 5 }
      ],
      dayColumns: [0],
      metricType: "revenue_per_retained_user"
    })

    expect(rows.length).toBe(1)

    const row = rows[0]

    expect(row.numericValues["0"]).toBe(null)
  })

})