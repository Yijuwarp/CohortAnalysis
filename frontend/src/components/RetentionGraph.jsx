import React from 'react'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from 'recharts'
import { getCohortColor } from '../utils/cohortColors'

export default function RetentionGraph({ data, maxDay, includeCI }) {
  if (!data || data.length === 0) return null

  const days = Array.from({ length: maxDay + 1 }, (_, i) => i)

  const chartData = days.map((day) => {
    const row = { day: `D${day}` }

    data.forEach((cohort) => {
      const retentionValue = cohort.retention?.[String(day)]
      row[`cohort_${cohort.cohort_id}`] = retentionValue ?? null

      if (includeCI && cohort.retention_ci) {
        row[`cohort_${cohort.cohort_id}_lower`] = cohort.retention_ci?.[String(day)]?.lower ?? null
        row[`cohort_${cohort.cohort_id}_upper`] = cohort.retention_ci?.[String(day)]?.upper ?? null
      }
    })

    return row
  })

  return (
    <div className="retention-graph-container">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="day" />
          <YAxis domain={[0, 100]} tickFormatter={(value) => `${value}%`} />
          <Tooltip formatter={(value) => (value !== null ? `${Number(value).toFixed(2)}%` : '—')} />
          <Legend />

          {data.map((cohort, index) => (
            <Line
              key={cohort.cohort_id}
              type="monotone"
              dataKey={`cohort_${cohort.cohort_id}`}
              name={cohort.cohort_name}
              stroke={getCohortColor(cohort.cohort_id, index)}
              strokeWidth={2}
              dot={{ r: 3 }}
              isAnimationActive
            />
          ))}

          {includeCI &&
            data.map((cohort, index) => (
              <React.Fragment key={`${cohort.cohort_id}-ci`}>
                <Line
                  type="monotone"
                  dataKey={`cohort_${cohort.cohort_id}_upper`}
                  name={`${cohort.cohort_name} UB`}
                  stroke={getCohortColor(cohort.cohort_id, index)}
                  strokeDasharray="5 5"
                  strokeWidth={1}
                  legendType="none"
                  dot={false}
                />
                <Line
                  type="monotone"
                  dataKey={`cohort_${cohort.cohort_id}_lower`}
                  name={`${cohort.cohort_name} LB`}
                  stroke={getCohortColor(cohort.cohort_id, index)}
                  strokeDasharray="5 5"
                  strokeWidth={1}
                  legendType="none"
                  dot={false}
                />
              </React.Fragment>
            ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
