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
import { formatCurrency } from '../utils/formatters'

export default function MonetizationGraph({
  rows,
  maxDay,
  metricType,
}) {
  if (!rows || rows.length === 0) return null

  const days = Array.from({ length: maxDay + 1 }, (_, i) => i)

  const chartData = days.map((day) => {
    const row = { day: `D${day}` }

    rows.forEach((cohort) => {
      const value = cohort.numericValues?.[String(day)]
      row[`cohort_${cohort.cohort_id}`] = typeof value === 'number' ? value : null
    })

    return row
  })

  return (
    <div className="monetization-graph-container">
      <ResponsiveContainer width="100%" height="100%">
        {/* Intentional: remount the chart when metric changes so animations restart cleanly. */}
        <LineChart key={metricType} data={chartData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="day" />
          <YAxis domain={[0, 'auto']} tickFormatter={formatCurrency} />
          <Tooltip formatter={(value) => formatCurrency(value)} />
          <Legend />

          {rows.map((cohort, index) => (
            <Line
              key={cohort.cohort_id}
              type="linear"
              dataKey={`cohort_${cohort.cohort_id}`}
              name={cohort.cohort_name}
              stroke={getCohortColor(cohort.cohort_id, index)}
              strokeWidth={2}
              dot={{ r: 3 }}
              isAnimationActive
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
