import React from 'react'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  Area,
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
  predictions,
  predictionHorizon,
  effectiveMaxDay,
}) {
  if (!rows || rows.length === 0) return null

  const hasPredictions = Boolean(predictions) && Object.keys(predictions).length > 0
  const maxProjectionDay = hasPredictions
    ? Math.max(effectiveMaxDay, predictionHorizon)
    : maxDay
  const days = Array.from({ length: maxProjectionDay + 1 }, (_, i) => i)

  const chartData = days.map((day) => {
    const row = { day: `D${day}` }

    rows.forEach((cohort) => {
      const actualValue = Number(cohort.numericValues?.[String(day)])
      const cohortPrediction = predictions?.[cohort.cohort_id]
      const predictionStartDay = cohortPrediction?.lastObservedDay ?? effectiveMaxDay

      const projectedValue = Number(cohortPrediction?.projectedCurve?.[day])
      const upper = Number(cohortPrediction?.upperCI?.[day])
      const lower = Number(cohortPrediction?.lowerCI?.[day])

      row[`cohort_${cohort.cohort_id}_actual`] = day <= effectiveMaxDay && Number.isFinite(actualValue) ? actualValue : null
      row[`cohort_${cohort.cohort_id}_projection`] = day > predictionStartDay && Number.isFinite(projectedValue) ? projectedValue : null
      row[`cohort_${cohort.cohort_id}_upper`] = day > predictionStartDay && Number.isFinite(upper) ? upper : null
      row[`cohort_${cohort.cohort_id}_lower`] = day > predictionStartDay && Number.isFinite(lower) ? lower : null
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

          {rows.map((cohort, index) => {
            const cohortColor = getCohortColor(cohort.cohort_id, index)
            return (
              <React.Fragment key={cohort.cohort_id}>
                <Area
                  type="linear"
                  dataKey={`cohort_${cohort.cohort_id}_upper`}
                  baseLine={`cohort_${cohort.cohort_id}_lower`}
                  fill={cohortColor}
                  fillOpacity={0.12}
                  stroke="none"
                  isAnimationActive
                />
                <Line
                  type="linear"
                  dataKey={`cohort_${cohort.cohort_id}_actual`}
                  name={cohort.cohort_name}
                  stroke={cohortColor}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  isAnimationActive
                />
                <Line
                  type="linear"
                  dataKey={`cohort_${cohort.cohort_id}_projection`}
                  name={`${cohort.cohort_name} Projection`}
                  stroke={cohortColor}
                  strokeDasharray="5 5"
                  strokeWidth={2}
                  dot={false}
                  isAnimationActive
                  legendType="none"
                />
              </React.Fragment>
            )
          })}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
