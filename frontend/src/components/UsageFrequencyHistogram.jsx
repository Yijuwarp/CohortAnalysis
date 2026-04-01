import React, { useEffect, useState, useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { getUsageFrequency } from '../api'

const COLORS = [
  '#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#d0ed57', '#a4de6c',
  '#f15c80', '#e4d354', '#2b908f', '#f45b5b', '#91e8e1', '#7cb5ec',
]

function formatCohortSize(size) {
  const numericSize = Number(size) || 0
  if (numericSize >= 1000) {
    const valueInThousands = numericSize / 1000
    const fixed = Number.isInteger(valueInThousands) ? valueInThousands.toFixed(0) : valueInThousands.toFixed(1)
    return `${fixed}K`
  }

  return numericSize.toLocaleString()
}

function formatPercentFromRatio(ratio) {
  return `${((Number(ratio) || 0) * 100).toFixed(2)}%`
}

function formatUserCount(count) {
  return (Number(count) || 0).toLocaleString()
}

export default function UsageFrequencyHistogram({ event, refreshToken, propertyFilter = null, prefetchedData = null, loadingState = null }) {
  const [localData, setLocalData] = useState(null)
  const [localLoading, setLocalLoading] = useState(false)
  const [error, setError] = useState('')

  const data = prefetchedData || localData
  const loading = loadingState !== null ? loadingState : localLoading

  useEffect(() => {
    if (prefetchedData || !event) {
      if (!event) setLocalData(null)
      return
    }

    let isMounted = true
    setLocalLoading(true)
    setError('')

    getUsageFrequency(event, propertyFilter)
      .then((res) => {
        if (!isMounted) return
        setLocalData(res)
      })
      .catch((err) => {
        if (!isMounted) return
        setError(err.message)
      })
      .finally(() => {
        if (isMounted) {
          setLocalLoading(false)
        }
      })

    return () => {
      isMounted = false
    }
  }, [event, propertyFilter?.operator, propertyFilter?.property, propertyFilter?.value, refreshToken, prefetchedData])

  const cohortMeta = useMemo(() => {
    if (!data || !data.cohort_sizes) return []

    return data.cohort_sizes.map((c) => {
      const size = Number(c.size) || 0
      const name = c.name || `Cohort ${c.cohort_id}`
      return {
        id: c.cohort_id,
        key: `cohort_${c.cohort_id}`,
        name,
        size,
        label: `${name} (${formatCohortSize(size)})`,
      }
    })
  }, [data])

  const chartData = useMemo(() => {
    if (!data || !data.buckets || data.buckets.length === 0) return []

    return data.buckets.map((bucketObj) => {
      const row = { name: bucketObj.bucket }
      const bucketByCohortId = new Map(bucketObj.cohorts.map((cohort) => [cohort.cohort_id, Number(cohort.users) || 0]))

      cohortMeta.forEach((cohort) => {
        const users = bucketByCohortId.get(cohort.id) || 0
        const percentage = cohort.size > 0 ? users / cohort.size : 0
        row[cohort.key] = Math.min(percentage, 1)
        row[`${cohort.key}__users`] = users
      })

      return row
    })
  }, [cohortMeta, data])

  if (!event) return null

  return (
    <div style={{ marginTop: '2rem' }}>
      <h3>Event Frequency Distribution</h3>
      {loading && <p>Loading frequency data...</p>}
      {error && <p className="error">{error}</p>}
      {!loading && !error && data && data.buckets && data.buckets.length > 0 && (
        <>
          <div style={{ width: '100%', height: 400, marginBottom: '2rem' }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis tickFormatter={formatPercentFromRatio} />
                <Tooltip
                  formatter={(value, _name, item) => {
                    const users = item && item.payload
                      ? item.payload[`${item.dataKey}__users`] ?? 0
                      : 0
                    return [`${formatPercentFromRatio(value)} (${formatUserCount(users)} users)`, item?.name || '']
                  }}
                />
                <Legend />
                {cohortMeta.map((c, index) => (
                  <Bar
                    key={c.id}
                    dataKey={c.key}
                    fill={COLORS[index % COLORS.length]}
                    name={c.label}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="analytics-table table-responsive">
            <table>
              <thead>
                <tr>
                  <th>Bucket</th>
                  {cohortMeta.map((c) => (
                    <th key={c.id}>{c.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.buckets.map((bucketObj) => {
                  const bucketByCohortId = new Map(bucketObj.cohorts.map((cohort) => [cohort.cohort_id, Number(cohort.users) || 0]))

                  return (
                    <tr key={bucketObj.bucket}>
                      <td>{bucketObj.bucket}</td>
                      {cohortMeta.map((cMeta) => {
                        const usersCount = bucketByCohortId.get(cMeta.id) || 0
                        const percentage = cMeta.size > 0 ? usersCount / cMeta.size : 0

                        return (
                          <td key={cMeta.id}>
                            {cMeta.size === 0 ? (
                              '—'
                            ) : (
                              <>
                                <div>{formatPercentFromRatio(percentage)}</div>
                                <small style={{ color: '#888' }}>({formatUserCount(usersCount)})</small>
                              </>
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
