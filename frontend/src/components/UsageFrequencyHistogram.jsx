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

export default function UsageFrequencyHistogram({ event, refreshToken }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!event) {
      setData(null)
      return
    }

    let isMounted = true
    setLoading(true)
    setError('')

    getUsageFrequency(event)
      .then((res) => {
        if (!isMounted) return
        setData(res)
      })
      .catch((err) => {
        if (!isMounted) return
        setError(err.message)
      })
      .finally(() => {
        if (isMounted) {
          setLoading(false)
        }
      })

    return () => {
      isMounted = false
    }
  }, [event, refreshToken])

  const chartData = useMemo(() => {
    if (!data || !data.buckets || data.buckets.length === 0) return []

    return data.buckets.map((bucketObj) => {
      const row = { name: bucketObj.bucket }
      let totalAllUsers = 0
      bucketObj.cohorts.forEach((c) => {
        const c_size_obj = data.cohort_sizes.find((cs) => cs.cohort_id === c.cohort_id)
        const cohortName = c_size_obj?.name || `Cohort ${c.cohort_id}`
        row[cohortName] = c.users
        totalAllUsers += c.users
      })
      row['All Users'] = totalAllUsers
      return row
    })
  }, [data])

  const cohortMeta = useMemo(() => {
    if (!data || !data.cohort_sizes) return []
    return data.cohort_sizes.map((c) => ({
      id: c.cohort_id,
      name: c.name || `Cohort ${c.cohort_id}`,
      size: c.size,
    }))
  }, [data])

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
                <YAxis />
                <Tooltip />
                <Legend />
                {cohortMeta.map((c, index) => (
                  <Bar
                    key={c.id}
                    dataKey={c.name}
                    fill={COLORS[index % COLORS.length]}
                    name={c.name}
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
                  <th>All Users</th>
                  {cohortMeta.map((c) => (
                    <th key={c.id}>{c.name}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.buckets.map((bucketObj) => {
                  let totalUsers = 0
                  bucketObj.cohorts.forEach(c => totalUsers += c.users)

                  return (
                    <tr key={bucketObj.bucket}>
                      <td>{bucketObj.bucket}</td>
                      <td>{totalUsers}</td>
                      {cohortMeta.map((cMeta) => {
                        const cohortInfo = bucketObj.cohorts.find((c) => c.cohort_id === cMeta.id)
                        const usersCount = cohortInfo ? cohortInfo.users : 0
                        const percentage = cMeta.size > 0 ? ((usersCount / cMeta.size) * 100).toFixed(2) : '0.00'
                        return (
                          <td key={cMeta.id}>
                            {usersCount} <small style={{ color: '#888' }}>({percentage}%)</small>
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
