import { useState, useMemo, useEffect, useCallback } from 'react'
import { runImpactAnalysis, listEvents } from '../api'
import SearchableSelect from './SearchableSelect'

const TOOLTIPS = {
  'CTR': 'Users who interacted / Users exposed',
  'Engagement': 'Total interaction events / total users',
  'Reach': 'Users who triggered event / total users',
  'Intensity': 'Event count / total users'
}

const getTooltip = (metric) => {
  if (metric === 'CTR') return TOOLTIPS.CTR
  if (metric === 'Engagement') return TOOLTIPS.Engagement
  if (metric.includes('Reach')) return TOOLTIPS.Reach
  if (metric.includes('Intensity')) return TOOLTIPS.Intensity
  return ''
}

const formatValue = (val, metric) => {
  if (val === null || val === undefined) return '—'
  if (metric === 'Engagement' || metric.includes('Intensity')) {
    return val.toFixed(2)
  }
  return (val * 100).toFixed(1) + '%'
}

const formatDelta = (delta) => {
  if (delta === null || delta === undefined) return '(—)'
  const arrow = delta >= 0 ? '↑' : '↓'
  return `(${arrow} ${Math.abs(delta * 100).toFixed(1)}%)`
}

export default function ExperimentImpactPane({ 
  refreshToken, 
  cohorts, 
  globalMaxDay, 
  appliedFilters, 
  state, 
  setState 
}) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [events, setEvents] = useState([])
  const [sortMetric, setSortMetric] = useState(null)

  // Config State (from persisted state or defaults)
  const config = useMemo(() => state?.config || {
    experimentType: 'UI Change',
    baseline_cohort_id: cohorts.length > 0 ? cohorts[0].cohort_id : null,
    start_day: 0,
    end_day: globalMaxDay,
    exposure_events: [],
    interaction_events: [],
    impact_events: []
  }, [state?.config, cohorts, globalMaxDay])

  const results = state?.results || null

  const updateConfig = (newConfig) => {
    setState({ ...state, config: { ...config, ...newConfig } })
  }

  useEffect(() => {
    listEvents().then(data => setEvents(data.events || [])).catch(() => {})
    
    // Auto-rerun analysis if we have already run it and a global refresh is triggered
    if (refreshToken > 0 && results) {
      handleRun()
    }
  }, [refreshToken])

  const handleRun = async () => {
    setLoading(true)
    setError('')
    try {
      const variant_cohort_ids = cohorts
        .filter(c => Number(c.cohort_id) !== Number(config.baseline_cohort_id) && !c.hidden)
        .map(c => c.cohort_id)

      const payload = {
        baseline_cohort_id: config.baseline_cohort_id,
        variant_cohort_ids,
        start_day: config.start_day,
        end_day: config.end_day,
        exposure_events: config.exposure_events,
        interaction_events: config.interaction_events,
        impact_events: config.impact_events
      }

      const data = await runImpactAnalysis(payload)
      setState({ ...state, results: data })
    } catch (err) {
      setError('Failed to run analysis. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  const renderTags = (list, type) => (
    <div className="impact-tag-container">
      {list.map(event => (
        <span key={event} className="impact-tag">
          {event}
          <button onClick={() => {
            updateConfig({ [type]: list.filter(e => e !== event) })
          }}>×</button>
        </span>
      ))}
    </div>
  )

  const sortedResults = useMemo(() => {
    if (!results || !sortMetric) return results
    
    const metricData = results.metrics.find(m => m.metric === sortMetric)
    if (!metricData) return results

    const baselineId = String(results.cohorts[0].id)
    const variants = [...results.cohorts.slice(1)]
    
    variants.sort((a, b) => {
      const deltaA = metricData.values[String(a.id)]?.delta ?? -Infinity
      const deltaB = metricData.values[String(b.id)]?.delta ?? -Infinity
      return deltaB - deltaA
    })

    return { ...results, cohorts: [results.cohorts[0], ...variants] }
  }, [results, sortMetric])

  return (
    <div className="experiment-impact-pane">
      <div className="impact-config-card card">
        <h3>Experiment Configuration</h3>
        <div className="impact-config-grid">
          <div className="config-field">
            <label>Experiment Type</label>
            <select value={config.experimentType} disabled>
              <option>UI Change</option>
            </select>
          </div>
          
          <div className="config-field">
            <label>Baseline Cohort</label>
            <SearchableSelect
              options={cohorts.filter(c => !c.hidden).map(c => ({ label: c.cohort_name, value: c.cohort_id }))}
              value={config.baseline_cohort_id}
              onChange={(val) => updateConfig({ baseline_cohort_id: val })}
            />
          </div>

          <div className="config-field">
            <label>Time Window (Days)</label>
            <div className="time-window-inputs">
              <input 
                type="number" 
                value={config.start_day} 
                onChange={(e) => updateConfig({ start_day: parseInt(e.target.value) || 0 })}
              />
              <span>→</span>
              <input 
                type="number" 
                value={config.end_day} 
                onChange={(e) => updateConfig({ end_day: parseInt(e.target.value) || 0 })}
              />
            </div>
          </div>
        </div>

        <div className="impact-events-config">
          <div className="event-select-group">
            <label>Exposure Events</label>
            <SearchableSelect
              options={events.filter(e => !config.exposure_events.includes(e))}
              value=""
              onChange={(val) => val && updateConfig({ exposure_events: [...config.exposure_events, val] })}
              placeholder="Select exposure events..."
            />
            {renderTags(config.exposure_events, 'exposure_events')}
          </div>

          <div className="event-select-group">
            <label>Interaction Events</label>
            <SearchableSelect
              options={events.filter(e => !config.interaction_events.includes(e))}
              value=""
              onChange={(val) => val && updateConfig({ interaction_events: [...config.interaction_events, val] })}
              placeholder="Select interaction events..."
            />
            {renderTags(config.interaction_events, 'interaction_events')}
          </div>

          <div className="event-select-group">
            <label>Impact Events (Optional)</label>
            <SearchableSelect
              options={events.filter(e => !config.impact_events.includes(e))}
              value=""
              onChange={(val) => val && updateConfig({ impact_events: [...config.impact_events, val] })}
              placeholder="Select impact events..."
            />
            {renderTags(config.impact_events, 'impact_events')}
          </div>
        </div>

        <button 
          className="button button-primary impact-run-btn" 
          onClick={handleRun} 
          disabled={loading || !config.baseline_cohort_id || config.exposure_events.length === 0 || config.interaction_events.length === 0}
        >
          {loading ? 'Running Analysis...' : 'Run Analysis'}
        </button>
      </div>

      {error && (
        <div className="impact-error card">
          <p>{error}</p>
          <button className="button" onClick={handleRun}>Retry</button>
        </div>
      )}

      {sortedResults && (
        <div className="impact-results-card card">
          <table className="impact-table">
            <thead>
              <tr>
                <th className="metric-col">Metric</th>
                {sortedResults.cohorts.map((cohort, i) => (
                  <th key={cohort.id} className={i === 0 ? 'baseline-col' : ''}>
                    {i === 0 ? 'Control' : 'Variant'} ({cohort.size.toLocaleString()})
                    <div className="cohort-name">{cohort.name}</div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              <tr className="impact-section-header">
                <td colSpan={sortedResults.cohorts.length + 1}>Exposure & Interaction</td>
              </tr>
              {sortedResults.metrics.slice(0, 3).map(metricRow => (
                <tr key={metricRow.metric}>
                  <td className="metric-cell" title={getTooltip(metricRow.metric)}>
                    <span 
                      className={`sort-indicator ${sortMetric === metricRow.metric ? 'active' : ''}`}
                      onClick={() => setSortMetric(sortMetric === metricRow.metric ? null : metricRow.metric)}
                    >
                      {sortMetric === metricRow.metric ? '●' : '○'}
                    </span>
                    {metricRow.metric}
                  </td>
                  {sortedResults.cohorts.map((cohort, i) => {
                    const data = metricRow.values[String(cohort.id)]
                    return (
                      <td key={cohort.id}>
                        {cohort.size === 0 ? '—' : (
                          <>
                            {formatValue(data.value, metricRow.metric)}
                            {i > 0 && <span className="delta-val">{formatDelta(data.delta)}</span>}
                          </>
                        )}
                      </td>
                    )
                  })}
                </tr>
              ))}

              {sortedResults.metrics.length > 3 && (
                <>
                  <tr className="impact-section-header">
                    <td colSpan={sortedResults.cohorts.length + 1}>Impact</td>
                  </tr>
                  {sortedResults.metrics.slice(3).map(metricRow => (
                    <tr key={metricRow.metric}>
                      <td className="metric-cell" title={getTooltip(metricRow.metric)}>
                        <span 
                          className={`sort-indicator ${sortMetric === metricRow.metric ? 'active' : ''}`}
                          onClick={() => setSortMetric(sortMetric === metricRow.metric ? null : metricRow.metric)}
                        >
                          {sortMetric === metricRow.metric ? '●' : '○'}
                        </span>
                        {metricRow.metric}
                      </td>
                      {sortedResults.cohorts.map((cohort, i) => {
                        const data = metricRow.values[String(cohort.id)]
                        return (
                          <td key={cohort.id}>
                            {cohort.size === 0 ? '—' : (
                              <>
                                {formatValue(data.value, metricRow.metric)}
                                {i > 0 && <span className="delta-val">{formatDelta(data.delta)}</span>}
                              </>
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
