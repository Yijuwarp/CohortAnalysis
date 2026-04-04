import { useState, useMemo, useEffect, useCallback, useRef } from 'react'
import { runImpactAnalysis, listEvents, runImpactStats } from '../api'
import SearchableSelect from './SearchableSelect'
import EventFilterChip from './EventFilterChip'

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
  const [expandedEventName, setExpandedEventName] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [events, setEvents] = useState([])
  const [sortMetric, setSortMetric] = useState(null)

  // Stats State (from parent shared state)
  const statsData = state?.statsData || null
  const statsLoading = state?.statsLoading || false
  const statsError = state?.statsError || false
  const statsAbortRef = useRef(null)

  // Config State (from persisted state or defaults)
  const config = useMemo(() => {
    const visibleCohorts = cohorts.filter(c => !c.hidden)
    let defaultBaselineId = null
    
    if (visibleCohorts.length > 0) {
      const match = visibleCohorts.find(c => {
        const name = (c.cohort_name || '').toLowerCase()
        return name.includes('control') || name.includes('baseline')
      })
      defaultBaselineId = String(match ? match.cohort_id : visibleCohorts[0].cohort_id)
    }

    const defaults = {
      experimentType: 'UI Change',
      baseline_cohort_id: defaultBaselineId,
      start_day: 0,
      end_day: globalMaxDay,
      exposure_events: [],
      interaction_events: [],
      impact_events: []
    }
    return state?.config ? { ...defaults, ...state.config } : defaults
  }, [state?.config, cohorts, globalMaxDay])

  const results = state?.results || null

  const updateConfig = (updater) => {
    setState(prev => {
      const currentConfig = prev?.config || config
      const updates = typeof updater === 'function' ? updater(currentConfig) : updater
      return {
        ...prev,
        config: { ...currentConfig, ...updates }
      }
    })
  }

  useEffect(() => {
    listEvents().then(data => setEvents(data.events || [])).catch(() => {})
    
    // Auto-rerun analysis if we have already run it and a global refresh is triggered
    if (refreshToken > 0 && results) {
      handleRun()
    }
  }, [refreshToken])

  const handleRun = async () => {
    // Cancel previous stats request if any
    if (statsAbortRef.current) {
      statsAbortRef.current.abort()
    }

    setLoading(true)
    setError('')
    
    // Reset stats in parent state using functional update to avoid stale config loss
    setState(prev => ({ 
      ...prev, 
      statsData: null, 
      statsLoading: false, 
      statsError: false 
    }))

    try {
      const variant_cohort_ids = cohorts
        .filter(c => Number(c.cohort_id) !== Number(config.baseline_cohort_id) && !c.hidden)
        .map(c => c.cohort_id)

      // Pre-process events to remove incomplete filters
      const sanitizeEvents = (list) => list.map(ev => ({
        event_name: ev.event_name,
        filters: (ev.filters || []).filter(f => f.property && f.value)
      }))

      const payload = {
        baseline_cohort_id: config.baseline_cohort_id,
        variant_cohort_ids,
        start_day: config.start_day,
        end_day: config.end_day,
        exposure_events: sanitizeEvents(config.exposure_events),
        interaction_events: sanitizeEvents(config.interaction_events),
        impact_events: sanitizeEvents(config.impact_events)
      }

      const data = await runImpactAnalysis(payload)
      
      // Update results and stats loading state in one go
      setState(prev => ({ 
        ...prev, 
        results: data, 
        statsLoading: !!data.run_id 
      }))
      
      // Phase 2: Lazy fetch stats
      if (data.run_id) {
        const controller = new AbortController()
        statsAbortRef.current = controller

        try {
          const statsRes = await runImpactStats({ run_id: data.run_id }, controller.signal)
          setState(prev => ({ ...prev, statsData: statsRes.stats, statsLoading: false }))
        } catch (err) {
          if (err.name !== 'AbortError') {
            console.error('Stats fetch failed:', err)
            setState(prev => ({ ...prev, statsError: true, statsLoading: false }))
          }
        } finally {
          if (statsAbortRef.current === controller) {
            statsAbortRef.current = null
          }
        }
      }
    } catch (err) {
      console.error(err)
      setError(err.response?.data?.detail || 'Failed to run analysis. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  const getSignificanceClass = (metricKey, cohortId, delta) => {
    if (!statsData) return '' // Black while loading or no stats
    const stat = statsData[metricKey]?.[String(cohortId)]
    if (!stat || stat.p_value === null || stat.p_value >= 0.05) return 'sig-neutral'
    return delta > 0 ? 'sig-positive' : delta < 0 ? 'sig-negative' : 'sig-neutral'
  }

  const getStatTooltip = (metricKey, cohortId) => {
    if (statsLoading) return 'Calculating statistical significance...'
    if (statsError) return 'Failed to compute statistical significance'
    if (!statsData) return 'No statistical data available'
    
    const stat = statsData[metricKey]?.[String(cohortId)]
    if (!stat) return 'No statistical data available'
    
    if (stat.skip_reason === 'no_difference') return 'No statistical test run (no difference)'
    if (stat.skip_reason === 'low_sample') return 'Sample size too low for significance test'
    if (stat.skip_reason === 'insufficient_data') return 'Insufficient data for test'
    if (stat.p_value === null) return 'Insufficient data for test'
    
    const pValueStr = stat.p_value < 0.001 ? 'p < 0.001' : `p-value: ${stat.p_value.toFixed(3)}`
    const sampledNote = stat.sampled ? ' | sampled users' : ''
    
    return `${stat.test_label} | ${pValueStr}${sampledNote}`
  }

  const renderTags = (list, type) => (
    <div className="impact-tag-container">
      {list.map((ev, idx) => (
        <EventFilterChip 
          key={`${ev.event_name}-${idx}`}
          eventConfig={ev}
          updateEvent={(newEv) => {
            updateConfig(curr => {
              const listArr = curr[type] || []
              const newList = [...listArr]
              const index = listArr.findIndex(e => e.event_name === ev.event_name) // Match by name since filters might change
              if (index !== -1) {
                newList[index] = newEv
                return { [type]: newList }
              }
              return {}
            })
          }}
          removeEvent={() => {
            updateConfig(curr => ({ 
              [type]: (curr[type] || []).filter(e => e.event_name !== ev.event_name) 
            }))
          }}
          isExpanded={expandedEventName === ev.event_name}
          setExpanded={setExpandedEventName}
        />
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
              options={cohorts.filter(c => !c.hidden).map(c => ({ label: c.cohort_name, value: String(c.cohort_id) }))}
              value={String(config.baseline_cohort_id || '')}
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

        <div className="impact-events-section-title">
          <h4>Event Configuration</h4>
        </div>
        <div className="impact-events-box">
          <div className="impact-events-config">
            <div className="event-select-group">
              <label>Exposure Events</label>
              <SearchableSelect
                options={events.filter(e => !config.exposure_events.find(ev => ev.event_name === e))}
                value=""
                onChange={(val) => val && updateConfig(curr => ({ exposure_events: [...(curr.exposure_events || []), { event_name: val, filters: [] }] }))}
                placeholder="Select exposure events..."
              />
              {renderTags(config.exposure_events, 'exposure_events')}
            </div>

            <div className="event-select-group">
              <label>Interaction Events</label>
              <SearchableSelect
                options={events.filter(e => !config.interaction_events.find(ev => ev.event_name === e))}
                value=""
                onChange={(val) => val && updateConfig(curr => ({ interaction_events: [...(curr.interaction_events || []), { event_name: val, filters: [] }] }))}
                placeholder="Select interaction events..."
              />
              {renderTags(config.interaction_events, 'interaction_events')}
            </div>

            <div className="event-select-group">
              <label>Impact Events (Optional)</label>
              <SearchableSelect
                options={events.filter(e => !config.impact_events.find(ev => ev.event_name === e))}
                value=""
                onChange={(val) => val && updateConfig(curr => ({ impact_events: [...(curr.impact_events || []), { event_name: val, filters: [] }] }))}
                placeholder="Select impact events..."
              />
              {renderTags(config.impact_events, 'impact_events')}
            </div>
          </div>
          <div className="impact-disclaimer">
            Filters apply only to the selected event
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
                <tr key={metricRow.metric} className="impact-metric-row">
                  <td 
                    className="metric-cell" 
                    title={getTooltip(metricRow.metric)}
                    onClick={() => setSortMetric(sortMetric === metricRow.metric ? null : metricRow.metric)}
                    style={{ cursor: 'pointer' }}
                  >
                    <span className={`sort-indicator ${sortMetric === metricRow.metric ? 'active' : ''}`}>
                      {sortMetric === metricRow.metric ? '●' : '○'}
                    </span>
                    {metricRow.metric}
                  </td>
                  {sortedResults.cohorts.map((cohort, i) => {
                    const data = metricRow.values[String(cohort.id)]
                    const sigClass = i > 0 ? getSignificanceClass(metricRow.metric_key, cohort.id, data.delta) : ''
                    const tooltip = i > 0 ? getStatTooltip(metricRow.metric_key, cohort.id) : ''
                    
                    return (
                      <td key={cohort.id}>
                        {cohort.size === 0 ? '—' : (
                          <>
                            {formatValue(data.value, metricRow.metric)}
                            {i > 0 && (
                              <span 
                                className={`delta-val ${sigClass}`}
                                title={tooltip}
                              >
                                {formatDelta(data.delta)}
                              </span>
                            )}
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
                    <tr key={metricRow.metric} className="impact-metric-row">
                      <td 
                        className="metric-cell" 
                        title={getTooltip(metricRow.metric)}
                        onClick={() => setSortMetric(sortMetric === metricRow.metric ? null : metricRow.metric)}
                        style={{ cursor: 'pointer' }}
                      >
                        <span className={`sort-indicator ${sortMetric === metricRow.metric ? 'active' : ''}`}>
                          {sortMetric === metricRow.metric ? '●' : '○'}
                        </span>
                        {metricRow.metric}
                      </td>
                      {sortedResults.cohorts.map((cohort, i) => {
                        const data = metricRow.values[String(cohort.id)]
                        const sigClass = i > 0 ? getSignificanceClass(metricRow.metric_key, cohort.id, data.delta) : ''
                        const tooltip = i > 0 ? getStatTooltip(metricRow.metric_key, cohort.id) : ''
                        
                        return (
                          <td key={cohort.id}>
                            {cohort.size === 0 ? '—' : (
                              <>
                                {formatValue(data.value, metricRow.metric)}
                                {i > 0 && (
                                  <span 
                                    className={`delta-val ${sigClass}`}
                                    title={tooltip}
                                  >
                                    {formatDelta(data.delta)}
                                  </span>
                                )}
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
