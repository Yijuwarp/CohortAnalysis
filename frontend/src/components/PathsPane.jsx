import { useState, useEffect, useCallback } from 'react'
import { runPaths, createPathsDropOffCohort, createPathsReachedCohort } from '../api'
import { formatInteger, formatDuration } from '../utils/formatters'
import { getCohortColor } from '../utils/cohortColors'
import SearchableSelect from './SearchableSelect'

export default function PathsPane({ refreshToken, events, state, setState, onRefreshCohorts }) {
  const [steps, setSteps] = useState(state?.steps || ['', ''])
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(state?.result || null)
  const [error, setError] = useState('')
  const [creatingCohort, setCreatingCohort] = useState(null) // { cohortId, stepIdx, type }
  const [showNamingModal, setShowNamingModal] = useState(null) // { cohortId, stepIdx, type, defaultName }
  const [customName, setCustomName] = useState('')

  const safeEvents = (events || []).map(e => ({ value: e, label: e }))

  useEffect(() => {
    setState({ steps, result })
  }, [steps, result, setState])

  const handleAddStep = () => {
    if (steps.length >= 10) return
    setSteps(prev => [...prev, ''])
  }

  const handleRemoveStep = (idx) => {
    if (steps.length <= 2) return
    setSteps(prev => prev.filter((_, i) => i !== idx))
  }

  const handleStepChange = (idx, val) => {
    setSteps(prev => prev.map((s, i) => i === idx ? val : s))
  }

  const handleRun = async () => {
    if (steps.some(s => !s)) {
        setError('All steps must have an event selected.')
        return
    }
    setRunning(true)
    setError('')
    setResult(null)
    try {
      const data = await runPaths(steps)
      setResult(data)
    } catch (err) {
      setError(err.message || 'Failed to run analysis')
    } finally {
      setRunning(false)
    }
  }

  const handleOpenNamingModal = (cohortId, stepIdx, type, eventName, cohortName) => {
    const sequence = steps.slice(0, stepIdx).join(' -> ')
    let defaultName = ''
    if (type === 'reached') {
        defaultName = `Reached Step ${stepIdx} (${eventName}) (${cohortName}): ${sequence}`
    } else {
        if (stepIdx === 1) {
            defaultName = `Did not start (${eventName}) (${cohortName})`
        } else {
            defaultName = `Drop-off at Step ${stepIdx} (${eventName}) (${cohortName}): ${sequence}`
        }
    }
    
    setShowNamingModal({ cohortId, stepIdx, type, defaultName })
    setCustomName(defaultName)
  }

  const handleConfirmCreate = async () => {
    if (!showNamingModal) return
    const { cohortId, stepIdx, type } = showNamingModal
    const nameToUse = customName.trim()
    
    setCreatingCohort(`${cohortId}-${stepIdx}-${type}`)
    setShowNamingModal(null)
    
    try {
      if (type === 'reached') {
        await createPathsReachedCohort(cohortId, stepIdx, steps, nameToUse)
      } else {
        await createPathsDropOffCohort(cohortId, stepIdx, steps, nameToUse)
      }
      
      if (onRefreshCohorts) {
          onRefreshCohorts()
      }
      alert('Cohort created successfully.')
    } catch (err) {
      alert(err.message || 'Failed to create cohort')
    } finally {
      setCreatingCohort(null)
    }
  }


  const isInvalid = steps.some(s => !s) || steps.length < 2

  return (
    <div className="paths-pane" data-testid="paths-pane">
      <section className="card ui-card paths-builder">
        <h3>Sequence Builder</h3>
        <p className="pane-section-hint">Define a strict ordered sequence of events (e.g. A → B → C)</p>
        
        <div className="paths-steps-list">
          {steps.map((step, idx) => (
            <div key={idx} className="paths-step-row">
              <span className="paths-step-number">{idx + 1}</span>
              <div className="paths-step-select">
                <SearchableSelect
                  options={safeEvents}
                  value={step}
                  onChange={(val) => handleStepChange(idx, val)}
                  placeholder="Select event..."
                />
              </div>
              {steps.length > 2 && (
                <button className="paths-remove-step" onClick={() => handleRemoveStep(idx)}>✕</button>
              )}
            </div>
          ))}
        </div>

        <div className="paths-builder-actions">
          <button 
            className="button button-secondary" 
            onClick={handleAddStep} 
            disabled={steps.length >= 10 || running}
          >
            + Add Step
          </button>
          <button 
            className="button button-primary" 
            onClick={handleRun} 
            disabled={isInvalid || running}
          >
            {running ? 'Running Analysis...' : 'Run Paths Analysis'}
          </button>
        </div>
        
        {error && <p className="error">{error}</p>}
      </section>

      {result && (
        <div className="paths-results animate-fade-in">
          {result.global_insights?.length > 0 && (
            <section className="card ui-card paths-global-insights">
              <h4>Global Insights</h4>
              <ul className="paths-insights-list">
                {result.global_insights.map((insight, idx) => (
                  <li key={idx} className="paths-insight-item global">{insight}</li>
                ))}
              </ul>
            </section>
          )}

          <div className="paths-cohort-results">
            {result.results.map((cohort, cohortIdx) => (
              <section key={cohort.cohort_id} className="card ui-card paths-cohort-card">
                <div className="paths-cohort-header">
                  <h4 style={{ color: getCohortColor(cohort.cohort_id, cohortIdx) }}>
                    {cohort.cohort_name}
                  </h4>
                  <span className="paths-cohort-size">{formatInteger(cohort.cohort_size)} users</span>
                </div>

                {cohort.insights?.length > 0 && (
                  <div className="paths-cohort-insights">
                    {cohort.insights.map((insight, idx) => (
                      <div key={idx} className="paths-insight-item cohort">{insight}</div>
                    ))}
                  </div>
                )}

                <div className="table-responsive">
                  <table className="paths-table">
                    <thead>
                      <tr>
                        <th>Step</th>
                        <th>Event</th>
                        <th className="text-right">Users</th>
                        <th className="text-right">Conv %</th>
                        <th className="text-right">Drop-off</th>
                        <th className="text-right">Mean Time</th>
                        <th className="text-right">P20</th>
                        <th className="text-right">P80</th>
                        <th className="text-center">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cohort.steps.map((s, sIdx) => (
                        <tr key={sIdx}>
                          <td>{s.step}</td>
                          <td><span className="paths-event-pill">{s.event}</span></td>
                          <td className="text-right">
                            {formatInteger(s.users)}
                            {s.users < 50 && s.users > 0 && (
                                <div className="low-sample-warn" title="Low sample size - metrics may be unstable">⚠️</div>
                            )}
                          </td>
                          <td className="text-right">{s.conversion_pct}%</td>
                          <td className="text-right">
                            {s.drop_off_pct !== null ? `${s.drop_off_pct}%` : '—'}
                          </td>
                          <td className="text-right">{formatDuration(s.mean_time)}</td>
                          <td className="text-right">{formatDuration(s.p20)}</td>
                          <td className="text-right">{formatDuration(s.p80)}</td>
                          <td className="text-center">
                            <div className="paths-action-dropdown">
                              <button 
                                className="button button-small button-secondary dropdown-trigger"
                                disabled={!!creatingCohort}
                              >
                                {creatingCohort?.startsWith(`${cohort.cohort_id}-${s.step}`) ? 'Creating...' : 'Create Cohort ▼'}
                              </button>
                              <div className="dropdown-menu">
                                <button onClick={() => handleOpenNamingModal(cohort.cohort_id, sIdx + 1, 'reached', s.event, cohort.cohort_name)}>
                                  Reached Step {sIdx + 1}
                                </button>
                                {sIdx === 0 && s.conversion_pct < 100 && (
                                  <button onClick={() => handleOpenNamingModal(cohort.cohort_id, sIdx + 1, 'dropoff', s.event, cohort.cohort_name)}>
                                    Did not start ({s.event})
                                  </button>
                                )}
                                {sIdx > 0 && s.drop_off_pct > 0 && (
                                  <button onClick={() => handleOpenNamingModal(cohort.cohort_id, sIdx + 1, 'dropoff', s.event, cohort.cohort_name)}>
                                    Drop-off after {steps[sIdx-1]}
                                  </button>
                                )}
                              </div>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            ))}
          </div>
        </div>
      )}

      <style>{`
        .paths-pane { display: flex; flex-direction: column; gap: 24px; padding-bottom: 40px; }
        .paths-steps-list { display: flex; flex-direction: column; gap: 12px; margin: 16px 0; max-width: 500px; }
        .paths-step-row { display: flex; align-items: center; gap: 12px; }
        .paths-step-number { 
            width: 24px; height: 24px; border-radius: 12px; 
            background: var(--primary-color); color: white; 
            display: flex; align-items: center; justify-content: center; 
            font-size: 12px; font-weight: bold; flex-shrink: 0;
        }
        .paths-step-select { flex: 1; min-width: 200px; }
        .paths-remove-step { 
            background: none; border: none; color: #999; cursor: pointer; font-size: 18px; 
            padding: 4px; line-height: 1; transition: color 0.2s;
        }
        .paths-remove-step:hover { color: var(--error-color); }
        .paths-builder-actions { display: flex; gap: 12px; margin-top: 16px; align-items: center; }
        
        .paths-results { display: flex; flex-direction: column; gap: 24px; }
        .paths-insights-list { list-style: none; padding: 0; margin: 0; }
        .paths-insight-item { 
            padding: 8px 12px; border-radius: 4px; margin-bottom: 8px; font-size: 14px; 
            border-left: 4px solid transparent;
        }
        .paths-insight-item.global { background: #f0f7ff; border-left-color: #007bff; color: #004085; }
        .paths-insight-item.cohort { background: #fff3cd; border-left-color: #ffc107; color: #856404; margin-top: 8px; }
        
        .paths-cohort-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; border-bottom: 1px solid #eee; padding-bottom: 12px; }
        .paths-cohort-size { font-size: 14px; color: #666; }
        
        .paths-cohort-card { overflow: visible !important; }
        .table-responsive { overflow: visible !important; }
        
        .paths-table { width: 100%; border-collapse: collapse; font-size: 14px; }
        .paths-table th { text-align: left; padding: 12px 8px; border-bottom: 2px solid #eee; color: #666; font-weight: 600; }
        .paths-table td { padding: 12px 8px; border-bottom: 1px solid #eee; vertical-align: middle; }
        .paths-table tr:last-child td { border-bottom: none; }
        .paths-event-pill { 
            display: inline-block; padding: 2px 8px; background: #f0f0f0; border-radius: 12px; 
            font-size: 12px; font-weight: 500; color: #333;
        }
        .text-right { text-align: right !important; }
        .paths-action-dropdown { position: relative; display: inline-block; }
        .dropdown-menu { 
            display: none; position: absolute; right: 0; top: 100%; 
            background: white; border: 1px solid #ddd; border-radius: 4px; 
            box-shadow: 0 4px 16px rgba(0,0,0,0.15); z-index: 9999; min-width: 160px;
            padding: 4px 0; margin-top: 6px;
        }
        /* Bridge the hover gap */
        .dropdown-menu::after {
            content: '';
            position: absolute;
            bottom: 100%;
            left: 0;
            right: 0;
            height: 10px;
        }
        .paths-action-dropdown:hover .dropdown-menu { display: block; }
        .dropdown-menu button { 
            display: block; width: 100%; padding: 8px 12px; border: none; 
            background: none; text-align: left; font-size: 13px; cursor: pointer;
            color: #333;
        }
        .dropdown-menu button:hover { background: #f5f5f5; color: var(--primary-color); }

        .modal-overlay {
            position: fixed; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center;
            z-index: 1000;
        }
        .modal-content {
            background: white; padding: 24px; border-radius: 8px; width: 100%; max-width: 450px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.2);
        }
        .modal-content h4 { margin-top: 0; margin-bottom: 12px; }
        .modal-footer { display: flex; justify-content: flex-end; gap: 12px; margin-top: 20px; }
        .modal-input { 
            width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; 
            font-size: 14px; margin-top: 8px;
        }

        .animate-fade-in { animation: fadeIn 0.4s ease-out; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
      `}</style>

      {showNamingModal && (
        <div className="modal-overlay" onClick={() => setShowNamingModal(null)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <h4>Create Cohort</h4>
            <p className="pane-section-hint">Give your new cohort a descriptive name:</p>
            <input 
              type="text" 
              className="modal-input" 
              value={customName} 
              onChange={e => setCustomName(e.target.value)}
              autoFocus
            />
            <div className="modal-footer">
              <button className="button button-secondary" onClick={() => setShowNamingModal(null)}>Cancel</button>
              <button className="button button-primary" onClick={handleConfirmCreate}>Create Cohort</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
