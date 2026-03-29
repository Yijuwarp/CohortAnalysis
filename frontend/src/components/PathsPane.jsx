import { useState, useEffect, useCallback, useRef } from 'react'
import { 
  runPaths, 
  createPathsDropOffCohort, 
  createPathsReachedCohort, 
  listPaths, 
  deletePath 
} from '../api'
import { formatInteger, formatDuration } from '../utils/formatters'
import { getCohortColor } from '../utils/cohortColors'
import PathsBuilderModal from './PathsBuilderModal'

// ---------------------------------------------------------------------------
// Paths Selector
// ---------------------------------------------------------------------------

function PathsSelector({ paths, value, onChange }) {
  const valid = paths.filter(p => p.is_valid)
  const invalid = paths.filter(p => !p.is_valid)

  return (
    <div className="funnel-selector" data-testid="paths-selector">
      <select
        value={value ?? ''}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
      >
        <option value="">— Select Path —</option>
        {valid.length > 0 && (
          <optgroup label="Saved Paths">
            {valid.map(p => (
              <option key={p.id} value={p.id}>{p.name}</option>
            ))}
          </optgroup>
        )}
        {invalid.length > 0 && (
          <optgroup label="Invalid for Current Dataset">
            {invalid.map(p => (
              <option key={p.id} value={p.id} title={p.invalid_reason} className="funnel-option-invalid">
                {p.name} (invalid: {p.invalid_reason})
              </option>
            ))}
          </optgroup>
        )}
      </select>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main PathsPane
// ---------------------------------------------------------------------------

export default function PathsPane({ refreshToken, events, state, setState, onRefreshCohorts }) {
  const [paths, setPaths] = useState([])
  const [selectedPathId, setSelectedPathId] = useState(state?.selectedPathId || null)
  const [editingPath, setEditingPath] = useState(null) // Local "unsaved" state
  const [isUnsaved, setIsUnsaved] = useState(false)
  
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(state?.result || null)
  const [error, setError] = useState('')
  
  const [showBuilder, setShowBuilder] = useState(false)
  const [deletingId, setDeletingId] = useState(null)
  
  const [creatingCohort, setCreatingCohort] = useState(null)
  const [showNamingModal, setShowNamingModal] = useState(null)
  const [customName, setCustomName] = useState('')

  const runIdRef = useRef(0)

  // Sync state to parent
  useEffect(() => {
    setState({ selectedPathId, result })
  }, [selectedPathId, result, setState])

  const loadPaths = useCallback(async () => {
    try {
      const data = await listPaths()
      setPaths(data || [])
    } catch {
      setPaths([])
    }
  }, [])

  useEffect(() => {
    loadPaths()
  }, [loadPaths, refreshToken])

  // Handle path selection
  const handleSelectPath = (id, overridePath = null) => {
    setSelectedPathId(id)
    const found = overridePath || paths.find(p => p.id === id)
    if (found) {
        setEditingPath(JSON.parse(JSON.stringify(found))) // Deep clone for local edits
        setIsUnsaved(false)
    } else {
        setEditingPath(null)
        setIsUnsaved(false)
    }
    setResult(null)
    setError('')
  }

  // Hydrate editingPath on mount or when paths list loads if we have a selection
  useEffect(() => {
    // Only auto-hydrate if we have a selection, and we ARE NOT currently showing the builder
    // This prevents "+ New Path" (which sets editingPath to null) from being immediately 
    // overwritten by the selected path in the background.
    if (selectedPathId && paths.length > 0 && !editingPath && !showBuilder) {
        const found = paths.find(p => p.id === selectedPathId)
        if (found) {
            setEditingPath(JSON.parse(JSON.stringify(found)))
            setIsUnsaved(false)
        }
    }
  }, [paths, selectedPathId, editingPath, showBuilder])

  const handleRun = async () => {
    if (!editingPath || !editingPath.steps) return
    const thisRunId = ++runIdRef.current
    setRunning(true)
    setError('')
    setResult(null)
    try {
      const data = await runPaths(editingPath.steps)
      if (runIdRef.current === thisRunId) {
        setResult(data)
      }
    } catch (err) {
      if (runIdRef.current === thisRunId) {
        setError(err.message || 'Failed to run analysis')
      }
    } finally {
      if (runIdRef.current === thisRunId) {
        setRunning(false)
      }
    }
  }

  const handleDelete = async (id) => {
    if (!window.confirm('Are you sure you want to delete this path?')) return
    setDeletingId(id)
    try {
      await deletePath(id)
      if (selectedPathId === id) {
        setSelectedPathId(null)
        setEditingPath(null)
        setIsUnsaved(false)
        setResult(null)
      }
      await loadPaths()
    } catch (err) {
      alert(err.message || 'Failed to delete path')
    } finally {
      setDeletingId(null)
    }
  }

  // Cohort creation helpers
  const handleOpenNamingModal = (cohortId, stepIdx, type, eventName, cohortName) => {
    if (!editingPath) return
    const sequence = editingPath.steps.slice(0, stepIdx).map(s => s.event_name).join(' -> ')
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
    if (!showNamingModal || !editingPath) return
    const { cohortId, stepIdx, type } = showNamingModal
    const nameToUse = customName.trim()
    
    setCreatingCohort(`${cohortId}-${stepIdx}-${type}`)
    setShowNamingModal(null)
    
    try {
      if (type === 'reached') {
        await createPathsReachedCohort(cohortId, stepIdx, editingPath.steps, nameToUse)
      } else {
        await createPathsDropOffCohort(cohortId, stepIdx, editingPath.steps, nameToUse)
      }
      if (onRefreshCohorts) onRefreshCohorts()
    } catch (err) {
      alert(err.message || 'Failed to create cohort')
    } finally {
      setCreatingCohort(null)
    }
  }

  const selectedPathBase = paths.find(p => p.id === selectedPathId)
  const isSelectedValid = selectedPathBase?.is_valid ?? false

  return (
    <div className="paths-pane" data-testid="paths-pane">
      <div className="funnel-topbar">
        <button
          className="button button-primary"
          onClick={() => {
            setEditingPath(null)
            setShowBuilder(true)
          }}
        >
          + New Path
        </button>

        <PathsSelector
          paths={paths}
          value={selectedPathId}
          onChange={handleSelectPath}
        />

        {selectedPathId && (
          <>
            <button
              className="button button-secondary"
              onClick={() => {
                setEditingPath(JSON.parse(JSON.stringify(selectedPathBase)))
                setShowBuilder(true)
              }}
              disabled={deletingId === selectedPathId || running}
            >
              Edit
            </button>
            <button
              className="button button-secondary"
              onClick={() => handleDelete(selectedPathId)}
              disabled={deletingId === selectedPathId || running}
            >
              {deletingId === selectedPathId ? 'Deleting…' : 'Delete'}
            </button>
          </>
        )}

        <button
          className="button button-primary"
          onClick={handleRun}
          disabled={!editingPath || (!isSelectedValid && !isUnsaved) || running}
          title={!selectedPathId ? 'Select or create a path first' : (selectedPathBase && !selectedPathBase.is_valid && !isUnsaved) ? selectedPathBase.invalid_reason : 'Run Path'}
        >
          {running ? 'Running…' : 'Run Path'}
        </button>
      </div>
      
      {selectedPathBase && !selectedPathBase.is_valid && (
        <div className="funnel-invalid-notice animate-fade-in">
          ⚠️ This path is not applicable to the current dataset — {selectedPathBase.invalid_reason}
        </div>
      )}

      {result && (
        <div className="paths-results animate-fade-in">
          <div className="paths-cohort-results">
            {result.results.map((cohort, cohortIdx) => (
              <section key={cohort.cohort_id} className="card ui-card paths-cohort-card">
                <div className="paths-cohort-header">
                  <h4 style={{ color: getCohortColor(cohort.cohort_id, cohortIdx) }}>
                    {cohort.cohort_name} ({formatInteger(cohort.cohort_size)} users)
                  </h4>
                </div>

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
                                <span className="low-sample-warn" title="Low sample size - metrics may be unstable" style={{ marginLeft: '4px', fontSize: '12px' }}>⚠️</span>
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
                                    Drop-off after {cohort.steps[sIdx-1].event}
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

          {(result.global_insights?.length > 0 || result.results.some(c => c.insights?.length > 0)) && (
            <section className="card ui-card paths-all-insights" style={{ marginTop: '24px' }}>
              <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '16px' }}>Insights</h3>
              
              {result.global_insights?.length > 0 && (
                <div className="paths-insight-group" style={{ marginBottom: '20px' }}>
                  <h4 style={{ fontSize: '15px', color: '#666', marginBottom: '8px' }}>Overall</h4>
                  <ul className="paths-insights-list">
                    {result.global_insights.map((insight, idx) => (
                      <li key={idx} className="paths-insight-item global">{insight}</li>
                    ))}
                  </ul>
                </div>
              )}

              {result.results.map((cohort, cohortIdx) => (
                cohort.insights?.length > 0 && (
                  <div key={cohort.cohort_id} className="paths-insight-group" style={{ marginBottom: '20px' }}>
                    <h4 style={{ 
                      fontSize: '15px', 
                      color: getCohortColor(cohort.cohort_id, cohortIdx),
                      marginBottom: '8px' 
                    }}>
                      {cohort.cohort_name}
                    </h4>
                    <div className="paths-cohort-insights">
                      {cohort.insights.map((insight, idx) => (
                        <div key={idx} className="paths-insight-item cohort">{insight}</div>
                      ))}
                    </div>
                  </div>
                )
              ))}
            </section>
          )}
        </div>
      )}

      {error && <p className="error" style={{ marginTop: '20px' }}>{error}</p>}

      <PathsBuilderModal
        isOpen={showBuilder}
        onClose={() => setShowBuilder(false)}
        onSaved={(saved) => {
            loadPaths()
            handleSelectPath(saved.id, saved)
        }}
        editingPath={editingPath}
        events={events}
      />

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

      <style>{`
        .paths-pane { display: flex; flex-direction: column; gap: 24px; padding-top: 16px; padding-bottom: 40px; }
        .paths-results { display: flex; flex-direction: column; gap: 24px; }
        .paths-insight-item { padding: 8px 12px; border-radius: 4px; margin-bottom: 8px; font-size: 14px; border-left: 4px solid transparent; }
        .paths-insight-item.global { background: #f0f7ff; border-left-color: #007bff; color: #004085; }
        .paths-insight-item.cohort { background: #fff3cd; border-left-color: #ffc107; color: #856404; margin-top: 8px; }
        .paths-cohort-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; border-bottom: 1px solid #eee; padding-bottom: 12px; }
        .paths-cohort-card { overflow: visible !important; }
        .paths-table { width: 100%; border-collapse: collapse; font-size: 14px; }
        .paths-table th { text-align: left; padding: 12px 8px; border-bottom: 2px solid #eee; color: #666; font-weight: 600; }
        .paths-table td { padding: 12px 8px; border-bottom: 1px solid #eee; vertical-align: middle; }
        .paths-event-pill { display: inline-block; padding: 2px 8px; background: #f0f0f0; border-radius: 12px; font-size: 12px; font-weight: 500; color: #333; }
        .text-right { text-align: right !important; }
        .funnel-topbar { margin-top: 4px; display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
        .paths-action-dropdown { position: relative; display: inline-block; }
        .dropdown-menu { 
            display: none; position: absolute; right: 0; top: 100%; background: white; border: 1px solid #ddd; 
            border-radius: 4px; box-shadow: 0 4px 16px rgba(0,0,0,0.15); z-index: 9999; min-width: 160px;
            padding: 4px 0; margin-top: 6px;
        }
        .paths-action-dropdown:hover .dropdown-menu { display: block; }
        .dropdown-menu button { display: block; width: 100%; padding: 8px 12px; border: none; background: none; text-align: left; font-size: 13px; cursor: pointer; color: #333; }
        .dropdown-menu button:hover { background: #f5f5f5; color: var(--primary-color); }
        .modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center; z-index: 1000; }
        .modal-content { background: white; padding: 24px; border-radius: 8px; width: 100%; max-width: 450px; box-shadow: 0 10px 25px rgba(0,0,0,0.2); }
        .modal-footer { display: flex; justify-content: flex-end; gap: 12px; margin-top: 20px; }
        .modal-input { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; margin-top: 8px; }
        .animate-fade-in { animation: fadeIn 0.4s ease-out; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .funnel-option-invalid { color: #999; font-style: italic; }
      `}</style>
    </div>
  )
}
