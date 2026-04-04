import { useState, useEffect, useCallback, useRef } from 'react'
import { createPortal } from 'react-dom'
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
// Visual Funnel Chart (Section 1)
// ---------------------------------------------------------------------------

function PathsFunnelChart({ result }) {
    if (!result || !result.results || result.results.length === 0) return null

    const stepNames = result.steps || []
    const cohorts = result.results

    return (
        <section className="card ui-card paths-funnel-chart-section">
            <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '20px' }}>
                {result.path_name || 'Conversion Funnel'}
            </h3>
            <div className="funnel-chart" data-testid="paths-funnel-chart">
                {stepNames.map((stepName, stepIdx) => (
                    <div key={stepIdx} className="funnel-step-row">
                        <div className="funnel-step-name">
                            {stepIdx > 0 && <span className="funnel-step-arrow">↓</span>}
                            <span className="funnel-step-pill">{stepIdx + 1}</span>
                            <span className="funnel-step-event-label">{stepName}</span>
                        </div>

                        <div className="funnel-bars">
                            {cohorts.map((cohort, cohortIdx) => {
                                const stepData = cohort.steps[stepIdx] || { users: 0, conversion_pct: 0, drop_off_pct: 0 }
                                const barWidth = Number(stepData.conversion_pct) || 0
                                const minWidthPx = barWidth > 0 ? '2px' : '0px'
                                return (
                                    <div
                                        key={cohort.cohort_id}
                                        className="funnel-bar-row"
                                    >
                                        <div
                                            className="funnel-cohort-label"
                                            style={{ color: getCohortColor(cohort.cohort_id, cohortIdx) }}
                                            title={cohort.cohort_name}
                                        >
                                            {cohort.cohort_name}
                                        </div>
                                        <div className="funnel-bar-track">
                                            <div
                                                className="funnel-bar-fill"
                                                style={{
                                                    width: `${Number(Math.min(100, barWidth).toFixed(1))}%`,
                                                    background: getCohortColor(cohort.cohort_id, cohortIdx),
                                                    minWidth: minWidthPx,
                                                }}
                                            />
                                            <div className="funnel-bar-meta">
                                                <span className="funnel-bar-users">{formatInteger(stepData.users)}</span>
                                                <span className="funnel-bar-pct">({(Number(stepData.conversion_pct) || 0).toFixed(1)}%)</span>
                                                {stepIdx > 0 && (Number(stepData.drop_off_pct) || 0) > 0 && (
                                                    <span className="funnel-bar-dropoff" title="Drop-off from previous step">
                                                        ↓{(Number(stepData.drop_off_pct) || 0).toFixed(1)}%
                                                    </span>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                )
                            })}
                        </div>
                    </div>
                ))}
            </div>
        </section>
    )
}

function formatStepGap(minutes) {
    if (minutes === null || minutes === undefined) return 'Unlimited'
    if (minutes === 1440) return '1 day'
    if (minutes % 1440 === 0) return `${minutes / 1440} days`
    if (minutes === 60) return '1 hour'
    if (minutes % 60 === 0) return `${minutes / 60} hours`
    return `${minutes} minutes`
}

export default function PathsPane({ refreshToken, events, state, setState, onRefreshCohorts, appliedFilters = [], onAddToExport }) {
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
  const [activeDropdown, setActiveDropdown] = useState(null) // { sIdx, cohort, rect }
  const closeTimerRef = useRef(null)

  const handleDropdownEnter = (e, sIdx, cohort) => {
    if (closeTimerRef.current) clearTimeout(closeTimerRef.current)
    const rect = e.currentTarget.getBoundingClientRect()
    setActiveDropdown({ sIdx, cohort, rect })
  }

  const handleDropdownLeave = () => {
    closeTimerRef.current = setTimeout(() => {
      setActiveDropdown(null)
    }, 150)
  }

  const handleMenuEnter = () => {
    if (closeTimerRef.current) clearTimeout(closeTimerRef.current)
  }

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

  useEffect(() => {
    if (refreshToken > 0 && selectedPathId) {
        handleRun()
    }
  }, [refreshToken])

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
      const data = await runPaths(
        editingPath.steps, 
        editingPath.max_step_gap_minutes, 
        isUnsaved ? null : selectedPathId
      )
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

  const handleAddToExport = () => {
    if (!result || !result.results) return

    const tables = [{
      title: 'Path Analysis Details',
      columns: [
        { key: 'cohort_name', label: 'Cohort Name', type: 'string' },
        { key: 'cohort_size', label: 'Cohort Size', type: 'number' },
        { key: 'step', label: 'Step', type: 'number' },
        { key: 'event', label: 'Event', type: 'string' },
        { key: 'users', label: 'Users', type: 'number' },
        { key: 'conversion_pct', label: 'Conv %', type: 'percentage' },
        { key: 'drop_off_pct', label: 'Drop-off %', type: 'percentage' },
        { key: 'mean_time', label: 'Mean Time (s)', type: 'number' },
        { key: 'p20', label: 'P20 (s)', type: 'number' },
        { key: 'p80', label: 'P80 (s)', type: 'number' }
      ],
      data: result.results.flatMap(cohort => 
        cohort.steps.map(s => ({
          cohort_name: cohort.cohort_name,
          cohort_size: cohort.cohort_size,
          step: s.step,
          event: s.event,
          users: s.users,
          conversion_pct: s.conversion_pct / 100,
          drop_off_pct: s.drop_off_pct !== null ? s.drop_off_pct / 100 : null,
          mean_time: s.mean_time,
          p20: s.p20,
          p80: s.p80
        }))
      )
    }]

    const payload = {
      id: crypto.randomUUID(),
      version: 2,
      type: 'paths',
      title: `Paths — ${result.path_name || 'Analysis'}`,
      summary: `Paths analysis for ${result.results.length} cohorts`,
      tables,
      meta: {
        filters: appliedFilters,
        cohorts: result.results.map(r => ({ cohort_id: r.cohort_id, name: r.cohort_name })),
        settings: {
          'Path Name': result.path_name || 'Unnamed',
          'Steps': result.steps.join(' → ')
        }
      }
    }
    onAddToExport(payload)
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
    let defaultName = ''
    if (type === 'reached') {
        defaultName = `${cohortName} - Reached Step ${stepIdx} (${eventName})`
    } else {
        if (stepIdx === 1) {
            defaultName = `${cohortName} - Didn't perform Step 1 (${eventName})`
        } else {
            const prevEventName = editingPath.steps[stepIdx - 2].event_name
            defaultName = `${cohortName} - Drop off after Step ${stepIdx - 1} (${prevEventName})`
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
    
    const effectivePathId = isUnsaved ? null : selectedPathId

    try {
      if (type === 'reached') {
        await createPathsReachedCohort(
          cohortId, 
          stepIdx, 
          editingPath.steps, 
          nameToUse, 
          editingPath.max_step_gap_minutes,
          effectivePathId
        )
      } else {
        await createPathsDropOffCohort(
          cohortId, 
          stepIdx, 
          editingPath.steps, 
          nameToUse, 
          editingPath.max_step_gap_minutes,
          effectivePathId
        )
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

        <button
            type="button"
            className="button button-secondary"
            onClick={handleAddToExport}
            disabled={!result}
            title="Add cohort tables to global export buffer"
        >
            📸 Add to Export
        </button>
      </div>

      {result && result.max_step_gap_minutes !== null && result.max_step_gap_minutes !== undefined && (
        <div className="funnel-notice animate-fade-in" style={{ background: '#f8fafc', border: '1px solid #e2e8f0', color: '#475569' }}>
          ⏱ Max time between steps: <strong>{formatStepGap(result.max_step_gap_minutes)}</strong> (Each step must occur within {formatStepGap(result.max_step_gap_minutes)} of the{' '}
          <span className="tooltip-trigger" style={{ textDecoration: 'underline', cursor: 'help' }}>
            previous step
            <span className="tooltip-content">Time is measured from when the user completes the previous step</span>
          </span>)
        </div>
      )}

      {result && (
        <div className="paths-results animate-fade-in">
          {/* Section 1: Combined Visual Funnel Chart */}
          <PathsFunnelChart result={result} />

          {/* Section 2: Detailed Cohort Tables (Grouped in one card) */}
          <section className="card ui-card paths-cohort-tables-card">
            <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '20px' }}>
                Cohort Tables
            </h3>
            {result.results.map((cohort, cohortIdx) => (
              <div key={cohort.cohort_id} className="paths-cohort-table-wrapper" style={{ marginBottom: cohortIdx < result.results.length - 1 ? '40px' : 0 }}>
                <div className="paths-cohort-header" style={{ borderBottom: '1px solid #f1f5f9', paddingBottom: '12px', marginBottom: '16px' }}>
                  <h3 style={{ margin: 0, color: getCohortColor(cohort.cohort_id, cohortIdx) }}>
                    {cohort.cohort_name} <span style={{ fontWeight: 400, color: '#64748b', marginLeft: '6px', fontSize: '0.9em' }}>({formatInteger(cohort.cohort_size)} users)</span>
                  </h3>
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
                                onMouseEnter={(e) => handleDropdownEnter(e, sIdx, cohort)}
                                onMouseLeave={handleDropdownLeave}
                              >
                                {creatingCohort?.startsWith(`${cohort.cohort_id}-${s.step}`) ? 'Creating...' : 'Create Cohort ▼'}
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </section>

          {(result.global_insights?.length > 0 || result.results.some(c => c.insights?.length > 0)) && (
            <section className="card ui-card paths-all-insights">
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
        .paths-pane { display: flex; flex-direction: column; gap: 20px; padding-top: 16px; padding-bottom: 40px; }
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
            position: absolute; right: 0; top: 100%; background: white; border: 1px solid #ddd; 
            border-radius: 4px; box-shadow: 0 4px 16px rgba(0,0,0,0.15); z-index: 9999; min-width: 160px;
            padding: 4px 0; margin-top: 6px;
        }
        .dropdown-menu::before {
            content: '';
            position: absolute;
            top: -10px;
            left: 0;
            right: 0;
            height: 10px;
        }
        .dropdown-menu button { display: block; width: 100%; padding: 8px 12px; border: none; background: none; text-align: left; font-size: 13px; cursor: pointer; color: #333; }
        .dropdown-menu button:hover { background: #f5f5f5; color: var(--primary-color); }
        .modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center; z-index: 1000; }
        .modal-content { background: white; padding: 24px; border-radius: 8px; width: 100%; max-width: 450px; box-shadow: 0 10px 25px rgba(0,0,0,0.2); }
        .modal-footer { display: flex; justify-content: flex-end; gap: 12px; margin-top: 20px; }
        .modal-input { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; margin-top: 8px; }
        .animate-fade-in { animation: fadeIn 0.4s ease-out; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .funnel-option-invalid { color: #999; font-style: italic; }

        /* Funnel Chart Styles */
        .funnel-step-row { margin-bottom: 24px; }
        .funnel-step-row:last-child { margin-bottom: 0; }
        .funnel-step-name { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
        .funnel-step-pill { display: flex; align-items: center; justify-content: center; width: 22px; height: 22px; border-radius: 50%; background: #ecfdf3; color: #2e7d32; font-size: 11px; font-weight: 700; }
        .funnel-step-event-label { font-size: 14px; font-weight: 600; color: #111; }
        .funnel-step-arrow { color: #999; opacity: 0.5; }
        .funnel-bars { display: flex; flex-direction: column; gap: 8px; padding-left: 30px; }
        .funnel-bar-row { display: grid; grid-template-columns: 180px 1fr; align-items: center; gap: 12px; min-height: 28px; }
        .funnel-cohort-label { font-size: 12px; font-weight: 600; text-overflow: ellipsis; overflow: hidden; white-space: nowrap; }
        .funnel-bar-track { width: 100%; height: 20px; border-radius: 4px; background: #f3f4f6; overflow: hidden; position: relative; }
        .funnel-bar-fill { height: 100%; border-radius: 4px; transition: width 0.4s cubic-bezier(.4,0,.2,1); }
        .funnel-bar-meta { position: absolute; right: 8px; top: 50%; transform: translateY(-50%); display: flex; gap: 6px; font-size: 11px; font-variant-numeric: tabular-nums; align-items: center; pointer-events: none; }
        .funnel-bar-users { font-weight: 700; color: #111; }
        .funnel-bar-pct { color: #666; }
        .funnel-bar-dropoff { color: #dc2626; font-weight: 700; }
        .paths-funnel-chart-section { padding-bottom: 0px !important; }
        .paths-results { display: flex; flex-direction: column; gap: 12px; }

        .tooltip-trigger { position: relative; display: inline-block; }
        .tooltip-content {
          visibility: hidden; width: 220px; background-color: #334155; color: #fff; text-align: center;
          border-radius: 6px; padding: 8px; position: absolute; z-index: 10002; bottom: 125%; left: 50%;
          margin-left: -110px; opacity: 0; transition: opacity 0.3s; font-size: 12px; font-weight: normal; line-height: 1.4;
          box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); pointer-events: none;
        }
        .tooltip-trigger:hover .tooltip-content { visibility: visible; opacity: 1; }
        .tooltip-content::after {
          content: ""; position: absolute; top: 100%; left: 50%; margin-left: -5px; border-width: 5px; border-style: solid; border-color: #334155 transparent transparent transparent;
        }
      `}</style>
      {activeDropdown && createPortal(
        <div 
          className="dropdown-menu animate-fade-in"
          onMouseEnter={handleMenuEnter}
          onMouseLeave={handleDropdownLeave}
          style={{
            display: 'block',
            position: 'fixed',
            top: activeDropdown.rect.bottom + 6,
            left: activeDropdown.rect.right - 160,
            zIndex: 10001,
            margin: 0
          }}
        >
          <button onClick={() => { handleOpenNamingModal(activeDropdown.cohort.cohort_id, activeDropdown.sIdx + 1, 'reached', activeDropdown.cohort.steps[activeDropdown.sIdx].event, activeDropdown.cohort.cohort_name); setActiveDropdown(null); }}>
            Reached Step {activeDropdown.sIdx + 1}
          </button>
          {activeDropdown.sIdx === 0 && activeDropdown.cohort.steps[activeDropdown.sIdx].conversion_pct < 100 && (
            <button onClick={() => { handleOpenNamingModal(activeDropdown.cohort.cohort_id, activeDropdown.sIdx + 1, 'dropoff', activeDropdown.cohort.steps[activeDropdown.sIdx].event, activeDropdown.cohort.cohort_name); setActiveDropdown(null); }}>
              Did not start ({activeDropdown.cohort.steps[activeDropdown.sIdx].event})
            </button>
          )}
          {activeDropdown.sIdx > 0 && activeDropdown.cohort.steps[activeDropdown.sIdx].drop_off_pct > 0 && (
            <button onClick={() => { handleOpenNamingModal(activeDropdown.cohort.cohort_id, activeDropdown.sIdx + 1, 'dropoff', activeDropdown.cohort.steps[activeDropdown.sIdx].event, activeDropdown.cohort.cohort_name); setActiveDropdown(null); }}>
              Drop-off after {activeDropdown.cohort.steps[activeDropdown.sIdx-1].event}
            </button>
          )}
        </div>,
        document.body
      )}
    </div>
  )
}
