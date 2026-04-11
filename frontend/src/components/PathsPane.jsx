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
// Action Icons
// ---------------------------------------------------------------------------

function CopyIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
    </svg>
  )
}

function EditIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
      <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
    </svg>
  )
}

function TrashIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="3 6 5 6 21 6"></polyline>
      <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
      <line x1="10" y1="11" x2="10" y2="17"></line>
      <line x1="14" y1="11" x2="14" y2="17"></line>
    </svg>
  )
}

function PlusIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="5" x2="12" y2="19"></line>
      <line x1="5" y1="12" x2="19" y2="12"></line>
    </svg>
  )
}

function PlayIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="5 3 19 12 5 21 5 3"></polygon>
    </svg>
  )
}

function ExportIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"></path>
      <circle cx="12" cy="13" r="4"></circle>
    </svg>
  )
}

function CompareIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M16 3h5v5"></path>
      <path d="M4 20L21 3"></path>
      <path d="M21 16v5h-5"></path>
      <path d="M15 15l6 6"></path>
      <path d="M4 4l5 5"></path>
    </svg>
  )
}

// ---------------------------------------------------------------------------
// Paths Selector
// ---------------------------------------------------------------------------

function PathsSelector({ paths, value, onChange, excludeId }) {
  const valid = paths.filter(p => p.is_valid && p.id !== excludeId)
  const invalid = paths.filter(p => !p.is_valid && p.id !== excludeId)

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

function PathsFunnelChart({ result, pathLabel }) {
    if (!result || !result.results || result.results.length === 0) return null

    const stepNames = result.steps || []
    const cohorts = result.results

    return (
        <section className="card ui-card paths-funnel-chart-section">
            <div className="funnel-header" style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div className="funnel-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    {pathLabel && <span className="path-label-pill">{pathLabel}</span>}
                    <h3 style={{ margin: 0, fontSize: '16px', fontWeight: 600 }}>{result.path_name || 'Conversion Funnel'}</h3>
                </div>
                <div className="funnel-meta" style={{ fontSize: '12px', color: '#6b7280' }}>
                    Max time between steps: <strong>{formatStepGap(result.max_step_gap_minutes)}</strong>
                </div>
            </div>
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

function FunnelSkeleton({ pathLabel }) {
    return (
        <section className="card ui-card funnel-skeleton">
            <div className="funnel-header" style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div className="funnel-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    {pathLabel && <span className="path-label-pill">{pathLabel}</span>}
                    <div style={{ height: '20px', background: '#e5e7eb', borderRadius: '4px', width: '120px' }} />
                </div>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                {[1, 2, 3].map(i => (
                    <div key={i} style={{ display: 'flex', gap: '15px', alignItems: 'center' }}>
                        <div style={{ width: '100px', height: '15px', background: '#e5e7eb', borderRadius: '4px' }} />
                        <div style={{ flex: 1, height: '20px', background: '#f3f4f6', borderRadius: '4px' }} />
                    </div>
                ))}
            </div>
        </section>
    )
}

function TableSkeleton() {
    return (
        <section className="card ui-card paths-cohort-tables-card">
            <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '20px' }}>
                Cohort Tables
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '32px' }}>
                {[1, 2].map(i => (
                    <div key={i} style={{ opacity: 0.5 }}>
                        <div style={{ height: '24px', background: '#f1f5f9', borderRadius: '4px', width: '200px', marginBottom: '16px' }} />
                        <div style={{ height: '150px', background: '#f8fafc', borderRadius: '8px', border: '1px solid #e2e8f0' }} />
                    </div>
                ))}
            </div>
        </section>
    )
}

export default function PathsPane({ refreshToken, events, state, setState, onRefreshCohorts, appliedFilters = [], onAddToExport }) {
  const [paths, setPaths] = useState([])
  const [selectedPathId, setSelectedPathId] = useState(state?.selectedPathId || null)
  const [comparePathId, setComparePathId] = useState(state?.comparePathId || null)
  const [showCompareDropdown, setShowCompareDropdown] = useState(!!state?.comparePathId)
  
  const [editingPath, setEditingPath] = useState(null) // Local "unsaved" state
  const [isUnsaved, setIsUnsaved] = useState(false)
  
  const [running, setRunning] = useState(false)
  const [results, setResults] = useState(state?.results || {})
  const [resultsStale, setResultsStale] = useState(false)
  const [error, setError] = useState('')
  
  const [showBuilder, setShowBuilder] = useState(false)
  const [builderMode, setBuilderMode] = useState('create') // 'create' | 'edit'
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
    setState({ selectedPathId, comparePathId, results })
  }, [selectedPathId, comparePathId, results, setState])

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
    setResults({})
    setResultsStale(false)
    setError('')
  }

  const handleCompareSelect = (id) => {
    setComparePathId(id)
    setResultsStale(true)
  }

  const handleCompareToggle = () => {
    if (showCompareDropdown) {
        // Exiting compare — clean up
        setResults(prev => {
            const next = { ...prev }
            if (comparePathId) delete next[comparePathId]
            return next
        })
        setComparePathId(null)
        setShowCompareDropdown(false)
        // No stale notice triggered on toggle OFF per user request
    } else {
        // Entering compare — show dropdown
        setShowCompareDropdown(true)
        // No stale notice triggered on toggle ON per user request
    }
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
            setBuilderMode('edit')
            setIsUnsaved(false)
        }
    }
  }, [paths, selectedPathId, editingPath, showBuilder])

  const handleCopyPath = (path) => {
    if (!path) return
    const cloned = structuredClone(path)
    
    // Sanitize
    delete cloned.id
    delete cloned.created_at
    delete cloned.updated_at
    delete cloned.results
    delete cloned.is_valid
    delete cloned.invalid_reason

    // Improved Naming logic
    const baseName = path.name || ""
    const cleanName = baseName.replace(/\s*\(copy\)$/i, "")
    cloned.name = cleanName ? `${cleanName} (copy)` : "(copy)"

    // Safeguard
    if (!cloned.steps || cloned.steps.length === 0) {
        cloned.steps = [
            { step_order: 0, groups: [{ event_name: '', filters: [] }] },
            { step_order: 1, groups: [{ event_name: '', filters: [] }] }
        ]
    }

    setEditingPath(cloned)
    setBuilderMode('create')
    setShowBuilder(true)
  }

  const handleRun = async () => {
    if (!editingPath || !editingPath.steps) return
    const thisRunId = ++runIdRef.current
    setRunning(true)
    setError('')
    setResults({})
    setResultsStale(false)

    const pathsToRun = [
        { id: selectedPathId, path: editingPath }
    ]

    if (comparePathId) {
        const comparePath = paths.find(p => p.id === comparePathId)
        if (comparePath) {
            pathsToRun.push({ id: comparePathId, path: comparePath })
        } else {
            // Path B was deleted or is missing
            setComparePathId(null)
            setShowCompareDropdown(false)
        }
    }

    try {
        const settled = await Promise.allSettled(
            pathsToRun.map(({ id, path }) => 
                runPaths(
                    path.steps,
                    path.max_step_gap_minutes,
                    isUnsaved && id === selectedPathId ? null : id
                ).then(data => ({ id, data }))
            )
        )

        if (runIdRef.current === thisRunId) {
            const newResults = {}
            let firstErr = null

            settled.forEach(outcome => {
                if (outcome.status === 'fulfilled') {
                    newResults[outcome.value.id] = outcome.value.data
                } else {
                    firstErr = outcome.reason?.message || 'Failed to run analysis'
                }
            })

            setResults(newResults)
            if (firstErr && !Object.keys(newResults).length) {
                setError(firstErr)
            }
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
    const activePaths = [selectedPathId]
    if (comparePathId) activePaths.push(comparePathId)

    const canExport = results[selectedPathId] && (!comparePathId || results[comparePathId])
    if (!canExport) return

    activePaths.forEach((pathId, idx) => {
        const pathResult = results[pathId]
        if (!pathResult) return

        const label = comparePathId ? (idx === 0 ? 'Path A' : 'Path B') : null
        
        const tables = [{
          title: label ? `${label} Analysis Details` : 'Path Analysis Details',
          columns: [
            { key: 'cohort_name', label: 'Cohort Name', type: 'string' },
            { key: 'cohort_size', label: 'Cohort Size', type: 'number' },
            { key: 'step', label: 'Step', type: 'number' },
            { key: 'event', label: 'Event', type: 'string' },
            { key: 'users', label: 'Users', type: 'number' },
            { key: 'conversion_pct', label: 'Cohort %', type: 'percentage' },
            { key: 'drop_off_pct', label: 'Drop-off %', type: 'percentage' },
            { key: 'mean_time', label: 'Mean Time (s)', type: 'number' },
            { key: 'p20', label: 'P20 (s)', type: 'number' },
            { key: 'p80', label: 'P80 (s)', type: 'number' }
          ],
          data: pathResult.results.flatMap(cohort => 
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
          title: label ? `${label} — ${pathResult.path_name || 'Analysis'}` : `Paths — ${pathResult.path_name || 'Analysis'}`,
          summary: `Paths analysis for ${pathResult.results.length} cohorts`,
          tables,
          meta: {
            filters: appliedFilters,
            cohorts: pathResult.results.map(r => ({ cohort_id: r.cohort_id, name: r.cohort_name })),
            settings: {
              'Path Name': pathResult.path_name || 'Unnamed',
              'Steps': pathResult.steps.join(' → ')
            }
          }
        }
        onAddToExport(payload)
    })
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
            // eventName passed here is the current step's event. 
            // For drop-off, the table usually passes the event that was meant to be performed.
            defaultName = `${cohortName} - Drop off at Step ${stepIdx} (${eventName})`
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
  const compareMode = comparePathId !== null
  const canExport = results[selectedPathId] && (!comparePathId || results[comparePathId])

  return (
    <div className="paths-pane" data-testid="paths-pane">
      <div className="funnel-topbar">
        <div className="main-actions">
          <button
            className="new-path-btn"
            onClick={() => {
              setEditingPath(null)
              setBuilderMode('create')
              setShowBuilder(true)
            }}
            title="Create new path"
            aria-label="Create new path"
          >
            <PlusIcon />
          </button>

          <button
            className="run-path-btn"
            onClick={handleRun}
            disabled={!editingPath || (!isSelectedValid && !isUnsaved) || running}
            aria-label={running ? 'Running analysis' : 'Run path'}
            title={!selectedPathId ? 'Select or create a path first' : (selectedPathBase && !selectedPathBase.is_valid && !isUnsaved) ? selectedPathBase.invalid_reason : (running ? 'Running…' : 'Run path')}
          >
            {running ? <span className="spinner" /> : <PlayIcon />}
          </button>
        </div>

        <PathsSelector
          paths={paths}
          value={selectedPathId}
          onChange={handleSelectPath}
        />

        <div className="path-actions-group utility-gap">
          {selectedPathId && (
            <>
              <button
                className="action-icon-button"
                onClick={() => handleCopyPath(selectedPathBase)}
                title="Duplicate path"
                aria-label="Duplicate path"
                disabled={deletingId === selectedPathId || running}
              >
                <CopyIcon />
              </button>
              <button
                className="action-icon-button"
                onClick={() => {
                  setEditingPath(JSON.parse(JSON.stringify(selectedPathBase)))
                  setBuilderMode('edit')
                  setShowBuilder(true)
                }}
                title="Edit path"
                aria-label="Edit path"
                disabled={deletingId === selectedPathId || running}
              >
                <EditIcon />
              </button>
              <button
                className="action-icon-button icon-delete"
                onClick={() => handleDelete(selectedPathId)}
                title="Delete path"
                aria-label="Delete path"
                disabled={deletingId === selectedPathId || running}
              >
                {deletingId === selectedPathId ? '...' : <TrashIcon />}
              </button>
            </>
          )}

          <button
            className={`action-icon-button ${showCompareDropdown ? 'compare-toggle-active' : ''}`}
            onClick={handleCompareToggle}
            title="Compare another path"
            aria-label="Compare another path"
            disabled={running}
          >
            <CompareIcon />
          </button>
        </div>

        {showCompareDropdown && (
          <div className="compare-dropdown-wrapper animate-fade-in">
            <span className="compare-vs">vs</span>
            <PathsSelector
                paths={paths}
                value={comparePathId}
                onChange={handleCompareSelect}
                excludeId={selectedPathId}
            />
          </div>
        )}

        <button
            type="button"
            className="action-icon-button"
            onClick={handleAddToExport}
            disabled={!canExport}
            aria-label="Add to Export"
            title="Add cohort tables to global export buffer"
        >
            <ExportIcon />
        </button>
      </div>

      {resultsStale && Object.keys(results).length > 0 && (
          <div className="stale-results-notice animate-fade-in">
              <span style={{ fontSize: '18px' }}>⚠️</span> Results outdated — click <PlayIcon /> to refresh (both paths)
          </div>
      )}



      {(Object.keys(results).length > 0 || running) && (
        <div className="paths-results animate-fade-in">
          {/* Section 1: Visual Funnel Charts */}
          <div className={`paths-compare-funnels ${!compareMode ? 'single' : ''}`}>
             {results[selectedPathId] ? (
                <PathsFunnelChart 
                    result={results[selectedPathId]} 
                    pathLabel={compareMode ? 'Path A' : null} 
                />
             ) : (running && (
                <FunnelSkeleton pathLabel={compareMode ? 'Path A' : null} />
             ))}
             
             {comparePathId && (
                 results[comparePathId] 
                    ? <PathsFunnelChart result={results[comparePathId]} pathLabel="Path B" />
                    : (running && <FunnelSkeleton pathLabel="Path B" />)
             )}
          </div>

          {/* Section 2: Detailed Cohort Tables */}
          {Object.keys(results).length > 0 ? (
            <section className="card ui-card paths-cohort-tables-card">
              <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '20px' }}>
                  Cohort Tables
              </h3>
              
              {Array.from(new Set(Object.values(results).flatMap(r => r.results.map(c => c.cohort_id)))).map(cohortId => {
                  const repResult = Object.values(results).find(r => r.results.some(c => c.cohort_id === cohortId))
                  const cohortSample = repResult.results.find(c => c.cohort_id === cohortId)

                  return (
                    <div key={cohortId} className="paths-cohort-table-wrapper" style={{ marginBottom: '40px' }}>
                      <div className="paths-cohort-header" style={{ borderBottom: '1px solid #f1f5f9', paddingBottom: '12px', marginBottom: '16px' }}>
                        <h3 style={{ margin: 0 }}>
                          {cohortSample.cohort_name} <span style={{ fontWeight: 400, color: '#64748b', marginLeft: '6px', fontSize: '0.9em' }}>({formatInteger(cohortSample.cohort_size)} users)</span>
                        </h3>
                      </div>

                      {[selectedPathId, comparePathId].filter(Boolean).map((pid, pIdx) => {
                          const pathResult = results[pid]
                          if (!pathResult) return null
                          const cohort = pathResult.results.find(c => c.cohort_id === cohortId)
                          if (!cohort) return null

                          return (
                            <div key={pid} className={pIdx > 0 ? 'compare-path-section' : ''}>
                              {compareMode && (
                                  <div className="path-label-pill" style={{ marginBottom: '12px' }}>
                                      {pIdx === 0 ? 'Path A' : 'Path B'}: {pathResult.path_name}
                                  </div>
                              )}
                              <div className="table-responsive">
                                <table className="paths-table">
                                  <thead>
                                    <tr>
                                      <th>Step</th>
                                      <th>Event</th>
                                      <th className="text-right">Users</th>
                                      <th className="text-right">Cohort %</th>
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
                                        <td>
                                          <div className="paths-event-cell">
                                            <span className="paths-event-pill">{s.event}</span>
                                            {s.group_breakdown && (
                                              <div className="group-breakdown-mini">
                                                {Object.entries(s.group_breakdown).map(([name, pct], bi) => (
                                                  <div key={bi} className="breakdown-item" title={`${name}: ${pct}%`}>
                                                    <span className="breakdown-name">{name}</span>
                                                    <span className="breakdown-pct">{pct}%</span>
                                                  </div>
                                                ))}
                                              </div>
                                            )}
                                          </div>
                                        </td>
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
                          )
                      })}
                    </div>
                  )
              })}
            </section>
          ) : (running && <TableSkeleton />)}

          {(Object.values(results).some(r => r.global_insights?.length > 0 || r.results.some(c => c.insights?.length > 0))) && (
            <section className="card ui-card paths-all-insights">
              <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '12px', marginBottom: '16px' }}>Insights</h3>
              
              {Object.entries(results).map(([pid, r], rIdx) => (
                <div key={pid} className={rIdx > 0 ? 'compare-path-section' : ''}>
                  {compareMode && (
                      <div className="path-label-pill" style={{ marginBottom: '12px' }}>
                          {pid === String(selectedPathId) ? 'Path A' : 'Path B'}: {r.path_name}
                      </div>
                  )}
                  {r.global_insights?.length > 0 && (
                    <div className="paths-insight-group" style={{ marginBottom: '20px' }}>
                      <h4 style={{ fontSize: '15px', color: '#666', marginBottom: '8px' }}>Overall</h4>
                      <ul className="paths-insights-list">
                        {r.global_insights.map((insight, idx) => (
                          <li key={idx} className="paths-insight-item global">{insight}</li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {r.results.map((cohort, cohortIdx) => (
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
                </div>
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
        mode={builderMode}
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
        .paths-event-cell { display: flex; flex-direction: column; gap: 4px; }
        .paths-event-pill { display: inline-block; width: fit-content; padding: 2px 8px; background: #f0f0f0; border-radius: 12px; font-size: 12px; font-weight: 500; color: #333; }
        .group-breakdown-mini { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 4px; }
        .breakdown-item { display: flex; align-items: center; gap: 4px; background: #f8fafc; border: 1px solid #e2e8f0; padding: 1px 6px; border-radius: 4px; font-size: 10px; }
        .breakdown-name { color: #64748b; font-weight: 500; max-width: 80px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .breakdown-pct { color: #6366f1; font-weight: 700; }
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
        
        .path-actions-group { display: flex; align-items: center; gap: 4px; }
        .action-icon-button {
          display: flex; align-items: center; justify-content: center;
          width: 32px; height: 32px; border-radius: 6px; border: 1px solid #e2e8f0;
          background: white; color: #64748b; cursor: pointer; transition: all 0.2s;
          padding: 0;
        }
        .action-icon-button:hover:not(:disabled) {
          background: #f8fafc; color: var(--primary-color); border-color: #cbd5e1;
          transform: translateY(-1px);
        }
        .action-icon-button:disabled { cursor: not-allowed; opacity: 0.5; }
        .action-icon-button.icon-delete:hover:not(:disabled) {
          color: #ef4444; border-color: #fecaca; background: #fff1f1;
        }
        
        button:focus-visible {
          outline: 2px solid #16a34a;
          outline-offset: 2px;
        }

        .main-actions {
          display: flex;
          gap: 8px;
          align-items: center;
        }

        .utility-gap {
          margin-left: 8px;
        }

        .run-path-btn {
          background: #16a34a !important;
          color: white !important;
          border: none !important;
          border-radius: 8px !important;
          padding: 8px !important;
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          transition: all 0.2s ease !important;
          cursor: pointer;
        }

        .run-path-btn:hover:not(:disabled) {
          background: #15803d !important;
          transform: scale(1.05);
        }

        .run-path-btn:disabled {
          background: #e5e7eb !important;
          color: #9ca3af !important;
          cursor: not-allowed;
          transform: none !important;
        }

        .new-path-btn {
          background: #ecfdf5 !important;
          color: #16a34a !important;
          border: 1px solid #bbf7d0 !important;
          border-radius: 8px !important;
          padding: 8px !important;
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          transition: all 0.2s ease !important;
          cursor: pointer;
        }

        .new-path-btn:hover:not(:disabled) {
          background: #d1fae5 !important;
          transform: scale(1.05);
        }

        .spinner {
          width: 16px;
          height: 16px;
          border: 2px solid white;
          border-top-color: transparent;
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
        }

        @keyframes spin { to { transform: rotate(360deg); } }

        /* Funnel Chart Styles */
        .funnel-step-row { margin-bottom: 24px; }
        .funnel-step-row:last-child { margin-bottom: 0; }
        .funnel-step-name { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
        .funnel-step-pill { display: flex; align-items: center; justify-content: center; width: 22px; height: 22px; border-radius: 50%; background: #10b981; color: #ffffff !important; font-size: 11px; font-weight: 700; }
        .funnel-step-event-label { font-size: 14px; font-weight: 600; color: #111; }
        .funnel-step-arrow { color: #999; opacity: 0.5; }
        .funnel-bars { display: flex; flex-direction: column; gap: 8px; padding-left: 30px; }
        .funnel-bar-row { display: grid; grid-template-columns: 180px 1fr; align-items: center; gap: 12px; min-height: 28px; }
        .funnel-cohort-label { font-size: 12px; font-weight: 600; text-overflow: ellipsis; overflow: hidden; white-space: nowrap; }
        .funnel-bar-track { width: 100%; height: 20px; border-radius: 4px; background: #f3f4f6; overflow: hidden; position: relative; }
        .funnel-bar-fill { height: 100%; border-radius: 4px; transition: width 0.4s cubic-bezier(.4,0,.2,1); }
        .funnel-bar-meta { position: absolute; right: 8px; top: 50%; transform: translateY(-50%); display: flex; gap: 6px; font-size: 11px; font-variant-numeric: tabular-nums; align-items: center; pointer-events: none; }
        .funnel-bar-users { font-weight: 700; color: #111; }
        .funnel-bar-pct { color: #111; }
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

        /* Dual Path Comparison Styles */
        .compare-dropdown-wrapper {
            display: flex;
            align-items: center;
            gap: 8px;
            padding-left: 8px;
            border-left: 1px solid #e2e8f0;
            margin-left: 8px;
        }
        .compare-vs {
            font-size: 12px;
            font-weight: 700;
            color: #94a3b8;
            text-transform: uppercase;
        }
        .compare-toggle-active {
            background: #e5e7eb !important;
            color: #374151 !important;
            border-color: #d1d5db !important;
        }
        .paths-compare-funnels {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        .paths-compare-funnels.single {
            grid-template-columns: 1fr;
        }
        .compare-path-section {
            margin-top: 32px;
            padding-top: 24px;
            border-top: 1px dashed #e2e8f0;
        }
        .path-label-pill {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 4px;
            font-weight: 600;
            font-size: 13px;
            background: #f0f4f8;
            color: #334155;
            border: 1px solid #e2e8f0;
        }
        .stale-results-notice {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            padding: 12px;
            background: #fffbeb;
            border: 1px solid #fde68a;
            border-radius: 8px;
            color: #b45309;
            font-weight: 600;
            font-size: 14px;
            margin-bottom: 8px;
        }
        .funnel-skeleton {
            background: #ffffff;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #e2e8f0;
            min-height: 200px;
            animation: skeletonPulse 1.5s ease-in-out infinite;
        }
        @keyframes skeletonPulse {
            0%, 100% { opacity: 0.8; }
            50% { opacity: 0.5; }
        }

        .funnel-header {
            margin-bottom: 24px;
        }
        .funnel-meta {
            font-weight: 500;
        }
        .path-label-pill {
            text-transform: uppercase;
            letter-spacing: 0.05em;
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
