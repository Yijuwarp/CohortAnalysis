import { useState, useEffect, useCallback, useRef } from 'react'
import { createFunnel, listFunnels, deleteFunnel, runFunnel, getEventProperties, getEventPropertyValues } from '../api'
import { formatInteger } from '../utils/formatters'
import { getCohortColor } from '../utils/cohortColors'

// ---------------------------------------------------------------------------
// Funnel Builder Modal
// ---------------------------------------------------------------------------

const EMPTY_STEP = () => ({ event_name: '', filters: [] })
const EMPTY_FILTER = () => ({ property_key: '', property_value: '' })

function FunnelBuilderModal({ events, isOpen, onClose, onCreated }) {
  // events is string[] from backend e.g. ["signup", "purchase"]
  const safeEvents = Array.isArray(events) ? events : []

  const [name, setName] = useState('')
  const [steps, setSteps] = useState([EMPTY_STEP(), EMPTY_STEP()])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  // { eventName -> string[] } — property key lists per event
  const [propsByEvent, setPropsByEvent] = useState({})
  // { "eventName::propKey" -> string[] } — value lists per event+prop combo
  const [valuesByProp, setValuesByProp] = useState({})

  const loadProps = useCallback(async (eventName) => {
    if (!eventName || propsByEvent[eventName] !== undefined) return
    // Optimistically mark as loading (empty array) to avoid duplicate fetches
    setPropsByEvent(prev => ({ ...prev, [eventName]: [] }))
    try {
      const data = await getEventProperties(eventName)
      setPropsByEvent(prev => ({ ...prev, [eventName]: data.properties || [] }))
    } catch {
      setPropsByEvent(prev => ({ ...prev, [eventName]: [] }))
    }
  }, [propsByEvent])

  const loadValues = useCallback(async (eventName, propKey) => {
    const cacheKey = `${eventName}::${propKey}`
    if (!eventName || !propKey || valuesByProp[cacheKey] !== undefined) return
    setValuesByProp(prev => ({ ...prev, [cacheKey]: [] }))
    try {
      const data = await getEventPropertyValues(eventName, propKey, 50)
      setValuesByProp(prev => ({ ...prev, [cacheKey]: data.values || [] }))
    } catch {
      setValuesByProp(prev => ({ ...prev, [cacheKey]: [] }))
    }
  }, [valuesByProp])

  // Reset on open
  useEffect(() => {
    if (!isOpen) return
    setName('')
    setSteps([EMPTY_STEP(), EMPTY_STEP()])
    setError('')
    setSaving(false)
    setPropsByEvent({})
    setValuesByProp({})
  }, [isOpen])

  if (!isOpen) return null

  const updateStep = (idx, field, value) => {
    setSteps(prev => prev.map((s, i) => i === idx ? { ...s, [field]: value, filters: field === 'event_name' ? [] : s.filters } : s))
    if (field === 'event_name' && value) {
      loadProps(value)
    }
  }

  const addStep = () => {
    if (steps.length >= 5) return
    setSteps(prev => [...prev, EMPTY_STEP()])
  }

  const removeStep = (idx) => {
    if (steps.length <= 2) return
    setSteps(prev => prev.filter((_, i) => i !== idx))
  }

  const addFilter = (stepIdx) => {
    setSteps(prev => prev.map((s, i) =>
      i === stepIdx ? { ...s, filters: [...s.filters, EMPTY_FILTER()] } : s
    ))
  }

  const removeFilter = (stepIdx, filterIdx) => {
    setSteps(prev => prev.map((s, i) =>
      i === stepIdx ? { ...s, filters: s.filters.filter((_, fi) => fi !== filterIdx) } : s
    ))
  }

  const updateFilter = (stepIdx, filterIdx, field, value) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx) return s
      const newFilters = s.filters.map((f, fi) =>
        fi !== filterIdx ? f : { ...f, [field]: value, ...(field === 'property_key' ? { property_value: '' } : {}) }
      )
      if (field === 'property_key' && value) {
        loadValues(s.event_name, value)
      }
      return { ...s, filters: newFilters }
    }))
  }

  const handleSave = async () => {
    setError('')
    const trimmedName = name.trim()
    if (!trimmedName) { setError('Funnel name is required'); return }
    for (let i = 0; i < steps.length; i++) {
      if (!steps[i].event_name.trim()) { setError(`Step ${i + 1}: select an event`); return }
    }

    setSaving(true)
    try {
      const payload = {
        name: trimmedName,
        steps: steps.map(s => ({
          event_name: s.event_name,
          filters: s.filters.filter(f => f.property_key && f.property_value),
        })),
      }
      await createFunnel(payload)
      onCreated()
      onClose()
    } catch (err) {
      setError(err.message || 'Failed to save funnel')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="funnel-modal-overlay" role="dialog" aria-modal="true" aria-label="Create Funnel">
      <div className="funnel-modal" data-testid="funnel-builder-modal">
        <div className="funnel-modal-header">
          <h2>Create New Funnel</h2>
          <button className="funnel-modal-close" onClick={onClose} aria-label="Close">✕</button>
        </div>

        <div className="funnel-modal-body">
          <div className="funnel-name-field">
            <label htmlFor="funnel-name-input">Funnel Name</label>
            <input
              id="funnel-name-input"
              data-testid="funnel-name-input"
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="e.g. signup → search → purchase"
              maxLength={120}
            />
          </div>

          <div className="funnel-steps-section">
            <p className="funnel-steps-label">Steps (2–5 required)</p>

            {steps.map((step, stepIdx) => {
              const props = propsByEvent[step.event_name] || []
              return (
                <div key={stepIdx} className="funnel-step-block" data-testid={`funnel-step-${stepIdx}`}>
                  <div className="funnel-step-header">
                    <span className="funnel-step-number">{stepIdx + 1}</span>

                    {/* Issue #1: events is string[], not { value, label }[] */}
                    {/* Issue #2: unique key on each option */}
                    <select
                      value={step.event_name}
                      onChange={e => updateStep(stepIdx, 'event_name', e.target.value)}
                      data-testid={`funnel-step-event-${stepIdx}`}
                      className="funnel-step-event-select"
                    >
                      <option value="">— select event —</option>
                      {safeEvents.length === 0 && (
                        <option value="" disabled>No events available in dataset</option>
                      )}
                      {safeEvents.map(ev => (
                        <option key={ev} value={ev}>{ev}</option>
                      ))}
                    </select>

                    {steps.length > 2 && (
                      <button
                        className="funnel-remove-step"
                        onClick={() => removeStep(stepIdx)}
                        aria-label={`Remove step ${stepIdx + 1}`}
                        type="button"
                      >Remove</button>
                    )}
                  </div>

                  {/* Property filters */}
                  {step.filters.map((filter, filterIdx) => {
                    const cacheKey = `${step.event_name}::${filter.property_key}`
                    const vals = valuesByProp[cacheKey] || []
                    return (
                      <div
                        key={filterIdx}  // Issue #2: unique key
                        className="funnel-filter-row"
                        data-testid={`funnel-filter-${stepIdx}-${filterIdx}`}
                      >
                        <span className="funnel-filter-where">where</span>

                        {/* Issue #2: unique keys on each prop option */}
                        <select
                          value={filter.property_key}
                          onChange={e => updateFilter(stepIdx, filterIdx, 'property_key', e.target.value)}
                          data-testid={`funnel-filter-key-${stepIdx}-${filterIdx}`}
                        >
                          <option value="">— property —</option>
                          {props.map(p => (
                            <option key={p} value={p}>{p}</option>
                          ))}
                        </select>

                        <span className="funnel-filter-equals">＝</span>

                        {vals.length > 0 ? (
                          <select
                            value={filter.property_value}
                            onChange={e => updateFilter(stepIdx, filterIdx, 'property_value', e.target.value)}
                            data-testid={`funnel-filter-val-${stepIdx}-${filterIdx}`}
                          >
                            <option value="">— value —</option>
                            {vals.map(v => (
                              <option key={v} value={v}>{v}</option>
                            ))}
                          </select>
                        ) : (
                          <input
                            type="text"
                            value={filter.property_value}
                            onChange={e => updateFilter(stepIdx, filterIdx, 'property_value', e.target.value)}
                            placeholder="value"
                            data-testid={`funnel-filter-val-text-${stepIdx}-${filterIdx}`}
                          />
                        )}

                        <button
                          type="button"
                          className="funnel-remove-filter"
                          onClick={() => removeFilter(stepIdx, filterIdx)}
                          aria-label="Remove filter"
                        >✕</button>
                      </div>
                    )
                  })}

                  <button
                    type="button"
                    className="funnel-add-filter-btn"
                    onClick={() => addFilter(stepIdx)}
                    disabled={!step.event_name}
                    data-testid={`funnel-add-filter-${stepIdx}`}
                  >+ Add filter</button>
                </div>
              )
            })}

            {steps.length < 5 && (
              <button
                type="button"
                className="funnel-add-step-btn"
                onClick={addStep}
                data-testid="funnel-add-step"
              >+ Add Step</button>
            )}
          </div>

          {error && <p className="error" data-testid="funnel-builder-error">{error}</p>}
        </div>

        <div className="funnel-modal-footer">
          <button type="button" className="button button-secondary" onClick={onClose} disabled={saving}>
            Cancel
          </button>
          <button
            type="button"
            className="button button-primary"
            onClick={handleSave}
            disabled={saving}
            data-testid="funnel-save-button"
          >
            {saving ? 'Saving…' : 'Save Funnel'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Bar funnel visualization — bar width = conversion_pct (% of each cohort's step 0)
// ---------------------------------------------------------------------------

function FunnelChart({ result }) {
  if (!result || !result.results || result.results.length === 0) return null

  const stepNames = result.steps || []
  const cohorts = result.results

  // Bar width = conversion_pct already computed by the backend per cohort.
  // Step 0 is always 100% for every cohort; subsequent steps scale linearly.
  // Two cohorts with the same % always render the same bar width regardless
  // of absolute user counts — no cross-cohort normalisation needed.

  return (
    <div className="funnel-chart" data-testid="funnel-chart">
      {stepNames.map((stepName, stepIdx) => (
        <div key={stepIdx} className="funnel-step-row">
          <div className="funnel-step-name">
            {stepIdx > 0 && <span className="funnel-step-arrow">↓</span>}
            <span className="funnel-step-pill">{stepIdx + 1}</span>
            <span className="funnel-step-event-label">{stepName}</span>
          </div>

          <div className="funnel-bars">
            {cohorts.map((cohort, cohortIdx) => {
              const stepData = cohort.steps[stepIdx] || { users: 0, conversion_pct: 0, dropoff_pct: 0 }
              // 1. Cast to Number (guards null / undefined / string from bad data)
              // 2. Clamp to [0, 100] (guards backend rounding overflow)
              // 3. Round to 1 decimal so bar width matches the displayed label exactly
              const barWidth = Math.round(Math.max(0, Math.min(100, Number(stepData.conversion_pct) || 0)) * 10) / 10
              // 2px minimum so tiny values (e.g. 0.1%) are still visible
              const minWidthPx = barWidth > 0 ? 2 : 0
              return (
                <div
                  key={cohort.cohort_id}
                  className="funnel-bar-row"
                  data-testid={`funnel-bar-${cohort.cohort_id}-${stepIdx}`}
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
                        width: `${barWidth}%`,
                        background: getCohortColor(cohort.cohort_id, cohortIdx),
                        minWidth: `${minWidthPx}px`,
                      }}
                    />
                  </div>
                  <div className="funnel-bar-meta">
                    <span className="funnel-bar-users">{formatInteger(stepData.users)}</span>
                    <span className="funnel-bar-pct">({(Number(stepData.conversion_pct) || 0).toFixed(1)}%)</span>
                    {stepIdx > 0 && (Number(stepData.dropoff_pct) || 0) > 0 && (
                      <span className="funnel-bar-dropoff" title="Drop-off from previous step">
                        ↓{(Number(stepData.dropoff_pct) || 0).toFixed(1)}%
                      </span>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Table view
// ---------------------------------------------------------------------------

function FunnelTable({ result }) {
  if (!result || !result.results || result.results.length === 0) return null

  // Issue #8: show message if all cohorts have 0 users at every step
  const hasAnyUsers = result.results.some(c => c.steps.some(s => s.users > 0))

  return (
    <div className="funnel-table-section" data-testid="funnel-table">
      {!hasAnyUsers && (
        <p className="funnel-no-users-msg" data-testid="funnel-no-users">
          No users completed this funnel across any cohort.
        </p>
      )}
      {result.results.map((cohort, cohortIdx) => (
        <div key={cohort.cohort_id} className="funnel-table-cohort">
          <h4 style={{ color: getCohortColor(cohort.cohort_id, cohortIdx) }}>
            {cohort.cohort_name}
          </h4>
          <div className="table-responsive">
            <table>
              <thead>
                <tr>
                  <th>Step</th>
                  <th>Event</th>
                  <th style={{ textAlign: 'right' }}>Users</th>
                  <th style={{ textAlign: 'right' }}>Conversion %</th>
                  <th style={{ textAlign: 'right' }}>Drop-off %</th>
                </tr>
              </thead>
              <tbody>
                {cohort.steps.map((step, stepIdx) => (
                  <tr key={stepIdx} data-testid={`funnel-table-row-${cohort.cohort_id}-${stepIdx}`}>
                    <td>{stepIdx + 1}</td>
                    <td>{step.event_name}</td>
                    <td style={{ textAlign: 'right' }}>{formatInteger(step.users)}</td>
                    <td style={{ textAlign: 'right' }}>
                      {(Number(step.conversion_pct) || 0).toFixed(1)}%
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {stepIdx === 0 ? '—' : `${(Number(step.dropoff_pct) || 0).toFixed(1)}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Funnel selector  (Issue #10: valid first, invalid greyed out + disabled)
// ---------------------------------------------------------------------------

function FunnelSelector({ funnels, value, onChange }) {
  const valid = funnels.filter(f => f.is_valid)
  const invalid = funnels.filter(f => !f.is_valid)

  return (
    <div className="funnel-selector" data-testid="funnel-selector">
      <select
        value={value ?? ''}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
        data-testid="funnel-select"
      >
        <option value="">— Select Funnel —</option>
        {valid.length > 0 && (
          <optgroup label="Valid Funnels">
            {valid.map(f => (
              <option key={f.id} value={f.id}>{f.name}</option>
            ))}
          </optgroup>
        )}
        {invalid.length > 0 && (
          <optgroup label="Invalid for Current Dataset">
            {invalid.map(f => (
              <option key={f.id} value={f.id} disabled className="funnel-option-invalid">
                {f.name} (invalid)
              </option>
            ))}
          </optgroup>
        )}
      </select>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main FunnelPane
// ---------------------------------------------------------------------------

export default function FunnelPane({ refreshToken, events }) {
  // Issue #1: safe-guard against undefined/null events prop
  const safeEvents = Array.isArray(events) ? events : []

  const [funnels, setFunnels] = useState([])
  const [selectedFunnelId, setSelectedFunnelId] = useState(null)
  const [result, setResult] = useState(null)
  // Issue #6: running flag disables the button and prevents duplicate runs
  const [running, setRunning] = useState(false)
  const [runError, setRunError] = useState('')
  const [showBuilder, setShowBuilder] = useState(false)
  const [deleting, setDeleting] = useState(null)
  // Guard against stale-response races
  const runIdRef = useRef(0)

  const loadFunnels = useCallback(async () => {
    try {
      const data = await listFunnels()
      setFunnels(data.funnels || [])
    } catch {
      setFunnels([])
    }
  }, [])

  useEffect(() => {
    loadFunnels()
  }, [loadFunnels, refreshToken])

  // Auto-select first valid funnel; Issue #10: if selected becomes invalid, keep showing notice
  useEffect(() => {
    if (selectedFunnelId != null) return
    const first = funnels.find(f => f.is_valid)
    if (first) setSelectedFunnelId(first.id)
  }, [funnels, selectedFunnelId])

  // Issue #6: prevent duplicate runs — ignore if already running
  const handleRun = async () => {
    if (!selectedFunnelId || running) return
    const thisRunId = ++runIdRef.current
    setRunning(true)
    setRunError('')
    setResult(null)
    try {
      const data = await runFunnel(selectedFunnelId)
      // Only apply if this is still the latest run
      if (runIdRef.current === thisRunId) {
        setResult(data)
      }
    } catch (err) {
      if (runIdRef.current === thisRunId) {
        setRunError(err.message || 'Failed to run funnel')
      }
    } finally {
      if (runIdRef.current === thisRunId) {
        setRunning(false)
      }
    }
  }

  const handleDelete = async (funnelId) => {
    setDeleting(funnelId)
    try {
      await deleteFunnel(funnelId)
      if (selectedFunnelId === funnelId) {
        setSelectedFunnelId(null)
        setResult(null)
      }
      await loadFunnels()
    } catch {
      // ignore
    } finally {
      setDeleting(null)
    }
  }

  const selectedFunnel = funnels.find(f => f.id === selectedFunnelId)
  // Issue #10: if selected funnel is invalid (e.g. dataset changed), show warning + disable run
  const isSelectedValid = selectedFunnel?.is_valid ?? false

  return (
    <div className="funnel-pane" data-testid="funnel-pane">
      {/* Top bar */}
      <div className="funnel-topbar">
        <button
          className="button button-primary funnel-new-btn"
          onClick={() => setShowBuilder(true)}
          data-testid="funnel-new-button"
        >
          + New Funnel
        </button>

        <FunnelSelector
          funnels={funnels}
          value={selectedFunnelId}
          onChange={id => {
            setSelectedFunnelId(id)
            setResult(null)
            setRunError('')
          }}
        />

        {selectedFunnelId && (
          <button
            className="button button-secondary funnel-delete-btn"
            onClick={() => handleDelete(selectedFunnelId)}
            disabled={deleting === selectedFunnelId || running}
            data-testid="funnel-delete-button"
            title="Delete selected funnel"
          >
            {deleting === selectedFunnelId ? 'Deleting…' : 'Delete'}
          </button>
        )}

        {/* Issue #6: disabled while running; Issue #10: disabled when invalid */}
        <button
          className="button button-primary"
          onClick={handleRun}
          disabled={!selectedFunnelId || !isSelectedValid || running}
          data-testid="funnel-run-button"
          title={
            !selectedFunnel
              ? 'Select a funnel first'
              : !isSelectedValid
                ? 'This funnel is not applicable to the current dataset (missing events or properties)'
                : running
                  ? 'Computing…'
                  : 'Run Funnel'
          }
        >
          {running ? 'Running…' : 'Run Funnel'}
        </button>
      </div>

      {/* Issue #10: Invalid funnel warning notice */}
      {selectedFunnel && !isSelectedValid && (
        <div className="funnel-invalid-notice" data-testid="funnel-invalid-notice">
          ⚠️ This funnel is not applicable to the current dataset — some events or property columns are missing.
        </div>
      )}

      {/* Run error */}
      {runError && (
        <p className="error" data-testid="funnel-run-error">{runError}</p>
      )}

      {/* Empty states */}
      {!result && !running && funnels.length === 0 && (
        <div className="funnel-empty-state" data-testid="funnel-empty-state">
          <div className="funnel-empty-icon">⬇</div>
          <p>No funnels yet. Click <strong>+ New Funnel</strong> to create your first conversion funnel.</p>
        </div>
      )}

      {!result && !running && funnels.length > 0 && !selectedFunnelId && (
        <div className="funnel-empty-state">
          <p>Select a funnel above and click <strong>Run Funnel</strong> to see conversion results.</p>
        </div>
      )}

      {/* Loading indicator */}
      {running && (
        <div className="funnel-loading" data-testid="funnel-loading">
          <div className="funnel-spinner" />
          <span>Computing funnel…</span>
        </div>
      )}

      {/* Chart + table */}
      {result && !running && (
        <div className="funnel-results" data-testid="funnel-results">
          <h3 className="funnel-results-title">{result.funnel_name}</h3>
          <FunnelChart result={result} />
          <div className="funnel-divider" />
          <FunnelTable result={result} />
        </div>
      )}

      {/* Builder modal — passes plain string[] events (Issue #1) */}
      <FunnelBuilderModal
        events={safeEvents}
        isOpen={showBuilder}
        onClose={() => setShowBuilder(false)}
        onCreated={loadFunnels}
      />
    </div>
  )
}
