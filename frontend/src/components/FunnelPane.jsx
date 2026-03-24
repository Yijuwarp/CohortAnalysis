import { useState, useEffect, useCallback, useRef } from 'react'
import { createFunnel, updateFunnel, listFunnels, deleteFunnel, runFunnel, getEventProperties, getEventPropertyValues } from '../api'
import { formatInteger } from '../utils/formatters'
import { getCohortColor } from '../utils/cohortColors'
import { DndContext, PointerSensor, useSensor, useSensors, closestCenter, DragOverlay } from '@dnd-kit/core'
import { SortableContext, useSortable, verticalListSortingStrategy, arrayMove } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

// ---------------------------------------------------------------------------
// Funnel Builder Modal
// ---------------------------------------------------------------------------

const MAX_FUNNEL_STEPS = 10
const MIN_FUNNEL_STEPS = 2
const STEP_WINDOW_NONE = 'none'
const STEP_WINDOW_CUSTOM = 'custom'

const EMPTY_STEP = (id) => ({ id, event_name: '', filters: [] })
const EMPTY_FILTER = () => ({ property_key: '', property_value: '' })

function SortableStep({ id, isDragOver, children }) {
  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id })
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  }
  return (
    <div ref={setNodeRef} style={style} className={`funnel-sortable-step${isDragOver ? ' drag-over' : ''}`}>
      {children({ attributes, listeners })}
    </div>
  )
}

function FunnelBuilderModal({ events, isOpen, onClose, onCreated, editingFunnel }) {
  // events is string[] from backend e.g. ["signup", "purchase"]
  const safeEvents = Array.isArray(events) ? events : []

  const [name, setName] = useState('')
  const nextStepIdRef = useRef(1)
  const [steps, setSteps] = useState([EMPTY_STEP(nextStepIdRef.current++), EMPTY_STEP(nextStepIdRef.current++)])
  const [conversionWindowMode, setConversionWindowMode] = useState(STEP_WINDOW_NONE)
  const [conversionWindowValue, setConversionWindowValue] = useState('10')
  const [activeId, setActiveId] = useState(null)
  const [overId, setOverId] = useState(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  // { eventName -> string[] } — property key lists per event
  const [propsByEvent, setPropsByEvent] = useState({})
  // { "eventName::propKey" -> string[] } — value lists per event+prop combo
  const [valuesByProp, setValuesByProp] = useState({})
  const sensors = useSensors(useSensor(PointerSensor))

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

  // Reset or Hydrate on open
  useEffect(() => {
    if (!isOpen) return
    setError('')
    setSaving(false)

    if (editingFunnel && editingFunnel.steps) {
      setName(editingFunnel.name)
      setConversionWindowMode(editingFunnel.conversion_window ? STEP_WINDOW_CUSTOM : STEP_WINDOW_NONE)
      setConversionWindowValue(String(editingFunnel.conversion_window?.value ?? 10))
      // Render steps without filters purely first while we load
      const strippedSteps = editingFunnel.steps.map(s => ({ ...s, id: nextStepIdRef.current++, filters: [] }))
      setSteps(strippedSteps)
      
      const hydrate = async () => {
        const newProps = {}
        const newValues = {}
        
        // Ensure events are handled.
        for (const s of editingFunnel.steps) {
          if (!s.event_name) continue
          
          try {
            const propData = await getEventProperties(s.event_name)
            newProps[s.event_name] = propData.properties || []
          } catch {
            newProps[s.event_name] = []
          }
          
          for (const f of s.filters) {
            if (!f.property_key) continue
            try {
              const cacheKey = `${s.event_name}::${f.property_key}`
              const valData = await getEventPropertyValues(s.event_name, f.property_key, 50)
              newValues[cacheKey] = valData.values || []
            } catch {
              newValues[`${s.event_name}::${f.property_key}`] = []
            }
          }
        }
        
        setPropsByEvent(prev => ({ ...prev, ...newProps }))
        setValuesByProp(prev => ({ ...prev, ...newValues }))
        // THEN apply filters
        setSteps(editingFunnel.steps.map(s => ({ ...s, id: nextStepIdRef.current++ })))
      }
      
      hydrate()
      
    } else {
      setName('')
      setConversionWindowMode(STEP_WINDOW_NONE)
      setConversionWindowValue('10')
      nextStepIdRef.current = 1
      setSteps([EMPTY_STEP(nextStepIdRef.current++), EMPTY_STEP(nextStepIdRef.current++)])
      setPropsByEvent({})
      setValuesByProp({})
    }
  }, [isOpen, editingFunnel])

  if (!isOpen) return null

  const updateStep = (idx, field, value) => {
    setSteps(prev => prev.map((s, i) => i === idx ? { ...s, [field]: value, filters: field === 'event_name' ? [] : s.filters } : s))
    if (field === 'event_name' && value) {
      loadProps(value)
    }
  }

  const addStep = () => {
    if (steps.length >= MAX_FUNNEL_STEPS) return
    setSteps(prev => [...prev, EMPTY_STEP(nextStepIdRef.current++)])
  }

  const removeStep = (idx) => {
    if (steps.length <= MIN_FUNNEL_STEPS) return
    setSteps(prev => prev.filter((_, i) => i !== idx))
  }

  const handleDragEnd = (event) => {
    const { active, over } = event
    setActiveId(null)
    setOverId(null)
    if (!over || active.id === over.id) return
    setSteps(prev => {
      const oldIndex = prev.findIndex(step => step.id === active.id)
      const newIndex = prev.findIndex(step => step.id === over.id)
      if (oldIndex < 0 || newIndex < 0) return prev
      return arrayMove(prev, oldIndex, newIndex)
    })
  }

  const activeStep = steps.find(step => step.id === activeId) || null

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
    if (steps.length < MIN_FUNNEL_STEPS || steps.length > MAX_FUNNEL_STEPS) {
      setError(`Funnels must have between ${MIN_FUNNEL_STEPS} and ${MAX_FUNNEL_STEPS} steps`)
      return
    }
    for (let i = 0; i < steps.length; i++) {
      if (!steps[i].event_name.trim()) { setError(`Step ${i + 1}: select an event`); return }
    }
    let conversionWindow = null
    if (conversionWindowMode === STEP_WINDOW_CUSTOM) {
      const numericValue = Number(conversionWindowValue)
      if (!Number.isInteger(numericValue) || numericValue <= 0) {
        setError('Conversion window must be a positive number of minutes')
        return
      }
      if (numericValue > 10080) {
        setError('Conversion window cannot exceed 7 days (10080 minutes)')
        return
      }
      conversionWindow = { value: numericValue, unit: 'minute' }
    }

    setSaving(true)
    try {
      const payload = {
        name: trimmedName,
        steps: steps.map((s, idx) => ({
          event_name: s.event_name,
          step_order: idx,
          filters: s.filters.filter(f => f.property_key && f.property_value),
        })),
        conversion_window: conversionWindow,
      }
      if (editingFunnel) {
        await updateFunnel(editingFunnel.id, payload)
      } else {
        await createFunnel(payload)
      }
      onCreated()
      onClose()
    } catch (err) {
      setError(err.message || 'Failed to save funnel')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="funnel-modal-overlay" role="dialog" aria-modal="true" aria-label={editingFunnel ? 'Edit Funnel' : 'Create Funnel'}>
      <div className="funnel-modal" data-testid="funnel-builder-modal">
        <div className="funnel-modal-header">
          <h2>{editingFunnel ? 'Edit Funnel' : 'Create New Funnel'}</h2>
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
            <div className="funnel-window-row">
              <label htmlFor="funnel-conversion-window-mode">Conversion Window</label>
              <select
                id="funnel-conversion-window-mode"
                value={conversionWindowMode}
                onChange={e => setConversionWindowMode(e.target.value)}
                data-testid="funnel-conversion-window-mode"
              >
                <option value={STEP_WINDOW_NONE}>None (lifetime)</option>
                <option value={STEP_WINDOW_CUSTOM}>Custom</option>
              </select>
              {conversionWindowMode === STEP_WINDOW_CUSTOM && (
                <div className="funnel-window-custom">
                  <input
                    type="number"
                    min="1"
                    value={conversionWindowValue}
                    onChange={e => setConversionWindowValue(e.target.value)}
                    data-testid="funnel-conversion-window-value"
                    placeholder="e.g. 10 (minutes)"
                  />
                  <select disabled data-testid="funnel-conversion-window-unit">
                    <option>minutes</option>
                  </select>
                </div>
              )}
            </div>
            <p className="funnel-window-help">
              {conversionWindowMode === STEP_WINDOW_NONE
                ? 'No time restriction between steps (lifetime conversion)'
                : 'Max time allowed between each step (e.g. 10 mins between Step 1 → Step 2)'}
            </p>
            <p className="funnel-dnd-help">Drag steps to reorder funnel</p>
            <p className="funnel-steps-label">Steps (2–10 required)</p>

            <div className="funnel-steps-scroll">
            <DndContext
              sensors={sensors}
              collisionDetection={closestCenter}
              onDragStart={({ active }) => setActiveId(active.id)}
              onDragOver={({ over }) => setOverId(over?.id ?? null)}
              onDragEnd={handleDragEnd}
              onDragCancel={() => {
                setActiveId(null)
                setOverId(null)
              }}
            >
            <SortableContext items={steps.map(s => s.id)} strategy={verticalListSortingStrategy}>
            {steps.map((step, stepIdx) => {
              const props = propsByEvent[step.event_name] || []
              return (
                <SortableStep key={step.id} id={step.id} isDragOver={overId === step.id}>
                {({ attributes, listeners }) => (
                <div className="funnel-step-block" data-testid={`funnel-step-${stepIdx}`}>
                  <div className="funnel-step-header">
                    <span className="funnel-step-number">{stepIdx + 1}</span>
                    <button
                      type="button"
                      className="funnel-drag-handle"
                      aria-label={`Drag step ${stepIdx + 1}`}
                      title="Drag to reorder"
                      {...attributes}
                      {...listeners}
                    >
                      ⋮⋮
                    </button>

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

                    {steps.length > MIN_FUNNEL_STEPS && (
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
                        key={`${step.id}-filter-${filterIdx}`}
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
                )}
                </SortableStep>
              )
            })}
            </SortableContext>
            <DragOverlay>
              {activeStep ? (
                <div className="drag-overlay">
                  {activeStep.event_name || 'Step'}
                </div>
              ) : null}
            </DragOverlay>
            </DndContext>
            </div>

            {steps.length < MAX_FUNNEL_STEPS && (
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
            {saving ? 'Saving…' : (editingFunnel ? 'Update Funnel' : 'Save Funnel')}
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
              const barWidth = Number(stepData.conversion_pct) || 0
              // 2px minimum so tiny values (e.g. 0.1%) are still visible
              const minWidthPx = barWidth > 0 ? '2px' : '0px'
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
                        width: `${Number(Math.min(100, barWidth).toFixed(1))}%`,
                        background: getCohortColor(cohort.cohort_id, cohortIdx),
                        minWidth: minWidthPx,
                      }}
                    />
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

export default function FunnelPane({ refreshToken, events, state, setState }) {
  // Issue #1: safe-guard against undefined/null events prop
  const safeEvents = Array.isArray(events) ? events : []

  const [funnels, setFunnels] = useState([])
  const [selectedFunnelId, setSelectedFunnelId] = useState(state?.selectedFunnelId || null)
  const [editingFunnel, setEditingFunnel] = useState(null)
  const [result, setResult] = useState(state?.result || null)
  // Issue #6: running flag disables the button and prevents duplicate runs
  const [running, setRunning] = useState(false)
  const [runError, setRunError] = useState('')

  useEffect(() => {
    setState({
      selectedFunnelId,
      result
    })
  }, [selectedFunnelId, result, setState])
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
          onClick={() => {
            setEditingFunnel(null)
            setShowBuilder(true)
          }}
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
          <>
            <button
              className="button button-secondary funnel-edit-btn"
              onClick={() => {
                setEditingFunnel(selectedFunnel)
                setShowBuilder(true)
              }}
              disabled={deleting === selectedFunnelId || running}
              data-testid="funnel-edit-button"
              title="Edit selected funnel"
            >
              Edit
            </button>
            <button
              className="button button-secondary funnel-delete-btn"
              onClick={() => handleDelete(selectedFunnelId)}
              disabled={deleting === selectedFunnelId || running}
              data-testid="funnel-delete-button"
              title="Delete selected funnel"
            >
              {deleting === selectedFunnelId ? 'Deleting…' : 'Delete'}
            </button>
          </>
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
        editingFunnel={editingFunnel}
      />
    </div>
  )
}
