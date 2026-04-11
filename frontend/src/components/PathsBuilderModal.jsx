import React, { useState, useEffect, useCallback, useRef, Fragment } from 'react'
import { createPath, updatePath, getEventProperties, getEventPropertyValues } from '../api'
import SearchableSelect from './SearchableSelect'
import { DndContext, PointerSensor, useSensor, useSensors, closestCenter, DragOverlay } from '@dnd-kit/core'
import { SortableContext, useSortable, verticalListSortingStrategy, arrayMove } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

// ---------------------------------------------------------------------------
// Sortable Step Component
// ---------------------------------------------------------------------------

function SortableStep({ id, isDragOver, children }) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id })
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.4 : 1,
  }
  return (
    <div ref={setNodeRef} style={style} className={`funnel-sortable-step${isDragOver ? ' drag-over' : ''}`}>
      {children({ attributes, listeners, isDragging })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Paths Builder Modal
// ---------------------------------------------------------------------------

const MAX_PATH_STEPS = 10
const MIN_PATH_STEPS = 2
const MAX_GROUPS_PER_STEP = 3

const EMPTY_GROUP = () => ({ event_name: '', filters: [] })
const EMPTY_STEP = (id) => ({ id, groups: [EMPTY_GROUP()] })
const EMPTY_FILTER = () => ({ property_key: '', property_value: '' })

export default function PathsBuilderModal({ events, isOpen, onClose, onSaved, editingPath, mode = 'create' }) {
  const safeEvents = Array.isArray(events) ? events : []

  const [name, setName] = useState('')
  const nextStepIdRef = useRef(1)
  const [steps, setSteps] = useState([EMPTY_STEP(nextStepIdRef.current++), EMPTY_STEP(nextStepIdRef.current++)])
  const [activeId, setActiveId] = useState(null)
  const [overId, setOverId] = useState(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  
  const [maxStepGapMinutes, setMaxStepGapMinutes] = useState(null)
  const [useCustomGap, setUseCustomGap] = useState(false)
  const [customGapValue, setCustomGapValue] = useState('')
  const [customGapUnit, setCustomGapUnit] = useState('minutes')
  
  const [propsByEvent, setPropsByEvent] = useState({})
  const [valuesByProp, setValuesByProp] = useState({})
  const sensors = useSensors(
    useSensor(PointerSensor, {
        activationConstraint: {
            distance: 8,
        },
    })
  )

  const loadProps = useCallback(async (eventName) => {
    if (!eventName || propsByEvent[eventName] !== undefined) return
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

  useEffect(() => {
    if (!isOpen) return
    setError('')
    setSaving(false)

    if (editingPath && editingPath.steps) {
      setName(editingPath.name)
      const gap = editingPath.max_step_gap_minutes
      setMaxStepGapMinutes(gap)
      if (gap !== null && gap !== 10 && gap !== 60 && gap !== 1440) {
        setUseCustomGap(true)
        if (gap % 1440 === 0) {
          setCustomGapValue(gap / 1440)
          setCustomGapUnit('days')
        } else if (gap % 60 === 0) {
          setCustomGapValue(gap / 60)
          setCustomGapUnit('hours')
        } else {
          setCustomGapValue(gap)
          setCustomGapUnit('minutes')
        }
      } else {
        setUseCustomGap(false)
        setCustomGapValue('')
        setCustomGapUnit('minutes')
      }

      // Hydrate
      const hydrate = async () => {
        const hSteps = editingPath.steps.map(s => ({
          ...s,
          id: nextStepIdRef.current++
        }))
        setSteps(hSteps)
        
        const newProps = {}
        const newValues = {}
        for (const s of hSteps) {
          for (const g of s.groups) {
            if (!g.event_name) continue
            try {
              const pData = await getEventProperties(g.event_name)
              newProps[g.event_name] = pData.properties || []
              for (const f of g.filters) {
                if (!f.property_key) continue
                const ck = `${g.event_name}::${f.property_key}`
                const vData = await getEventPropertyValues(g.event_name, f.property_key, 50)
                newValues[ck] = vData.values || []
              }
            } catch {}
          }
        }
        setPropsByEvent(prev => ({ ...prev, ...newProps }))
        setValuesByProp(prev => ({ ...prev, ...newValues }))
      }
      hydrate()
    } else {
      setName('')
      setMaxStepGapMinutes(null)
      setUseCustomGap(false)
      setCustomGapValue('')
      setCustomGapUnit('minutes')
      nextStepIdRef.current = 1
      setSteps([EMPTY_STEP(nextStepIdRef.current++), EMPTY_STEP(nextStepIdRef.current++)])
      setPropsByEvent({})
      setValuesByProp({})
    }
  }, [isOpen, editingPath])

  if (!isOpen) return null

  const updateGroupEvent = (stepIdx, groupIdx, value) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx) return s
      const newGroups = s.groups.map((g, gi) => 
        gi === groupIdx ? { ...g, event_name: value, filters: [] } : g
      )
      return { ...s, groups: newGroups }
    }))
    if (value) loadProps(value)
  }

  const addGroup = (stepIdx) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx || s.groups.length >= MAX_GROUPS_PER_STEP) return s
      return { ...s, groups: [...s.groups, EMPTY_GROUP()] }
    }))
  }

  const removeGroup = (stepIdx, groupIdx) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx || s.groups.length <= 1) return s
      return { ...s, groups: s.groups.filter((_, gi) => gi !== groupIdx) }
    }))
  }

  const addStep = () => {
    if (steps.length >= MAX_PATH_STEPS) return
    setSteps(prev => [...prev, { ...EMPTY_STEP(nextStepIdRef.current++), isNew: true }])
  }

  const removeStep = (idx) => {
    if (steps.length <= MIN_PATH_STEPS) return
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
      return arrayMove(prev, oldIndex, newIndex)
    })
  }

  const addFilter = (stepIdx, groupIdx) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx) return s
      const newGroups = s.groups.map((g, gi) => 
        gi === groupIdx ? { ...g, filters: [...g.filters, EMPTY_FILTER()] } : g
      )
      return { ...s, groups: newGroups }
    }))
  }

  const removeFilter = (stepIdx, groupIdx, filterIdx) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx) return s
      const newGroups = s.groups.map((g, gi) => 
        gi === groupIdx ? { ...g, filters: g.filters.filter((_, fi) => fi !== filterIdx) } : g
      )
      return { ...s, groups: newGroups }
    }))
  }

  const updateFilter = (stepIdx, groupIdx, filterIdx, field, value) => {
    setSteps(prev => prev.map((s, i) => {
      if (i !== stepIdx) return s
      const newGroups = s.groups.map((g, gi) => {
        if (gi !== groupIdx) return g
        const newFilters = g.filters.map((f, fi) =>
          fi !== filterIdx ? f : { ...f, [field]: value, ...(field === 'property_key' ? { property_value: '' } : {}) }
        )
        if (field === 'property_key' && value) {
          loadValues(g.event_name, value)
        }
        return { ...g, filters: newFilters }
      })
      return { ...s, groups: newGroups }
    }))
  }

  const handleSave = async () => {
    setError('')
    const trimmedName = name.trim()
    if (!trimmedName) { setError('Path name is required'); return }
    if (steps.length < MIN_PATH_STEPS) { setError(`At least ${MIN_PATH_STEPS} steps required`); return }
    
    for (let i = 0; i < steps.length; i++) {
        const step = steps[i]
        if (step.groups.length === 0) { setError(`Step ${i+1} must have at least one event`); return }
        for (let j = 0; j < step.groups.length; j++) {
            if (!step.groups[j].event_name) { setError(`Step ${i+1}, Alternative ${j+1}: Event is required`); return }
        }
    }

    setSaving(true)
    try {
      let finalGap = useCustomGap ? parseInt(customGapValue, 10) : maxStepGapMinutes
      if (useCustomGap) {
        if (customGapUnit === 'hours') finalGap *= 60
        if (customGapUnit === 'days') finalGap *= 1440
      }
      if (useCustomGap && (isNaN(finalGap) || finalGap <= 0)) {
        setError('Please enter a valid custom time gap'); setSaving(false); return
      }

      const payload = {
        name: trimmedName,
        max_step_gap_minutes: finalGap,
        steps: steps.map((s, idx) => ({
          step_order: idx,
          groups: s.groups.map(g => ({
            event_name: g.event_name,
            filters: g.filters.filter(f => f.property_key && f.property_value).map(f => {
                let val = f.property_value
                if (val !== '' && !isNaN(val)) {
                    val = val.includes('.') ? parseFloat(val) : parseInt(val, 10)
                }
                return { property_key: f.property_key, property_value: val }
            })
          }))
        }))
      }
      let result
      if (mode === 'edit' && editingPath?.id) {
        result = await updatePath(editingPath.id, payload)
      } else {
        result = await createPath(payload)
      }
      onSaved(result); onClose()
    } catch (err) {
      setError(err.message || 'Failed to save path')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="funnel-modal-overlay" role="dialog" aria-modal="true">
      <div className="funnel-modal modal-paths">
        <div className="funnel-modal-header">
          <h2>{mode === 'edit' ? "Edit Path" : "Create New Path"}</h2>
          <button className="funnel-modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="funnel-modal-body">
          <div className="funnel-header-settings">
            <div className="funnel-name-field" style={{ flex: 1 }}>
              <label>Path Name</label>
              <input
                type="text"
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder="e.g. Purchase Sequence"
                maxLength={120}
              />
            </div>
            <div className="funnel-name-field" style={{ width: "240px" }}>
              <label>Max time between steps</label>
              <select
                value={useCustomGap ? "custom" : (maxStepGapMinutes === null ? "null" : String(maxStepGapMinutes))}
                onChange={e => {
                  const val = e.target.value
                  if (val === "custom") { setUseCustomGap(true) } 
                  else { setUseCustomGap(false); setMaxStepGapMinutes(val === "null" ? null : parseInt(val, 10)) }
                }}
                className="funnel-select"
              >
                <option value="null">Unlimited</option>
                <option value="10">10 minutes</option>
                <option value="60">1 hour</option>
                <option value="1440">1 day</option>
                <option value="custom">Custom...</option>
              </select>
            </div>
          </div>
          {useCustomGap && (
            <div style={{ display: "flex", gap: "8px", marginBottom: "16px", justifyContent: "flex-end" }}>
                <input
                    type="number"
                    value={customGapValue}
                    onChange={e => setCustomGapValue(e.target.value)}
                    placeholder="Value"
                    style={{ width: "80px", padding: "6px" }}
                    min="1"
                />
                <select
                    value={customGapUnit}
                    onChange={e => setCustomGapUnit(e.target.value)}
                    className="funnel-select"
                    style={{ width: "100px" }}
                >
                    <option value="minutes">minutes</option>
                    <option value="hours">hours</option>
                    <option value="days">days</option>
                </select>
            </div>
          )}

          <div className="funnel-steps-section">
            <p className="funnel-dnd-help">Drag steps to reorder sequence</p>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                <p className="funnel-steps-label" style={{ margin: 0 }}>Steps ({MIN_PATH_STEPS}–{MAX_PATH_STEPS} required)</p>
            </div>

            <div className="funnel-steps-scroll">
              <DndContext
                sensors={sensors}
                collisionDetection={closestCenter}
                onDragStart={({ active }) => setActiveId(active.id)}
                onDragOver={({ over }) => setOverId(over?.id ?? null)}
                onDragEnd={handleDragEnd}
              >
                <SortableContext items={steps.map(s => s.id)} strategy={verticalListSortingStrategy}>
                  {steps.map((step, idx) => (
                    <SortableStep key={step.id} id={step.id} isDragOver={overId === step.id}>
                      {({ attributes, listeners, isDragging }) => (
                        <div 
                          className={`funnel-step-block${isDragging ? " is-dragging" : ""}${step.groups.length > 1 ? " is-or-mode" : ""}`} 
                          key={`step-${step.id}`}
                        >
                          <div className="funnel-step-header" onPointerDown={e => e.stopPropagation()}>
                            <div className="step-badge">
                                <span className="funnel-step-number">{idx + 1}</span>
                            </div>
                            <button className="funnel-drag-handle" {...attributes} {...listeners} title="Drag to reorder">⋮⋮</button>
                            
                            <div className="step-title-row" style={{ flex: 1 }}>
                                {step.groups.length === 1 ? (
                                    <div style={{ flex: 1 }}>
                                        <SearchableSelect
                                            options={safeEvents}
                                            value={step.groups[0].event_name}
                                            onChange={val => updateGroupEvent(idx, 0, val)}
                                            placeholder="— select event —"
                                            column="event_name"
                                        />
                                    </div>
                                ) : (
                                    <span className="or-mode-label">Multi-Event Step (Any of these)</span>
                                )}
                            </div>

                            <div className="step-actions">
                                {step.groups.length < MAX_GROUPS_PER_STEP && (
                                    <button 
                                        className="btn-add-or" 
                                        onClick={() => addGroup(idx)}
                                        title="Add alternative event (OR logic)"
                                    >+ OR</button>
                                )}
                                {steps.length > MIN_PATH_STEPS && (
                                    <button className="funnel-remove-step" onClick={() => removeStep(idx)} title="Remove entire step">✕</button>
                                )}
                            </div>
                          </div>

                          <div className="step-content" onPointerDown={e => e.stopPropagation()}>
                            {step.groups.map((group, gIdx) => (
                                <Fragment key={`step-${idx}-group-${gIdx}`}>
                                    <div className={`group-block ${step.groups.length > 1 ? "group-or" : ""}`}>
                                        {step.groups.length > 1 && (
                                            <div className="group-header">
                                                <div className="group-title">
                                                    <SearchableSelect
                                                        options={safeEvents}
                                                        value={group.event_name}
                                                        onChange={val => updateGroupEvent(idx, gIdx, val)}
                                                        placeholder="— select alternative —"
                                                        column="event_name"
                                                    />
                                                </div>
                                                <button className="btn-remove-group" onClick={() => removeGroup(idx, gIdx)}>✕</button>
                                            </div>
                                        )}

                                        {group.filters.map((f, fIdx) => (
                                            <div key={`filter-${idx}-${gIdx}-${fIdx}`} className="funnel-filter-stack">
                                                <div className="filter-line">
                                                    <span className="funnel-filter-label">where</span>
                                                    <SearchableSelect
                                                        options={propsByEvent[group.event_name] || []}
                                                        value={f.property_key}
                                                        onChange={val => updateFilter(idx, gIdx, fIdx, "property_key", val)}
                                                        placeholder="— property —"
                                                        width="100%"
                                                    />
                                                </div>
                                                <div className="filter-line">
                                                    <span className="funnel-filter-label">＝</span>
                                                    <SearchableSelect
                                                        options={valuesByProp[`${group.event_name}::${f.property_key}`] || []}
                                                        value={f.property_value}
                                                        onChange={val => updateFilter(idx, gIdx, fIdx, "property_value", val)}
                                                        placeholder="value"
                                                        column={f.property_key}
                                                        eventName={group.event_name}
                                                        width="100%"
                                                    />
                                                    <button className="funnel-remove-filter" onClick={() => removeFilter(idx, gIdx, fIdx)}>✕</button>
                                                </div>
                                            </div>
                                        ))}

                                        <div style={{ marginTop: "8px" }}>
                                            <button
                                                className="funnel-add-filter-btn"
                                                onClick={() => addFilter(idx, gIdx)}
                                                disabled={!group.event_name}
                                            >+ Add filter</button>
                                        </div>
                                    </div>
                                    {gIdx < step.groups.length - 1 && (
                                        <div className="or-divider">
                                            <span className="or-chip">OR</span>
                                        </div>
                                    )}
                                </Fragment>
                            ))}
                          </div>
                        </div>
                      )}
                    </SortableStep>
                  ))}
                </SortableContext>
                <DragOverlay>
                    {activeId ? (
                        <div className="drag-overlay">
                            {steps.find(s => s.id === activeId)?.groups[0]?.event_name || "Step"}
                        </div>
                    ) : null}
                </DragOverlay>
              </DndContext>
              
              {steps.length < MAX_PATH_STEPS && (
                <button className="funnel-add-step-btn" onClick={addStep} style={{ marginTop: "12px" }}>+ Add Step</button>
              )}
            </div>
          </div>

          {error && <p className="error">{error}</p>}
        </div>

        <div className="funnel-modal-footer">
          <button className="button button-secondary" onClick={onClose} disabled={saving}>Cancel</button>
          <button
            className="button button-primary"
            onClick={handleSave}
            disabled={saving || steps.length < MIN_PATH_STEPS}
          >
            {saving ? "Saving…" : "Save Path"}
          </button>
        </div>
      </div>
      <style>{`
        .modal-paths .funnel-modal { max-width: 950px; }
        .funnel-header-settings { display: flex; gap: 24px; margin-bottom: 12px; align-items: flex-start; }
        
        .funnel-step-block { position: relative; margin-bottom: 24px; border: 1px solid #e0e0e0; border-radius: 8px; background: white; overflow: hidden; }
        .is-or-mode { border-color: #6366f1; box-shadow: 0 4px 12px rgba(99, 102, 241, 0.1); }
        
        .funnel-step-header { display: flex; align-items: center; gap: 12px; padding: 12px 16px; background: #f8fafc; border-bottom: 1px solid #e0e0e0; }
        .is-or-mode .funnel-step-header { background: #f5f3ff; border-bottom-color: #ddd6fe; }
        
        .step-badge { width: 28px; height: 28px; background: #64748b; color: #ffffff !important; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: 800; font-size: 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .is-or-mode .step-badge { background: #6366f1; color: #ffffff !important; }
        .funnel-step-number { color: #ffffff !important; }
        
        .funnel-drag-handle { background: none; border: none; font-size: 18px; color: #94a3b8; cursor: grab; padding: 4px; }
        .or-mode-label { font-weight: 600; color: #4338ca; }
        
        .step-actions { display: flex; gap: 8px; }
        .btn-add-or { background: #6366f1; color: white; border: none; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
        .btn-add-or:hover { background: #4f46e5; transform: translateY(-1px); }
        
        .step-content { padding: 16px; }
        .group-block { position: relative; transition: all 0.3s ease; }
        .group-or { background: #fff; border: 1px solid #e2e8f0; padding: 16px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
        .group-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px dashed #e2e8f0; }
        .group-title { flex: 1; max-width: 320px; }
        .btn-remove-group { background: #fee2e2; border: none; color: #ef4444; cursor: pointer; padding: 4px 8px; border-radius: 4px; font-size: 14px; transition: all 0.2s; }
        .btn-remove-group:hover { background: #fecaca; }

        .or-divider { position: relative; margin: 16px 0; text-align: center; }
        .or-divider::before { content: ''; position: absolute; left: 0; top: 50%; width: 100%; height: 1px; background: #e2e8f0; }
        .or-chip { position: relative; background: #6366f1; color: white; padding: 2px 14px; border-radius: 12px; font-size: 10px; font-weight: 800; text-transform: uppercase; letter-spacing: 1px; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }

        .funnel-filter-stack { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; padding: 12px; background: #f8fafc; border-radius: 6px; border: 1px solid #f1f5f9; }
        .filter-line { display: flex; align-items: center; gap: 8px; }
        .funnel-filter-label { font-size: 11px; color: #64748b; font-weight: 700; text-transform: uppercase; min-width: 44px; text-align: right; }
        .funnel-remove-filter { background: #fee2e2; border: none; color: #ef4444; border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 12px; transition: all 0.2s; }
        .funnel-remove-filter:hover { background: #fecaca; }

        .funnel-add-filter-btn { background: none; border: none; color: #6366f1; font-size: 12px; font-weight: 600; cursor: pointer; padding: 8px 0; display: inline-flex; align-items: center; gap: 4px; }
        .funnel-add-filter-btn:hover:not(:disabled) { color: #4f46e5; text-decoration: underline; }
        
        .funnel-steps-scroll { padding-bottom: 100px !important; overflow-y: auto !important; position: relative; min-height: 400px; }
        .funnel-step-block.is-dragging { z-index: 1000 !important; transform: scale(1.02); box-shadow: 0 10px 25px rgba(0,0,0,0.15); }
      `}</style>
    </div>
  )
}
