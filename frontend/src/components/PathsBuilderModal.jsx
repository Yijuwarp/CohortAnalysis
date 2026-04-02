import { useState, useEffect, useCallback, useRef } from 'react'
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

const EMPTY_STEP = (id) => ({ id, event_name: '', filters: [] })
const EMPTY_FILTER = () => ({ property_key: '', property_value: '' })

export default function PathsBuilderModal({ events, isOpen, onClose, onSaved, editingPath }) {
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
          if (!s.event_name) continue
          try {
            const pData = await getEventProperties(s.event_name)
            newProps[s.event_name] = pData.properties || []
            for (const f of s.filters) {
              if (!f.property_key) continue
              const ck = `${s.event_name}::${f.property_key}`
              const vData = await getEventPropertyValues(s.event_name, f.property_key, 50)
              newValues[ck] = vData.values || []
            }
          } catch {}
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

  const updateStep = (idx, field, value) => {
    setSteps(prev => prev.map((s, i) => i === idx ? { ...s, [field]: value, filters: field === 'event_name' ? [] : s.filters } : s))
    if (field === 'event_name' && value) {
      loadProps(value)
    }
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
    if (!trimmedName) { setError('Path name is required'); return }
    if (steps.length < MIN_PATH_STEPS) { setError(`At least ${MIN_PATH_STEPS} steps required`); return }
    
    for (let i = 0; i < steps.length; i++) {
        if (!steps[i].event_name) { setError(`Step ${i+1}: Event is required`); return }
    }

    setSaving(true)
    try {
      let finalGap = useCustomGap ? parseInt(customGapValue, 10) : maxStepGapMinutes
      if (useCustomGap) {
        if (customGapUnit === 'hours') finalGap *= 60
        if (customGapUnit === 'days') finalGap *= 1440
      }
      if (useCustomGap && (isNaN(finalGap) || finalGap <= 0)) {
        setError('Please enter a valid custom time gap')
        setSaving(false)
        return
      }

      const payload = {
        name: trimmedName,
        max_step_gap_minutes: finalGap,
        steps: steps.map((s, idx) => ({
          event_name: s.event_name,
          step_order: idx,
          filters: s.filters.filter(f => f.property_key && f.property_value).map(f => {
            // Check if value is numeric for the backend (optional, backend can also handle Union)
            let val = f.property_value
            if (val !== '' && !isNaN(val)) {
                val = val.includes('.') ? parseFloat(val) : parseInt(val, 10)
            }
            return { property_key: f.property_key, property_value: val }
          })
        }))
      }
      let result
      if (editingPath?.id) {
        result = await updatePath(editingPath.id, payload)
      } else {
        result = await createPath(payload)
      }
      onSaved(result)
      onClose()
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
          <h2>{editingPath ? 'Edit Path' : 'Create New Path'}</h2>
          <button className="funnel-modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="funnel-modal-body">
          <div className="funnel-name-field">
            <label>Path Name</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="e.g. Purchase Sequence"
              maxLength={120}
            />
          </div>

          <div className="funnel-name-field">
            <label>Max time between steps</label>
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <select
                value={useCustomGap ? 'custom' : (maxStepGapMinutes === null ? 'null' : String(maxStepGapMinutes))}
                onChange={e => {
                  const val = e.target.value
                  if (val === 'custom') {
                    setUseCustomGap(true)
                  } else {
                    setUseCustomGap(false)
                    setMaxStepGapMinutes(val === 'null' ? null : parseInt(val, 10))
                  }
                }}
                className="funnel-select"
                style={{ width: useCustomGap ? '120px' : '100%' }}
              >
                <option value="null">Unlimited</option>
                <option value="10">10 minutes</option>
                <option value="60">1 hour</option>
                <option value="1440">1 day</option>
                <option value="custom">Custom...</option>
              </select>

              {useCustomGap && (
                <>
                  <input
                    type="number"
                    value={customGapValue}
                    onChange={e => setCustomGapValue(e.target.value)}
                    placeholder="Value"
                    style={{ width: '80px', padding: '6px' }}
                    min="1"
                  />
                  <select
                    value={customGapUnit}
                    onChange={e => setCustomGapUnit(e.target.value)}
                    className="funnel-select"
                    style={{ width: '100px' }}
                  >
                    <option value="minutes">minutes</option>
                    <option value="hours">hours</option>
                    <option value="days">days</option>
                  </select>
                </>
              )}
            </div>
            <p className="pane-section-hint">Each step must occur within this window of the previous step.</p>
          </div>

          <div className="funnel-steps-section">
            <p className="funnel-dnd-help">Drag steps to reorder sequence</p>
            <p className="funnel-steps-label">Steps (2–10 required)</p>

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
                          className={`funnel-step-block${isDragging ? ' is-dragging' : ''}`} 
                          key={`step-${step.id}`}
                        >
                          <div className="funnel-step-header" onPointerDown={e => e.stopPropagation()}>
                            <span className="funnel-step-number">{idx + 1}</span>
                            <button className="funnel-drag-handle" {...attributes} {...listeners}>⋮⋮</button>
                            
                            <div style={{ flex: 1, minWidth: '160px' }}>
                                <SearchableSelect
                                    options={safeEvents}
                                    value={step.event_name}
                                    onChange={val => updateStep(idx, 'event_name', val)}
                                    placeholder="— select event —"
                                />
                            </div>

                            {steps.length > MIN_PATH_STEPS && (
                              <button className="funnel-remove-step" onClick={() => removeStep(idx)}>Remove</button>
                            )}
                          </div>

                          {step.filters.map((f, fIdx) => (
                            <div key={`filter-${idx}-${fIdx}`} className="funnel-filter-row">
                              <span className="funnel-filter-where">where</span>
                              <div style={{ width: '220px' }}>
                                <SearchableSelect
                                  options={propsByEvent[step.event_name] || []}
                                  value={f.property_key}
                                  onChange={val => updateFilter(idx, fIdx, 'property_key', val)}
                                  placeholder="— property —"
                                />
                              </div>
                              <span className="funnel-filter-equals">＝</span>
                              
                              <div style={{ flex: 1, minWidth: '140px' }}>
                                <SearchableSelect
                                  options={valuesByProp[`${step.event_name}::${f.property_key}`] || []}
                                  value={f.property_value}
                                  onChange={val => updateFilter(idx, fIdx, 'property_value', val)}
                                  placeholder="value"
                                />
                              </div>

                              <button className="funnel-remove-filter" onClick={() => removeFilter(idx, fIdx)}>✕</button>
                            </div>
                          ))}

                          <div onPointerDown={e => e.stopPropagation()} style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                            <button
                              className="funnel-add-filter-btn"
                              onClick={() => addFilter(idx)}
                              disabled={!step.event_name}
                            >+ Add filter</button>
                          </div>
                        </div>
                      )}
                    </SortableStep>
                  ))}
                </SortableContext>
                <DragOverlay>
                    {activeId ? (
                        <div className="drag-overlay">
                            {steps.find(s => s.id === activeId)?.event_name || 'Step'}
                        </div>
                    ) : null}
                </DragOverlay>
              </DndContext>
              
              {steps.length < MAX_PATH_STEPS && (
                <button className="funnel-add-step-btn" onClick={addStep} style={{ marginTop: '12px' }}>+ Add Step</button>
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
            {saving ? 'Saving…' : 'Save Path'}
          </button>
        </div>
      </div>
      <style>{`
        .modal-paths .funnel-modal { max-width: 800px; }
        .funnel-filter-row { display: flex; align-items: center; gap: 8px; margin-top: 8px; }
        
        /* Ensure dropdowns are not clipped and stay above everything */
        .funnel-steps-scroll { padding-bottom: 240px !important; overflow-y: auto !important; overflow-x: visible !important; position: relative; }
        .funnel-step-block { position: relative; transition: all 0.2s ease; background: #fafafa; }
        .funnel-step-block.is-dragging { 
          z-index: 1000 !important; 
          transform: scale(1.02); 
          box-shadow: 0 10px 25px rgba(0,0,0,0.15);
          cursor: grabbing;
        }
        .funnel-step-block:focus-within { z-index: 100 !important; }
        .searchable-select-dropdown { z-index: 10002 !important; transform: translateZ(0); }
        .funnel-modal-footer { z-index: 5; position: relative; background: #f9fafb; }
        .funnel-select { padding: 8px; border: 1px solid #ddd; border-radius: 4px; background: white; font-size: 14px; }
      `}</style>
    </div>
  )
}
