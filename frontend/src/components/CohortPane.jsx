import React, { Fragment, useEffect, useMemo, useState } from 'react'
import cloneDeep from 'lodash/cloneDeep'
import { createCohort, deleteCohort, listCohorts, toggleCohortHide, getSavedCohorts, getCohortDetail } from '../api'
import { formatCohortLogic, formatChildCohortTooltip } from '../utils/cohortUtils'
import CohortForm from './CohortForm'
import CohortSplitModal from './CohortSplitModal'
import SavedCohortsPanel from './SavedCohortsPanel'
import SearchableSelect from './SearchableSelect'

const formatCohortSize = (size) => {
  const numeric = Number(size || 0)
  if (numeric >= 1000000) {
    return `${(numeric / 1000000).toFixed(numeric >= 10000000 ? 0 : 1)}M`
  }
  if (numeric >= 1000) {
    return `${(numeric / 1000).toFixed(numeric >= 100000 ? 0 : 1)}K`
  }
  return String(numeric)
}


let cohortStateCache = {
  schema_hash: null,
  cohorts: null,
  savedCohorts: null
}

export default function CohortPane({ refreshToken, onCohortsChanged, datasetMetadata }) {
  const [cohorts, setCohorts] = useState([])
  const [pendingCohorts, setPendingCohorts] = useState([])
  const [savedCohorts, setSavedCohorts] = useState([])
  const [selectedCohortId, setSelectedCohortId] = useState(null)
  const [deletingId, setDeletingId] = useState(null)

  useEffect(() => {
    if (selectedCohortId && !savedCohorts.some(c => c.id === selectedCohortId)) {
      setSelectedCohortId(null)
    }
  }, [savedCohorts, selectedCohortId])

  const [splittingId, setSplittingId] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  
  // Modals state
  const [isFormOpen, setIsFormOpen] = useState(false)
  const [formMode, setFormMode] = useState('create_saved') // 'create_saved', 'edit_saved'
  const [editData, setEditData] = useState(null)
  const [isPanelOpen, setIsPanelOpen] = useState(false)
  const [activeTooltipId, setActiveTooltipId] = useState(null)
  const [splitModalCohort, setSplitModalCohort] = useState(null) // cohort object for split modal

  const loadData = async (forceOptions = {}) => {
    const currentSchemaHash = datasetMetadata?.schema_hash || datasetMetadata?.users || refreshToken;
    const lastFetchedSchemaHash = cohortStateCache?.schema_hash;

    if (!forceOptions.forceDb && currentSchemaHash === lastFetchedSchemaHash && cohortStateCache.cohorts) {
      setCohorts(cohortStateCache.cohorts);
      setSavedCohorts(cohortStateCache.savedCohorts);
      return;
    }

    setLoading(true)
    setError('')
    try {
      const [cohortsListRes, savedRes] = await Promise.all([
        listCohorts(),
        getSavedCohorts()
      ])
      
      const cohortsList = cohortsListRes.cohorts || []
      
      // Fetch full details for each cohort in the list to get conditions/definitions
      const detailedCohorts = await Promise.all(
        cohortsList.map(c => getCohortDetail(c.cohort_id).catch(err => {
          console.error(`Failed to fetch detail for cohort ${c.cohort_id}`, err)
          return c // Fallback to lightweight if detail fails
        }))
      )
      
      const enriched = detailedCohorts.map(c => ({
        ...c,
        isInvalid: Number(c.size) === 0
      }))

      setCohorts(enriched)
      setSavedCohorts(savedRes || [])
      
      cohortStateCache = {
        schema_hash: currentSchemaHash,
        cohorts: enriched,
        savedCohorts: savedRes || []
      }
    } catch (err) {
      console.error("Failed to load cohort data", err)
      setError("Failed to load cohort data")
      setCohorts([])
      setSavedCohorts([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
  }, [refreshToken])

  useEffect(() => {
    setActiveTooltipId(null)
  }, [cohorts])

  const handleToggleHide = async (cohortId) => {
    setError('')
    try {
      await toggleCohortHide(cohortId)
      await loadData({ forceDb: true })
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    }
  }

  const handleDelete = async (cohortId) => {
    setDeletingId(cohortId)
    setError('')
    try {
      await deleteCohort(cohortId)
      await loadData({ forceDb: true })
      onCohortsChanged()
    } catch (err) {
      setError(err.message || 'Failed to delete cohort')
    } finally {
      setDeletingId(null)
    }
  }


  const handleSplitToggle = async (cohort) => {
    if (cohort.has_splits) {
      // Remove existing split children
      const children = cohorts.filter(
        c => c.split_parent_cohort_id === cohort.cohort_id
      )
      try {
        setSplittingId(cohort.cohort_id)
        for (const child of children) {
          await deleteCohort(child.cohort_id)
        }
        await loadData({ forceDb: true })
        onCohortsChanged()
      } catch (err) {
        setError(err.message)
      } finally {
        setSplittingId(null)
      }
    } else {
      // Open split modal instead of hard-coded random split
      setSplitModalCohort(cohort)
    }
  }

  // Active Cohorts organization
  const parentCohorts = useMemo(
    () => cohorts.filter((cohort) => !cohort.split_parent_cohort_id),
    [cohorts]
  )

  const childCohortsByParent = useMemo(() => {
    const childrenMap = {}
    cohorts.forEach((cohort) => {
      if (!cohort.split_parent_cohort_id) {
        return
      }
      if (!childrenMap[cohort.split_parent_cohort_id]) {
        childrenMap[cohort.split_parent_cohort_id] = []
      }
      childrenMap[cohort.split_parent_cohort_id].push(cohort)
    })

    Object.keys(childrenMap).forEach((parentId) => {
      childrenMap[parentId].sort((a, b) => (a.split_group_index ?? 0) - (b.split_group_index ?? 0))
    })

    return childrenMap
  }, [cohorts])

  const parentIdsWithChildren = useMemo(() => {
    return new Set(
      cohorts
        .filter(c => c.split_parent_cohort_id)
        .map(c => c.split_parent_cohort_id)
    )
  }, [cohorts])

  const enrichedParentCohorts = useMemo(() => {
    return parentCohorts.map(c => ({
      ...c,
      has_splits: parentIdsWithChildren.has(c.cohort_id)
    }))
  }, [parentCohorts, parentIdsWithChildren])

  const parentDefinitionTooltips = useMemo(
    () => Object.fromEntries(parentCohorts.map((cohort) => [cohort.cohort_id, formatCohortLogic(cohort)])),
    [parentCohorts]
  )

  const childDefinitionTooltips = useMemo(() => {
    const map = {}
    Object.entries(childCohortsByParent).forEach(([parentId, children]) => {
      const parentCohort = parentCohorts.find(c => String(c.cohort_id) === String(parentId))
      const parentName = parentCohort?.cohort_name || parentCohort?.name || 'Parent'
      children.forEach((child) => {
        map[child.cohort_id] = formatChildCohortTooltip(child, parentName, children)
      })
    })
    return map
  }, [childCohortsByParent, parentCohorts])

  // Saved Cohorts Dropdown options
  const cohortOptions = useMemo(() => {
    return savedCohorts
      .filter((c) => {
        if (c.is_valid === false) return false
        const activeMatch = cohorts.find(ac => ac.source_saved_id === c.id || ac.cohort_name === c.name)
        if (activeMatch && activeMatch.isInvalid) return false
        return true
      })
      .map((c) => ({
        label: c.name,
        value: c.id,
      }))
  }, [savedCohorts, cohorts])

  const selectedSavedObj = savedCohorts.find((c) => c.id === selectedCohortId)
  const isSelectedInvalid = selectedSavedObj && (selectedSavedObj.is_valid === false)
  
  const handleAddCohort = async () => {
    if (!selectedCohortId || isSelectedInvalid) return
    setError('')
    const targetSaved = selectedSavedObj
    if (!targetSaved) return
    
    const isDuplicate = cohorts.some(c => c.cohort_name === targetSaved.name) ||
                        pendingCohorts.some(p => p.name === targetSaved.name)
    
    if (isDuplicate) {
        setError(`A cohort named "${targetSaved.name}" already exists or is pending.`)
        return
    }

    const payload = {
      name: targetSaved.name,
      logic_operator: targetSaved.definition.logic_operator,
      join_type: targetSaved.definition.join_type,
      conditions: targetSaved.definition.conditions,
      source_saved_id: targetSaved.id
    }
    
    const pendingId = `pending_${Date.now()}_${Math.random()}`
    const pendingItem = { id: pendingId, name: targetSaved.name, payload, status: 'creating', error: null }
    setPendingCohorts(prev => [...prev, pendingItem])

    try {
      await createCohort(payload)
      await loadData({ forceDb: true })
      setPendingCohorts(prev => prev.filter(p => p.id !== pendingId))
      onCohortsChanged()
    } catch (err) {
      setPendingCohorts(prev => prev.map(p => p.id === pendingId ? { ...p, status: 'error', error: err.message } : p))
    }
  }

  const handleRetryPending = async (pendingItem) => {
    setPendingCohorts(prev => prev.map(p => p.id === pendingItem.id ? { ...p, status: 'creating', error: null } : p))
    try {
      await createCohort(pendingItem.payload)
      await loadData({ forceDb: true })
      setPendingCohorts(prev => prev.filter(p => p.id !== pendingItem.id))
      onCohortsChanged()
    } catch (err) {
      setPendingCohorts(prev => prev.map(p => p.id === pendingItem.id ? { ...p, status: 'error', error: err.message } : p))
    }
  }

  const handleRemovePending = (id) => {
    setPendingCohorts(prev => prev.filter(p => p.id !== id))
  }

  const handleEditSaved = (id) => {
    const target = savedCohorts.find((c) => c.id === id)
    if (target) {
      setEditData(target)
      setFormMode('edit_saved')
      setIsFormOpen(true)
    }
  }

  const handleDuplicate = (cohort) => {
    if (!cohort?.definition) {
      setError('Cannot duplicate cohort: invalid definition')
      return
    }
    
    const def = cohort.definition || {}
    const duplicated = {
      name: `Copy of ${cohort.name || 'Untitled Cohort'}`,
      definition: {
        logic_operator: ['AND', 'OR'].includes(def.logic_operator) ? def.logic_operator : 'AND',
        join_type: ['condition_met', 'first_event'].includes(def.join_type) ? def.join_type : 'condition_met',
        conditions: Array.isArray(def.conditions) ? cloneDeep(def.conditions) : [],
      },
    }
    
    setError('')
    setEditData(duplicated)
    setFormMode('create_saved')
    
    // Safety timeout to prevent state batching/race issues
    setTimeout(() => {
      setIsFormOpen(true)
      setIsPanelOpen(false)
    }, 0)
  }

  const onFormSaved = () => {
    setIsFormOpen(false)
    loadData({ forceDb: true })
  }

  return (
    <section>
      <div className="cohorts-section-card create-cohorts-card">
        <h3>Add Cohort</h3>
        <p className="secondary-text">Select a saved cohort to materialize into the active dataset, or manage saved definitions globally.</p>
        <div className="cohort-picker-row">
          <div className="cohort-select">
            <SearchableSelect
              options={cohortOptions}
              value={savedCohorts.some(c => c.id === selectedCohortId) ? selectedCohortId : null}
              onChange={setSelectedCohortId}
              placeholder="Select Saved Cohort"
            />
          </div>
          <button 
            className="button button-primary add-cohort-btn" 
            onClick={handleAddCohort} 
            disabled={!selectedCohortId || isSelectedInvalid}
          >
            Add
          </button>
        </div>
        <div className="inline-controls" style={{ marginTop: '16px', gap: '12px' }}>
          <button className="button button-secondary" onClick={() => {
            setEditData(null)
            setFormMode('create_saved')
            setIsFormOpen(true)
          }}>+ New Cohort</button>
          
          <button className="button button-secondary" onClick={() => setIsPanelOpen(true)}>Manage</button>
        </div>
        
        {error && <p className="error" style={{marginTop: '12px'}}>{error}</p>}
      </div>

      <div className="cohorts-section-card existing-cohorts-card existing-cohorts-container">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
          <h3 style={{ margin: 0 }}>Existing Cohorts</h3>
          {loading && <span className="secondary-text" style={{ fontSize: '12px' }}>Loading...</span>}
        </div>
        
        {loading && cohorts.length === 0 && pendingCohorts.length === 0 ? (
          <p className="secondary-text">Loading cohorts...</p>
        ) : cohorts.length === 0 && pendingCohorts.length === 0 ? (
          <p className="secondary-text">No cohorts created yet.</p>
        ) : (
          <div className="existing-cohorts-list">
            <div className="existing-cohort-header existing-cohort-row">
              <div className="cohort-name">Name</div>
              <div className="cohort-size">Size</div>
              <div className="cohort-actions">Actions</div>
            </div>
            

            {enrichedParentCohorts.map((cohort) => {
              const isSystemCohort = cohort.cohort_name === 'All Users'
              const childCohorts = cohort.hidden ? [] : (childCohortsByParent[cohort.cohort_id] || [])
              const minSizeForSplit = Number(cohort.size || 0) >= 8
              const definitionTooltip = parentDefinitionTooltips[cohort.cohort_id]

              return (
                <Fragment key={cohort.cohort_id}>
                  <div className="existing-cohort-row cohort-row" title={cohort.isInvalid ? 'Invalid cohort (0 users)' : (cohort.is_active ? '' : 'No matching members under current filters')}>
                    <div className="cohort-name cohort-left">
                      <span title={cohort.cohort_name}>{cohort.cohort_name}</span>
                      {cohort.isInvalid && <span className="cohort-invalid-badge" style={{ background: '#fef2f2', color: '#991b1b', border: '1px solid #f87171', padding: '2px 6px', borderRadius: '4px', fontSize: '12px', marginLeft: '6px' }}>Invalid</span>}
                      {cohort.hidden && <span className="badge-hidden">Hidden</span>}
                      {!cohort.is_active && !cohort.isInvalid && <span className="badge-inactive">Inactive</span>}
                    </div>

                    <div className="cohort-size">{formatCohortSize(cohort.size)}</div>

                    <div className="cohort-actions">
                      <button 
                        className="cohort-icon-button info-icon" 
                        type="button" 
                        aria-label="View cohort definition" 
                        onMouseEnter={() => setActiveTooltipId(cohort.cohort_id)}
                        onMouseLeave={() => setActiveTooltipId(null)}
                        onClick={(e) => {
                          e.stopPropagation();
                          setActiveTooltipId(prev => prev === cohort.cohort_id ? null : cohort.cohort_id);
                        }}
                      >
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                          <circle cx="12" cy="12" r="10"/>
                          <path d="M12 16v-4"/>
                          <path d="M12 8h.01"/>
                        </svg>
                        {activeTooltipId === cohort.cohort_id && (
                          <div className="cohort-info-tooltip">
                            {definitionTooltip}
                          </div>
                        )}
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleSplitToggle(cohort)}
                        disabled={cohort.isInvalid || (!minSizeForSplit && !cohort.has_splits) || splittingId === cohort.cohort_id}
                        title={cohort.has_splits ? "Remove split" : "Split cohort"}
                      >
                        <span className={cohort.has_splits ? "split-active" : ""} style={{ display: 'inline-flex' }}>
                          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                            <circle cx="12" cy="6" r="1.5"/>
                            <circle cx="6" cy="18" r="1.5"/>
                            <circle cx="12" cy="18" r="1.5"/>
                            <circle cx="18" cy="18" r="1.5"/>
                            <path d="M12 7.5v6"/>
                            <path d="M12 13.5l-6 4.5"/>
                            <path d="M12 13.5l6 4.5"/>
                          </svg>
                        </span>
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleToggleHide(cohort.cohort_id)}
                        title={cohort.hidden ? 'Show cohort in charts' : 'Hide cohort from charts'}
                      >
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M1 12s4-6 11-6 11 6 11 6-4 6-11 6S1 12 1 12z"/>
                          <circle cx="12" cy="12" r="3"/>
                        </svg>
                      </button>

                      <button
                        className="cohort-icon-button"
                        onClick={() => handleEditSaved(cohort.source_saved_id)}
                        disabled={cohort.isInvalid || !cohort.source_saved_id}
                        title={
                          !cohort.source_saved_id
                            ? "Cannot edit (not linked to a saved cohort)"
                            : "Edit saved cohort definition"
                        }
                      >
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M12 20h9"/>
                          <path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/>
                        </svg>
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleDelete(cohort.cohort_id)}
                        disabled={deletingId === cohort.cohort_id || isSystemCohort}
                        title={isSystemCohort ? 'System cohort cannot be deleted' : 'Delete cohort'}
                      >
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M3 6h18"/>
                          <path d="M8 6v-2h8v2"/>
                          <path d="M19 6l-1 14H6L5 6"/>
                          <path d="M10 11v6"/>
                          <path d="M14 11v6"/>
                        </svg>
                      </button>
                    </div>
                  </div>

                  {childCohorts.map((child) => {
                    const isChildSystemCohort = child.cohort_name === 'All Users'
                    const childDefinitionTooltip = childDefinitionTooltips[child.cohort_id]
                    return (
                      <div key={child.cohort_id} className="existing-cohort-row cohort-row child" title={child.isInvalid ? 'Invalid cohort (0 users)' : (child.is_active ? '' : 'No matching members under current filters')}>
                        <div className="cohort-name cohort-left">
                          <span className="child-prefix">↳</span>
                          <span title={child.cohort_name}>{child.cohort_name}</span>
                          {child.isInvalid && <span className="cohort-invalid-badge" style={{ background: '#fef2f2', color: '#991b1b', border: '1px solid #f87171', padding: '2px 6px', borderRadius: '4px', fontSize: '12px', marginLeft: '6px' }}>Invalid</span>}
                          {child.hidden && <span className="badge-hidden">Hidden</span>}
                          {!child.is_active && !child.isInvalid && <span className="badge-inactive">Inactive</span>}
                        </div>

                        <div className="cohort-size">{formatCohortSize(child.size)}</div>

                        <div className="cohort-actions">
                          <button
                            className="cohort-icon-button info-icon"
                            type="button"
                            aria-label="View cohort definition"
                            onMouseEnter={() => setActiveTooltipId(child.cohort_id)}
                            onMouseLeave={() => setActiveTooltipId(null)}
                            onClick={(e) => {
                              e.stopPropagation();
                              setActiveTooltipId(prev => prev === child.cohort_id ? null : child.cohort_id);
                            }}
                          >
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                              <circle cx="12" cy="12" r="10"/>
                              <path d="M12 16v-4"/>
                              <path d="M12 8h.01"/>
                            </svg>
                            {activeTooltipId === child.cohort_id && (
                              <div className="cohort-info-tooltip">
                                {childDefinitionTooltip}
                              </div>
                            )}
                          </button>
                          <button
                            className="cohort-icon-button"
                            type="button"
                            onClick={() => handleDelete(child.cohort_id)}
                            disabled={deletingId === child.cohort_id || isChildSystemCohort}
                            title={isChildSystemCohort ? 'System cohort cannot be deleted' : 'Delete cohort'}
                          >
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                              <path d="M3 6h18"/>
                              <path d="M8 6v-2h8v2"/>
                              <path d="M19 6l-1 14H6L5 6"/>
                              <path d="M10 11v6"/>
                              <path d="M14 11v6"/>
                            </svg>
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </Fragment>
              )
            })}

            {pendingCohorts
              .filter(p => !cohorts.some(c => c.cohort_name === p.name))
              .map((pending) => (
              <div key={pending.id} className="existing-cohort-row cohort-row" style={{ opacity: pending.status === 'creating' ? 0.6 : 1, background: pending.status === 'error' ? '#fef2f2' : undefined }}>
                <div className="cohort-name cohort-left" style={{ color: pending.status === 'error' ? '#dc2626' : undefined, display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span>{pending.name}</span>
                  {pending.status === 'creating' && <span className="badge-inactive">Creating...</span>}
                  {pending.status === 'error' && <span className="badge-inactive" style={{ background: '#fca5a5', color: '#991b1b', border: '1px solid #f87171' }}>Failed</span>}
                </div>
                <div className="cohort-size" style={{ color: pending.status === 'error' ? '#dc2626' : undefined }}>
                  {pending.status === 'creating' ? '⏳' : '—'}
                </div>
                <div className="cohort-actions">
                  {pending.status === 'error' ? (
                    <>
                      <button className="button button-small" onClick={() => handleRetryPending(pending)} style={{ background: '#ef4444', color: 'white', marginRight: '4px', border: 'none' }}>Retry</button>
                      <button className="button button-small button-secondary" onClick={() => handleRemovePending(pending.id)}>Remove</button>
                    </>
                  ) : <span style={{ fontSize: '12px', color: '#9ca3af' }}>Processing...</span>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {isFormOpen && (
        <CohortForm
           mode={formMode}
           initialData={editData}
           onCancel={() => setIsFormOpen(false)}
           onSave={(shouldClose = true) => {
             if (shouldClose) setIsFormOpen(false)
             loadData({ forceDb: true })
             onCohortsChanged()
           }}
           refreshToken={refreshToken}
        />
      )}
      
      {isPanelOpen && (
        <SavedCohortsPanel
           savedCohorts={savedCohorts}
           cohorts={cohorts}
           onClose={() => setIsPanelOpen(false)}
           onDeleted={() => {
              loadData({ forceDb: true })
              onCohortsChanged()
           }}
           onEdit={handleEditSaved}
           onDuplicate={handleDuplicate}
        />
      )}

      {splitModalCohort && (
        <CohortSplitModal
          cohort={splitModalCohort}
          onClose={() => setSplitModalCohort(null)}
          onSplitDone={() => {
            loadData({ forceDb: true })
            onCohortsChanged()
          }}
        />
      )}
    </section>
  )
}
