import { Fragment, useEffect, useMemo, useState } from 'react'
import { createCohort, deleteCohort, listCohorts, randomSplitCohort, toggleCohortHide, getSavedCohorts } from '../api'
import CohortForm from './CohortForm'
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

const isMultiOperator = (operator) => operator === 'IN' || operator === 'NOT IN'

const formatPropertyFilter = (propertyFilter) => {
  if (!propertyFilter) {
    return ''
  }

  const formattedValues = Array.isArray(propertyFilter.values)
    ? propertyFilter.values.join(', ')
    : propertyFilter.values

  if (isMultiOperator(propertyFilter.operator)) {
    return ` WHERE ${propertyFilter.column} ${propertyFilter.operator} (${formattedValues})`
  }

  return ` WHERE ${propertyFilter.column} ${propertyFilter.operator} ${formattedValues}`
}

const describeJoinType = (joinType) => (joinType === 'first_event' ? 'Join on first event' : 'Join when condition is met')

const buildCohortDefinition = (cohort) => {
  const logic = cohort.condition_logic || cohort.logic_operator || 'AND'
  const conditionLines = (cohort.conditions || []).map((condition) => {
    const property = condition.property_filter ? formatPropertyFilter(condition.property_filter) : ''
    return `${condition.event_name} ≥ ${condition.min_event_count}${property}`
  })
  return [`Logic: ${logic}`, ...conditionLines, describeJoinType(cohort.join_type)].join(' • ')
}


export default function CohortPane({ refreshToken, onCohortsChanged }) {
  const [cohorts, setCohorts] = useState([])
  const [savedCohorts, setSavedCohorts] = useState([])
  const [selectedCohortId, setSelectedCohortId] = useState(null)
  const [deletingId, setDeletingId] = useState(null)
  const [splittingId, setSplittingId] = useState(null)
  const [error, setError] = useState('')
  
  // Modals state
  const [isFormOpen, setIsFormOpen] = useState(false)
  const [formMode, setFormMode] = useState('create_saved') // 'create_saved', 'edit_saved'
  const [editData, setEditData] = useState(null)
  const [isPanelOpen, setIsPanelOpen] = useState(false)

  const loadData = async () => {
    try {
      const [cohortsRes, savedRes] = await Promise.all([
        listCohorts(),
        getSavedCohorts()
      ])
      setCohorts(cohortsRes.cohorts || [])
      setSavedCohorts(savedRes || [])
    } catch {
      // Best effort
      setCohorts([])
      setSavedCohorts([])
    }
  }

  useEffect(() => {
    loadData()
  }, [refreshToken])

  const handleToggleHide = async (cohortId) => {
    setError('')
    try {
      await toggleCohortHide(cohortId)
      await loadData()
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
      setCohorts((prev) => prev.filter((cohort) => cohort.cohort_id !== cohortId))
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    } finally {
      setDeletingId(null)
    }
  }

  const handleRandomSplit = async (cohort) => {
    setError('')
    setSplittingId(cohort.cohort_id)
    try {
      await randomSplitCohort(cohort.cohort_id)
      await loadData()
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    } finally {
      setSplittingId(null)
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

  const parentDefinitionTooltips = useMemo(
    () => Object.fromEntries(parentCohorts.map((cohort) => [cohort.cohort_id, buildCohortDefinition(cohort)])),
    [parentCohorts]
  )

  const childDefinitionTooltips = useMemo(() => {
    const map = {}
    Object.values(childCohortsByParent).forEach((children) => {
      children.forEach((child) => {
        map[child.cohort_id] = buildCohortDefinition(child)
      })
    })
    return map
  }, [childCohortsByParent])

  // Saved Cohorts Dropdown options
  const cohortOptions = useMemo(() => {
    console.log("Saved cohorts:", savedCohorts)
    return savedCohorts.map((c) => ({
      label: c.name,
      value: c.id,
      disabled: c.is_valid === false
    }))
  }, [savedCohorts])

  const selectedSavedObj = savedCohorts.find((c) => c.id === selectedCohortId)
  const isSelectedInvalid = selectedSavedObj && (selectedSavedObj.is_valid === false)
  
  const handleAddCohort = async () => {
    if (!selectedCohortId || isSelectedInvalid) return
    setError('')
    const targetSaved = selectedSavedObj
    if (!targetSaved) return
    
    try {
      const payload = {
        name: targetSaved.name,
        logic_operator: targetSaved.definition.logic_operator,
        join_type: targetSaved.definition.join_type,
        conditions: targetSaved.definition.conditions,
        source_saved_id: targetSaved.id
      }
      await createCohort(payload)
      await loadData()
      onCohortsChanged()
    } catch (err) {
      setError(err.message)
    }
  }

  const handleEditSaved = (id) => {
    const target = savedCohorts.find((c) => c.id === id)
    if (target) {
      setEditData(target)
      setFormMode('edit_saved')
      setIsFormOpen(true)
    }
  }

  const onFormSaved = () => {
    setIsFormOpen(false)
    loadData()
  }

  return (
    <section>
      <div className="cohorts-section-card create-cohorts-card">
        <h3>Add Cohort</h3>
        <p className="secondary-text">Select a saved cohort to materialize into the active dataset, or manage saved definitions globally.</p>
        <div className="grid" style={{ display: 'flex', gap: '8px', alignItems: 'center', marginTop: '12px', marginBottom: '12px', overflow: 'visible' }}>
          <div style={{ flex: 1, position: 'relative' }}>
            <SearchableSelect
              options={cohortOptions}
              value={selectedCohortId}
              onChange={setSelectedCohortId}
              placeholder="Select Saved Cohort"
              style={{ flex: 1, minWidth: "200px" }}
            />
          </div>
          <button 
            className="button button-primary" 
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

      <div className="cohorts-section-card existing-cohorts-card">
        <h3>Existing Cohorts</h3>
        {cohorts.length === 0 ? (
          <p className="secondary-text">No cohorts created yet.</p>
        ) : (
          <div className="cohort-list-table">
            <div className="cohort-list-header cohort-list-row">
              <span>Name</span>
              <span>Size</span>
              <span>Actions</span>
            </div>
            {parentCohorts.map((cohort) => {
              const isSystemCohort = cohort.cohort_name === 'All Users'
              const childCohorts = cohort.hidden ? [] : (childCohortsByParent[cohort.cohort_id] || [])
              const minSizeForSplit = Number(cohort.size || 0) >= 8
              const definitionTooltip = parentDefinitionTooltips[cohort.cohort_id]

              return (
                <Fragment key={cohort.cohort_id}>
                  <div className="cohort-list-row cohort-row" title={cohort.is_active ? '' : 'No matching members under current filters'}>
                    <div className="cohort-list-name cohort-left">
                      <span>{cohort.cohort_name}</span>
                      {cohort.hidden && <span className="badge-hidden">Hidden</span>}
                      {!cohort.is_active && <span className="badge-inactive">Inactive</span>}
                    </div>

                    <span className="cohort-list-size">{formatCohortSize(cohort.size)}</span>

                    <div className="cohort-actions">
                      <button className="cohort-icon-button" type="button" aria-label="View cohort definition" title={definitionTooltip}>
                        ℹ
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleRandomSplit(cohort)}
                        disabled={!minSizeForSplit || splittingId === cohort.cohort_id}
                        title={minSizeForSplit ? 'Create random subsets from this cohort' : 'Minimum 8 users required'}
                      >
                        {splittingId === cohort.cohort_id ? '⏳' : '🎲'}
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleToggleHide(cohort.cohort_id)}
                        title={cohort.hidden ? 'Show cohort in charts' : 'Hide cohort from charts'}
                      >
                        👁
                      </button>

                      <button
                        className="cohort-icon-button"
                        type="button"
                        onClick={() => handleDelete(cohort.cohort_id)}
                        disabled={deletingId === cohort.cohort_id || isSystemCohort}
                        title={isSystemCohort ? 'System cohort cannot be deleted' : 'Delete cohort'}
                      >
                        {deletingId === cohort.cohort_id ? '⏳' : '🗑'}
                      </button>
                    </div>
                  </div>

                  {childCohorts.map((child) => {
                    const isChildSystemCohort = child.cohort_name === 'All Users'
                    const childDefinitionTooltip = childDefinitionTooltips[child.cohort_id]
                    return (
                      <div key={child.cohort_id} className="cohort-list-row cohort-row child" title={child.is_active ? '' : 'No matching members under current filters'}>
                        <div className="cohort-list-name cohort-left">
                          <span>{child.cohort_name}</span>
                          {child.hidden && <span className="badge-hidden">Hidden</span>}
                          {!child.is_active && <span className="badge-inactive">Inactive</span>}
                        </div>

                        <span className="cohort-list-size">{formatCohortSize(child.size)}</span>

                        <div className="cohort-actions">
                          <button className="cohort-icon-button" type="button" aria-label="View cohort definition" title={childDefinitionTooltip}>
                            ℹ
                          </button>
                          <button
                            className="cohort-icon-button"
                            type="button"
                            onClick={() => handleDelete(child.cohort_id)}
                            disabled={deletingId === child.cohort_id || isChildSystemCohort}
                            title={isChildSystemCohort ? 'System cohort cannot be deleted' : 'Delete cohort'}
                          >
                            {deletingId === child.cohort_id ? '⏳' : '🗑'}
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </Fragment>
              )
            })}
          </div>
        )}
      </div>

      {isFormOpen && (
        <CohortForm
           mode={formMode}
           initialData={editData}
           onCancel={() => setIsFormOpen(false)}
           onSave={() => {
             setIsFormOpen(false)
             loadData()
             onCohortsChanged()
           }}
           refreshToken={refreshToken}
        />
      )}
      
      {isPanelOpen && (
        <SavedCohortsPanel
           savedCohorts={savedCohorts}
           onClose={() => setIsPanelOpen(false)}
           onDeleted={() => {
              loadData()
              onCohortsChanged()
           }}
           onEdit={handleEditSaved}
        />
      )}
    </section>
  )
}
