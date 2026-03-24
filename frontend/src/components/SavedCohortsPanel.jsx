import React, { useState } from 'react'
import { deleteSavedCohort } from '../api'
import { formatCohortLogic } from '../utils/cohortUtils'

export default function SavedCohortsPanel({ savedCohorts, onClose, onDeleted, onEdit, onDuplicate }) {
  const [error, setError] = useState('')
  const [deletingId, setDeletingId] = useState(null)
  const [activeTooltipId, setActiveTooltipId] = useState(null)

  const handleDelete = async (id) => {
    setError('')
    setDeletingId(id)
    try {
      await deleteSavedCohort(id)
      onDeleted()
    } catch (err) {
      setError(err.message)
    } finally {
      setDeletingId(null)
    }
  }

  return (
    <div className="modal-overlay" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.5)' }}>
      <div className="modal-content card" style={{ padding: '24px', width: '600px', maxHeight: '80vh', overflowY: 'auto' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
          <h3>Manage Saved Cohorts</h3>
          <button className="button button-text" onClick={onClose}>Close</button>
        </div>

        {error && <p className="error">{error}</p>}

        {savedCohorts.length === 0 ? (
          <p className="secondary-text">No saved cohorts.</p>
        ) : (
          <div className="cohort-list-table">
            <div className="cohort-list-header cohort-list-row">
              <span>Name</span>
              <span>Status</span>
              <span>Actions</span>
            </div>
            {savedCohorts.map((cohort) => {
              const warningTooltip = cohort.is_valid
                ? ''
                : cohort.errors?.map(e => e.message).join('; ') || 'Invalid cohort definition'
              
              return (
                <div key={cohort.id} className="cohort-list-row cohort-row">
                  <div className="cohort-list-name-group" style={{ flex: 2 }}>
                    <span style={{ fontWeight: 500 }}>{cohort.name}</span>
                  </div>

                  <span className="cohort-list-size">
                    {cohort.is_valid ? (
                      <span className="badge-active" style={{ backgroundColor: '#e2f5e9', color: '#138944', padding: '4px 8px', borderRadius: '4px', fontSize: '12px' }}>Valid</span>
                    ) : (
                      <span className="badge-inactive" title={warningTooltip} style={{ backgroundColor: '#fcdcd8', color: '#c72e2e', padding: '4px 8px', borderRadius: '4px', fontSize: '12px', cursor: 'help' }}>Invalid ⚠️</span>
                    )}
                  </span>

                   <div className="cohort-actions">
                    <button
                      className="cohort-icon-button info-icon"
                      type="button"
                      aria-label="View cohort definition"
                      onMouseEnter={() => setActiveTooltipId(cohort.id)}
                      onMouseLeave={() => setActiveTooltipId(null)}
                      onClick={(e) => {
                        e.stopPropagation();
                        setActiveTooltipId(prev => prev === cohort.id ? null : cohort.id);
                      }}
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                        <circle cx="12" cy="12" r="10"/>
                        <path d="M12 16v-4"/>
                        <path d="M12 8h.01"/>
                      </svg>
                      {activeTooltipId === cohort.id && (
                        <div className="cohort-info-tooltip">
                          {formatCohortLogic(cohort)}
                        </div>
                      )}
                    </button>

                    <button
                      className="cohort-icon-button"
                      type="button"
                      onClick={() => {
                        onClose();
                        onEdit(cohort.id);
                      }}
                      title="Edit global saved cohort"
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M12 20h9"/>
                        <path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/>
                      </svg>
                    </button>
                    <button
                      className="cohort-icon-button"
                      type="button"
                      onClick={() => onDuplicate?.(cohort)}
                      title="Duplicate saved cohort"
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                        <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
                        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                      </svg>
                    </button>
                    <button
                      className="cohort-icon-button"
                      type="button"
                      onClick={() => handleDelete(cohort.id)}
                      disabled={deletingId === cohort.id}
                      title="Delete saved cohort"
                    >
                      {deletingId === cohort.id ? (
                        '⏳'
                      ) : (
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M3 6h18"/>
                          <path d="M8 6v-2h8v2"/>
                          <path d="M19 6l-1 14H6L5 6"/>
                          <path d="M10 11v6"/>
                          <path d="M14 11v6"/>
                        </svg>
                      )}
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
