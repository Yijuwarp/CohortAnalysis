import React, { useState } from 'react'
import { deleteSavedCohort } from '../api'

export default function SavedCohortsPanel({ savedCohorts, onClose, onDeleted, onEdit }) {
  const [error, setError] = useState('')
  const [deletingId, setDeletingId] = useState(null)

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
                  <div className="cohort-list-name cohort-left" style={{ flex: 2 }}>
                    <span>{cohort.name}</span>
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
                      className="cohort-icon-button"
                      type="button"
                      onClick={() => {
                        onClose();
                        onEdit(cohort.id);
                      }}
                      title="Edit global saved cohort"
                    >
                      ✏
                    </button>
                    <button
                      className="cohort-icon-button"
                      type="button"
                      onClick={() => handleDelete(cohort.id)}
                      disabled={deletingId === cohort.id}
                      title="Delete saved cohort"
                    >
                      {deletingId === cohort.id ? '⏳' : '🗑'}
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
