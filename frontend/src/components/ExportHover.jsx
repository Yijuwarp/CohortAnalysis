import React from 'react'

export default function ExportHover({ exportBuffer, onRemoveItem }) {
  const visibleItems = exportBuffer.slice(0, 5)
  const remainingCount = exportBuffer.length - 5

  if (exportBuffer.length === 0) {
    return (
      <div className="export-hover-panel empty">
        <p>No items added</p>
      </div>
    )
  }

  return (
    <div className="export-hover-panel">
      <div className="export-hover-list">
        {visibleItems.map((item) => (
          <div key={item.id} className="export-hover-item">
            <div className="export-hover-item-content">
              <div className="export-hover-item-title">{item.title}</div>
              <div className="export-hover-item-summary">{item.summary}</div>
            </div>
            <button 
              className="export-hover-remove" 
              onClick={(e) => {
                e.stopPropagation()
                onRemoveItem(item.id)
              }}
              title="Remove item"
            >
              ✕
            </button>
          </div>
        ))}
      </div>
      {remainingCount > 0 && (
        <div className="export-hover-more">
          +{remainingCount} more
        </div>
      )}
    </div>
  )
}
