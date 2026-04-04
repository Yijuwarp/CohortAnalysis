import { useState, useEffect } from 'react'
import { getEventProperties, getEventPropertyValues } from '../api'
import SearchableSelect from './SearchableSelect'

export default function EventFilterChip({ eventConfig, updateEvent, removeEvent, isExpanded, setExpanded }) {
  const [properties, setProperties] = useState([])
  const [propertyValuesCache, setPropertyValuesCache] = useState({})
  const [loadingProperties, setLoadingProperties] = useState(false)

  const eventName = eventConfig.event_name
  const filters = eventConfig.filters || []

  useEffect(() => {
    if (isExpanded && properties.length === 0 && !loadingProperties) {
      setLoadingProperties(true)
      getEventProperties(eventName)
        .then(data => setProperties(data.properties || []))
        .catch(() => {})
        .finally(() => setLoadingProperties(false))
    }
  }, [isExpanded, eventName, properties.length, loadingProperties])

  const addFilter = (e) => {
    e.stopPropagation()
    const newFilters = [...filters, { property: '', operator: '=', value: '' }]
    updateEvent({ ...eventConfig, filters: newFilters })
  }

  const updateFilter = (index, patch) => {
    const newFilters = [...filters]
    newFilters[index] = { ...newFilters[index], ...patch }
    updateEvent({ ...eventConfig, filters: newFilters })
  }

  const removeFilter = (index, e) => {
    e.stopPropagation()
    const newFilters = filters.filter((_, i) => i !== index)
    updateEvent({ ...eventConfig, filters: newFilters })
  }

  const getLabel = () => {
    if (filters.length === 0) return eventName
    if (filters.length === 1) {
      const f = filters[0]
      if (!f.property || !f.value) return eventName
      const val = String(f.value).length > 15 ? String(f.value).substring(0, 12) + '...' : f.value
      return `${eventName} (${f.property}=${val})`
    }
    return `${eventName} (${filters.length} filters)`
  }

  return (
    <div className={`impact-tag event-filter-chip ${isExpanded ? 'impact-tag-expanded' : ''}`}>
      <div className="chip-header">
        <span className="chip-label" title={eventName}>{getLabel()}</span>
        <div className="chip-actions">
          <button className="gear-btn" onClick={(e) => {
            e.stopPropagation()
            setExpanded(isExpanded ? null : eventName)
          }} title="Toggle filters">⚙️</button>
          <button className="remove-btn" onClick={(e) => {
            e.stopPropagation()
            removeEvent()
          }}>×</button>
        </div>
      </div>

      {isExpanded && (
        <div className="chip-content">
          <hr style={{ margin: '8px 0', border: 'none', borderTop: '1px solid #eee' }} />
          <div className="filters-list">
            {filters.length === 0 && <div className="no-filters-msg">No filters applied</div>}
            {filters.map((filter, idx) => (
              <FilterRow 
                key={idx}
                filter={filter}
                eventName={eventName}
                properties={properties}
                propertyValuesCache={propertyValuesCache}
                setPropertyValuesCache={setPropertyValuesCache}
                onChange={(patch) => updateFilter(idx, patch)}
                onRemove={(e) => removeFilter(idx, e)}
              />
            ))}
          </div>
          <button className="add-filter-btn" onClick={addFilter}>+ Add Filter</button>
        </div>
      )}
    </div>
  )
}

function FilterRow({ filter, eventName, properties, propertyValuesCache, setPropertyValuesCache, onChange, onRemove }) {
  const [values, setValues] = useState([])
  const [loadingValues, setLoadingValues] = useState(false)

  // Load values when property changes
  useEffect(() => {
    if (filter.property) {
      if (propertyValuesCache[filter.property]) {
        setValues(propertyValuesCache[filter.property])
      } else if (!loadingValues) {
        setLoadingValues(true)
        getEventPropertyValues(eventName, filter.property)
          .then(data => {
            const vals = data.values || []
            setValues(vals)
            setPropertyValuesCache(prev => ({ ...prev, [filter.property]: vals }))
          })
          .catch(() => {})
          .finally(() => setLoadingValues(false))
      }
    } else {
      setValues([])
    }
  }, [filter.property, eventName, propertyValuesCache, setPropertyValuesCache, loadingValues])

  return (
    <div className="filter-row">
      <SearchableSelect 
        options={properties.map(p => ({ label: p, value: p }))}
        value={filter.property}
        onChange={(val) => onChange({ property: val, value: '' })}
        placeholder="Property"
        className="filter-prop-select"
      />
      <span className="filter-op">=</span>
      <SearchableSelect 
        options={values.map(v => ({ label: String(v), value: String(v) }))}
        value={filter.value}
        onChange={(val) => onChange({ value: val })}
        placeholder="Value"
        className="filter-val-select"
      />
      <button className="remove-filter-btn" onClick={onRemove} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#999' }}>×</button>
    </div>
  )
}
