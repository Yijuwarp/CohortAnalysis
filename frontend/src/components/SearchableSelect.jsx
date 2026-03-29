import { useEffect, useId, useMemo, useRef, useState, useLayoutEffect } from 'react'
import { createPortal } from 'react-dom'

const normalizeOption = (option) => {
  if (typeof option === 'string') {
    return { label: option, value: option }
  }

  return {
    label: String(option?.label ?? option?.value ?? ''),
    value: String(option?.value ?? option?.label ?? ''),
    disabled: option?.disabled === true,
  }
}

export default function SearchableSelect({ options, value, onChange, onSearch, placeholder = 'Select...', disabled = false, className = '', style = {}, autoFocus = false, defaultOpen = false, onClear = null }) {
  const rootRef = useRef(null)
  const listboxId = useId()
  const [isOpen, setIsOpen] = useState(defaultOpen)
  const [searchTerm, setSearchTerm] = useState('')
  const [activeIndex, setActiveIndex] = useState(0)
  const [coords, setCoords] = useState({ top: 0, left: 0, width: 0 })

  const normalizedOptions = useMemo(() => (options || []).map(normalizeOption), [options])

  const selectedOption = useMemo(
    () => normalizedOptions.find((option) => option.value === value),
    [normalizedOptions, value]
  )

  const filteredOptions = useMemo(() => {
    const loweredSearch = searchTerm.toLowerCase()
    return normalizedOptions
      .filter((option) => option.label.toLowerCase().includes(loweredSearch))
      .slice(0, 100)
  }, [normalizedOptions, searchTerm])

  const updateCoords = () => {
    if (rootRef.current) {
        const rect = rootRef.current.getBoundingClientRect()
        setCoords({
            top: rect.bottom,
            left: rect.left,
            width: rect.width
        })
    }
  }

  useLayoutEffect(() => {
    if (isOpen) {
      updateCoords()
      
      // Pin dropdown to input using rAF for maximum smoothness during scrolls/resizes
      let animFrame
      const loop = () => {
        updateCoords()
        animFrame = requestAnimationFrame(loop)
      }
      animFrame = requestAnimationFrame(loop)
      
      return () => cancelAnimationFrame(animFrame)
    }
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) {
      return
    }

    const selectedIndex = filteredOptions.findIndex((option) => option.value === value)
    setActiveIndex(selectedIndex >= 0 ? selectedIndex : 0)
  }, [filteredOptions, isOpen, value])

  useEffect(() => {
    const handleDocumentMouseDown = (event) => {
      // If clicking inside the portalled dropdown, or inside the root input, don't close.
      // Since portal isn't inside rootRef, we need to check if target is in the dropdown class.
      const isExternalDropdown = event.target.closest('.searchable-select-dropdown')
      if (!rootRef.current?.contains(event.target) && !isExternalDropdown) {
        setIsOpen(false)
        setSearchTerm('')
        if (onSearch) onSearch('')
      }
    }

    document.addEventListener('mousedown', handleDocumentMouseDown)
    return () => document.removeEventListener('mousedown', handleDocumentMouseDown)
  }, [])

  const displayValue = isOpen ? searchTerm : selectedOption?.label || value || ''

  const handleSelect = (optionValue) => {
    onChange(optionValue)
    setIsOpen(false)
    setSearchTerm('')
    if (onSearch) onSearch('')
  }

  const handleKeyDown = (event) => {
    if (disabled) {
      return
    }

    if (!isOpen && (event.key === 'ArrowDown' || event.key === 'ArrowUp' || event.key === 'Enter')) {
      setIsOpen(true)
      return
    }

    if (event.key === 'ArrowDown') {
      event.preventDefault()
      setActiveIndex((index) => Math.min(index + 1, Math.max(filteredOptions.length - 1, 0)))
    }

    if (event.key === 'ArrowUp') {
      event.preventDefault()
      setActiveIndex((index) => Math.max(index - 1, 0))
    }

    if (event.key === 'Enter' && isOpen && filteredOptions.length > 0) {
      event.preventDefault()
      handleSelect(filteredOptions[activeIndex]?.value)
    }

    if (event.key === 'Escape') {
      setIsOpen(false)
      setSearchTerm('')
      if (onSearch) onSearch('')
    }
  }

  const hasNoOptions = normalizedOptions.length === 0
  const hasNoMatches = !hasNoOptions && filteredOptions.length === 0

  return (
    <div className={`searchable-select ${className} ${disabled ? 'searchable-select-disabled' : ''}`} ref={rootRef} style={{ ...style }}>
      <input
        className="searchable-select-input"
        title={selectedOption?.label || value || ''}
        role="combobox"
        aria-expanded={isOpen}
        aria-controls={listboxId}
        aria-autocomplete="list"
        disabled={disabled}
        placeholder={placeholder}
        value={displayValue}
        autoFocus={autoFocus}
        onFocus={() => setIsOpen(true)}
        onClick={() => setIsOpen(true)}
        onChange={(event) => {
          setSearchTerm(event.target.value)
          if (onSearch) onSearch(event.target.value)
          if (!isOpen) {
            setIsOpen(true)
          }
        }}
        onKeyDown={handleKeyDown}
      />
      
      {value && onClear && !disabled && (
        <button
          type="button"
          onClick={(e) => {
            e.preventDefault()
            e.stopPropagation()
            onClear()
            setSearchTerm('')
            if (onSearch) onSearch('')
          }}
          style={{
            position: 'absolute',
            right: 28,
            top: '50%',
            transform: 'translateY(-50%)',
            background: 'transparent',
            border: 'none',
            fontSize: '16px',
            cursor: 'pointer',
            color: '#999',
            padding: '2px 6px'
          }}
          title="Clear selection"
        >
          ×
        </button>
      )}

      {isOpen && createPortal(
        <div 
          className="searchable-select-dropdown"
          onMouseEnter={(e) => e.stopPropagation()}
          style={{
            position: "fixed",
            top: `${coords.top}px`,
            left: `${coords.left}px`,
            width: `${coords.width}px`,
            zIndex: 20002, // Higher than modal
            background: "#fff",
            border: "1px solid #ddd",
            boxShadow: "0 4px 12px rgba(0,0,0,0.1)",
            maxHeight: "300px",
            overflow: "hidden",
            display: "flex",
            flexDirection: "column"
          }}
        >
          <div className="searchable-select-list" role="listbox" id={listboxId} style={{ overflowY: "auto", flex: 1 }}>
            {hasNoOptions && <div className="searchable-select-empty">No options available</div>}
            {hasNoMatches && <div className="searchable-select-empty">No matching results</div>}
            {!hasNoOptions &&
              filteredOptions.map((option, index) => {
                const isActive = index === activeIndex
                const isSelected = option.value === value

                return (
                  <button
                    className={`searchable-select-option ${isActive ? 'searchable-select-option-active' : ''} ${
                      isSelected ? 'searchable-select-option-selected' : ''
                    } ${option.disabled ? 'searchable-select-option-disabled' : ''}`}
                    role="option"
                    aria-selected={isSelected}
                    aria-disabled={option.disabled}
                    type="button"
                    key={`${option.value}-${index}`}
                    disabled={option.disabled}
                    onMouseEnter={() => !option.disabled && setActiveIndex(index)}
                    onClick={() => {
                      if (!option.disabled) {
                        handleSelect(option.value)
                      }
                    }}
                  >
                    <span 
                      title={option.label}
                      style={{
                        display: "inline-block",
                        width: "100%",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap"
                      }}
                    >
                      {option.label}
                    </span>
                  </button>
                )
              })}
          </div>
          {!hasNoOptions && (
            <small className="searchable-select-count" style={{ padding: "6px 8px", borderTop: "1px solid #eee", fontSize: "11px", color: "#666" }}>
              Showing {filteredOptions.length} matching results
            </small>
          )}
        </div>,
        document.body
      )}
    </div>
  )
}
