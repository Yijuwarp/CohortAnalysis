import { useEffect, useId, useMemo, useRef, useState } from 'react'

const normalizeOption = (option) => {
  if (typeof option === 'string') {
    return { label: option, value: option }
  }

  return {
    label: String(option?.label ?? option?.value ?? ''),
    value: String(option?.value ?? option?.label ?? ''),
  }
}

export default function SearchableSelect({ options, value, onChange, placeholder = 'Select...', disabled = false, className = '' }) {
  const rootRef = useRef(null)
  const listboxId = useId()
  const [isOpen, setIsOpen] = useState(false)
  const [searchTerm, setSearchTerm] = useState('')
  const [activeIndex, setActiveIndex] = useState(0)

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

  useEffect(() => {
    if (!isOpen) {
      return
    }

    const selectedIndex = filteredOptions.findIndex((option) => option.value === value)
    setActiveIndex(selectedIndex >= 0 ? selectedIndex : 0)
  }, [filteredOptions, isOpen, value])

  useEffect(() => {
    const handleDocumentMouseDown = (event) => {
      if (!rootRef.current?.contains(event.target)) {
        setIsOpen(false)
        setSearchTerm('')
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
    }
  }

  const hasNoOptions = normalizedOptions.length === 0
  const hasNoMatches = !hasNoOptions && filteredOptions.length === 0

  return (
    <div className={`searchable-select ${className} ${disabled ? 'searchable-select-disabled' : ''}`} ref={rootRef}>
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
        onFocus={() => setIsOpen(true)}
        onClick={() => setIsOpen(true)}
        onChange={(event) => {
          setSearchTerm(event.target.value)
          if (!isOpen) {
            setIsOpen(true)
          }
        }}
        onKeyDown={handleKeyDown}
      />

      {isOpen && (
        <div className="searchable-select-menu">
          <div className="searchable-select-list" role="listbox" id={listboxId}>
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
                    }`}
                    role="option"
                    aria-selected={isSelected}
                    type="button"
                    key={`${option.value}-${index}`}
                    onMouseEnter={() => setActiveIndex(index)}
                    onClick={() => handleSelect(option.value)}
                  >
                    <span title={option.label}>{option.label}</span>
                  </button>
                )
              })}
          </div>
          {!hasNoOptions && (
            <small className="searchable-select-count">
              Showing {filteredOptions.length} matching results
            </small>
          )}
        </div>
      )}
    </div>
  )
}
