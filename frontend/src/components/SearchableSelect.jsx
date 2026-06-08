import { useEffect, useId, useMemo, useRef, useState, useLayoutEffect } from 'react'
import { createPortal } from 'react-dom'
import { getColumnValues } from '../api'

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

export default function SearchableSelect({ 
  options: propOptions, 
  value, 
  onChange, 
  onSearch, 
  placeholder = 'Select...', 
  disabled = false, 
  className = '', 
  style = {}, 
  autoFocus = false, 
  defaultOpen = false, 
  onClear = null,
  // Options that always appear at the top, regardless of search
  pinnedOptions: pinnedOptionsProp = [],
  // Server-side search props
  column = null,
  eventName = null,
  excludeValues = []
}) {
  const rootRef = useRef(null)
  const listboxId = useId()
  const [isOpen, setIsOpen] = useState(defaultOpen)
  const [searchTerm, setSearchTerm] = useState('')
  const [activeIndex, setActiveIndex] = useState(0)
  const [coords, setCoords] = useState({ top: 0, left: 0, width: 0 })
  
  // Server-side state
  const [serverOptions, setServerOptions] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState(null)
  const [isFullListFetched, setIsFullListFetched] = useState(false)
  const cacheRef = useRef({}) // { [cacheKey]: values[] }
  const latestRequestRef = useRef(0)

  // Normalized pinned options (always at top)
  const normalizedPinned = useMemo(
    () => (pinnedOptionsProp || []).map(normalizeOption),
    [pinnedOptionsProp]
  )
  const pinnedValues = useMemo(
    () => new Set(normalizedPinned.map(o => o.value)),
    [normalizedPinned]
  )

  const excludedSet = useMemo(
    () => new Set((excludeValues || []).map(v => String(v))),
    [excludeValues]
  )

  // Combined options: merge prop options with server-side results
  const allOptions = useMemo(() => {
    const normalizedPropOptions = (propOptions || []).map(normalizeOption)
    const normalizedServerOptions = serverOptions.map(normalizeOption)
    
    // Merge strategy: unique values, prioritize server options for labels if collision
    const seen = new Set(pinnedValues) // exclude pinned from dedup — they go first
    const merged = []
    
    // Add server options first (most relevant to search)
    normalizedServerOptions.forEach(opt => {
      if (!pinnedValues.has(opt.value)) {
        seen.add(opt.value)
        merged.push(opt)
      }
    })
    
    // Add prop options (ensure current value is always visible even if not in search results)
    normalizedPropOptions.forEach(opt => {
      if (!seen.has(opt.value)) {
        seen.add(opt.value)
        merged.push(opt)
      }
    })
    
    return merged.filter(opt => !excludedSet.has(opt.value))
  }, [propOptions, serverOptions, pinnedValues, excludedSet])

  const selectedOption = useMemo(
    () => [...normalizedPinned, ...allOptions].find((option) => option.value === value),
    [normalizedPinned, allOptions, value]
  )

  // Filter options: if server-side is active, we don't do client-side filtering
  // unless we have the full list fetched.
  const filteredOptions = useMemo(() => {
    const loweredSearch = searchTerm.toLowerCase()

    // Filter pinned options by search term (but always include them when no search)
    const matchingPinned = searchTerm
      ? normalizedPinned.filter(o => o.label.toLowerCase().includes(loweredSearch))
      : normalizedPinned

    let rest
    if (!column || isFullListFetched) {
      rest = allOptions
        .filter((option) => option.label.toLowerCase().includes(loweredSearch))
        .slice(0, 100)
    } else {
      // Server-side mode: server already filtered for us
      rest = allOptions.slice(0, 100)
    }

    // Prepend pinned, dedup
    const pinnedVals = new Set(matchingPinned.map(o => o.value))
    return [...matchingPinned, ...rest.filter(o => !pinnedVals.has(o.value))]
  }, [normalizedPinned, allOptions, searchTerm, column, isFullListFetched])

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
      
      let animFrame
      const loop = () => {
        updateCoords()
        animFrame = requestAnimationFrame(loop)
      }
      animFrame = requestAnimationFrame(loop)
      
      return () => cancelAnimationFrame(animFrame)
    }
  }, [isOpen])

  // Reset server state when scope changes
  useEffect(() => {
    setIsFullListFetched(false)
    setServerOptions([])
    cacheRef.current = {}
  }, [column, eventName])

  // Server-side search effect
  useEffect(() => {
    if (!column || !isOpen) return

    // If we have the full list, we don't need to search server-side anymore
    if (isFullListFetched && searchTerm !== '') return

    // Minimum search threshold: 1 char is too broad, but empty search is "load defaults"
    if (searchTerm.length === 1) return

    const cacheKey = `${column}:${eventName || ''}:${searchTerm}`
    if (cacheRef.current[cacheKey]) {
      const results = cacheRef.current[cacheKey]
      setServerOptions(results)
      if (searchTerm === '') {
        setIsFullListFetched(results.length < 100)
      }
      return
    }

    const requestId = ++latestRequestRef.current
    const controller = new AbortController()
    
    const fetchValues = async () => {
      setIsLoading(true)
      setError(null)
      try {
        const response = await getColumnValues(column, eventName, searchTerm, 100, controller.signal)
        if (requestId !== latestRequestRef.current) return
        
        const results = response.values || []
        cacheRef.current[cacheKey] = results
        
        if (searchTerm === '') {
          setIsFullListFetched(results.length < 100)
        }

        setServerOptions(prev => {
          // Merge strategy for UI stability: results + unique items from prev
          const seen = new Set(results)
          const merged = [...results]
          prev.forEach(val => {
            if (!seen.has(val)) {
              merged.push(val)
              seen.add(val)
            }
          })
          return merged.slice(0, 200)
        })
      } catch (err) {
        if (err.name === 'AbortError') return
        if (requestId !== latestRequestRef.current) return
        setError(err.message)
      } finally {
        if (requestId === latestRequestRef.current) {
          setIsLoading(false)
        }
      }
    }

    const timeoutId = setTimeout(fetchValues, 300)
    return () => {
      clearTimeout(timeoutId)
      controller.abort()
    }
  }, [column, eventName, searchTerm, isOpen, isFullListFetched])

  useEffect(() => {
    if (!isOpen) {
      return
    }

    const selectedIndex = filteredOptions.findIndex((option) => option.value === value)
    setActiveIndex(selectedIndex >= 0 ? selectedIndex : 0)
  }, [filteredOptions, isOpen, value])

  useEffect(() => {
    const handleDocumentMouseDown = (event) => {
      const isExternalDropdown = event.target.closest('.searchable-select-dropdown')
      if (!rootRef.current?.contains(event.target) && !isExternalDropdown) {
        setIsOpen(false)
        setSearchTerm('')
        if (onSearch) onSearch('')
      }
    }

    document.addEventListener('mousedown', handleDocumentMouseDown)
    return () => document.removeEventListener('mousedown', handleDocumentMouseDown)
  }, [onSearch])

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

  const hasNoOptions = allOptions.length === 0 && !isLoading
  const hasNoMatches = !hasNoOptions && filteredOptions.length === 0 && !isLoading

  return (
    <div className={`searchable-select ${className} ${disabled ? 'searchable-select-disabled' : ''}`} ref={rootRef} style={{ ...style, position: 'relative' }}>
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
            minWidth: `${coords.width}px`,
            width: "max-content",
            maxWidth: "400px",
            zIndex: 20002,
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
            {isLoading && filteredOptions.length === 0 && (
               <div className="searchable-select-empty">Loading...</div>
            )}
            {!isLoading && hasNoOptions && <div className="searchable-select-empty">No options available</div>}
            {!isLoading && hasNoMatches && <div className="searchable-select-empty">No matching results</div>}
            {error && <div className="searchable-select-empty error">{error}</div>}
            
            {filteredOptions.map((option, index) => {
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
          <div style={{ padding: "6px 8px", borderTop: "1px solid #eee", fontSize: "11px", color: "#666", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
             <span>{isLoading ? 'Searching...' : `Showing ${filteredOptions.length} results`}</span>
             {column && (
               <span style={{ opacity: 0.8 }}>
                 {isFullListFetched ? 'Full list (instant search)' : 'Server-side search'}
               </span>
             )}
          </div>
        </div>,
        document.body
      )}
    </div>
  )
}
