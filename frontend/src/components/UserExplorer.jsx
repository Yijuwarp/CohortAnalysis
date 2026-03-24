import { useEffect, useMemo, useRef, useState } from 'react'
import { getUserExplorer, searchUsers } from '../api'
import SearchableSelect from './SearchableSelect'

const PAGE_SIZE = 50

function UserSummaryCard({ summary }) {
  if (!summary) return null

  return (
    <div className="card ui-card user-explorer-summary">
      <h4>User Summary</h4>
      <div className="user-explorer-summary-grid">
        <div><strong>First event</strong><span>{summary.first_event_time || '—'}</span></div>
        <div><strong>Last event</strong><span>{summary.last_event_time || '—'}</span></div>
        <div><strong>Total events</strong><span>{summary.total_events ?? 0}</span></div>
      </div>
      <div className="user-explorer-properties">
        <div className="section-label">Latest properties</div>
        <div className="user-explorer-properties-grid">
          {Object.entries(summary.properties || {}).map(([key, value]) => (
            <div key={key} className="user-explorer-property">
              <span className="property-key">{key}</span>
              <span className="property-value">
                {value === null || value === undefined ? '—' : String(value)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function EventControls({ eventOptions, eventSearchTerm, onEventSearchTermChange, onNavigate, jumpDatetime, onJumpDatetimeChange, onReset, disabled, disablePrev, disableNext }) {
  return (
    <div className="user-explorer-controls card ui-card">
      <div className="user-explorer-controls-row">
        <SearchableSelect
          options={eventOptions}
          value={eventSearchTerm}
          onChange={onEventSearchTermChange}
          placeholder="Find Event"
          disabled={disabled}
        />
        <button type="button" className="button" onClick={() => onNavigate('prev')} disabled={disabled || disablePrev || !eventSearchTerm}>↑ Prev</button>
        <button type="button" className="button" onClick={() => onNavigate('next')} disabled={disabled || disableNext || !eventSearchTerm}>↓ Next</button>
      </div>
      <div className="user-explorer-controls-row">
        <input
          type="text"
          placeholder="YYYY-MM-DD"
          value={jumpDatetime}
          onChange={(event) => onJumpDatetimeChange(event.target.value)}
          disabled={disabled}
        />
        <button type="button" className="button" onClick={() => onNavigate('jump')} disabled={disabled || !jumpDatetime}>Jump to DateTime</button>
        <button type="button" className="button" onClick={onReset} disabled={disabled}>Reset</button>
      </div>
    </div>
  )
}

function EventTable({ events, highlightedTime, highlightedName }) {
  useEffect(() => {
    if (!highlightedTime) return;

    const el = document.getElementById(`event-${highlightedTime}-${highlightedName}`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }, [highlightedTime, highlightedName]);

  if (events.length === 0) {
    return <div className="card ui-card">No events found for this user</div>
  }

  return (
    <div className="analytics-table user-explorer-table">
      <div className="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Event</th>
            </tr>
          </thead>
          <tbody>
            {events.map((event, index) => {
              const isHighlighted = highlightedTime && event.event_time === highlightedTime && event.event_name === highlightedName
              return (
                <tr
                  id={`event-${event.event_time}-${event.event_name}`}
                  key={`${event.event_time}-${event.event_name}-${index}`}
                  className={isHighlighted ? 'user-explorer-highlighted-row' : ''}
                >
                  <td>{event.event_time}</td>
                  <td>
                    <div className="user-explorer-event-cell">
                      <span>{event.event_name}</span>
                      {event.cohort_joins?.length > 0 && (
                        <span className="user-explorer-join-tag">Joined: {event.cohort_joins.join(', ')}</span>
                      )}
                      <div className="user-explorer-tooltip">
                        <pre>{JSON.stringify(event.properties || {}, null, 2)}</pre>
                      </div>
                    </div>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function PaginationBar({ page, totalPages, onChangePage, disabled }) {
  const [pageInput, setPageInput] = useState(String(page || 1))

  useEffect(() => {
    setPageInput(String(page || 1))
  }, [page])

  return (
    <div className="user-explorer-pagination card ui-card">
      <button type="button" className="button" onClick={() => onChangePage(page - 1)} disabled={disabled || page <= 1}>Prev</button>
      <span>Page {page} / {totalPages}</span>
      <button type="button" className="button" onClick={() => onChangePage(page + 1)} disabled={disabled || page >= totalPages}>Next</button>
      <input
        type="number"
        min="1"
        max={totalPages}
        value={pageInput}
        onChange={(event) => setPageInput(event.target.value)}
      />
      <button
        type="button"
        className="button"
        disabled={disabled}
        onClick={() => onChangePage(Number(pageInput || 1))}
      >
        Go
      </button>
    </div>
  )
}

export default function UserExplorer({ state, setState }) {
  const [selectedUser, setSelectedUser] = useState(state?.selectedUser || '')
  const [userOptions, setUserOptions] = useState([])
  const [searchTerm, setSearchTerm] = useState('')
  const [events, setEvents] = useState([])
  const [summary, setSummary] = useState(null)
  const [page, setPage] = useState(state?.page || 1)
  const [totalPages, setTotalPages] = useState(1)
  const [currentEventTime, setCurrentEventTime] = useState(null)
  const [highlightedEventTime, setHighlightedEventTime] = useState(null)
  const [highlightedEventName, setHighlightedEventName] = useState(null)
  const [eventSearchTerm, setEventSearchTerm] = useState(state?.eventSearchTerm || '')
  const [jumpDatetime, setJumpDatetime] = useState(state?.jumpDatetime || '')
  const [loading, setLoading] = useState(false)
  const [noMorePrev, setNoMorePrev] = useState(false)
  const [noMoreNext, setNoMoreNext] = useState(false)

  useEffect(() => {
    setState({
      selectedUser,
      page,
      eventSearchTerm,
      jumpDatetime
    })
  }, [selectedUser, page, eventSearchTerm, jumpDatetime, setState])

  useEffect(() => {
    setNoMorePrev(false)
    setNoMoreNext(false)
  }, [eventSearchTerm])

  useEffect(() => {
    const id = setTimeout(async () => {
      try {
        const users = await searchUsers(searchTerm, 20)
        setUserOptions((users || []).map((user) => ({ label: user.user_id, value: user.user_id })))
      } catch {
        setUserOptions([])
      }
    }, 300)

    return () => clearTimeout(id)
  }, [searchTerm])

  const eventOptions = useMemo(() => {
    const distinct = new Set(events.map((event) => event.event_name).filter(Boolean))
    return Array.from(distinct).sort().map((event) => ({ label: event, value: event }))
  }, [events])

  const fetchData = async ({ targetPage = page, direction = null, jump = null, resetSearch = false } = {}) => {
    if (!selectedUser) return
    setLoading(true)

    try {
      const payload = await getUserExplorer({
        userId: selectedUser,
        page: targetPage,
        pageSize: PAGE_SIZE,
        eventSearch: resetSearch ? null : eventSearchTerm,
        direction,
        fromEventTime: currentEventTime,
        jumpDatetime: jump,
      })
      setSummary(payload.summary || null)
      setEvents(payload.events || [])
      setPage(payload.pagination?.page || 1)
      setTotalPages(payload.pagination?.total_pages || 1)
      setCurrentEventTime(payload.cursor?.current_event_time || null)
      
      let isMatchFound = false
      if (payload.matched_event) {
        setHighlightedEventTime(payload.matched_event.event_time)
        setHighlightedEventName(payload.matched_event.event_name)
        isMatchFound = true
      } else if (resetSearch) {
        setHighlightedEventTime(null)
        setHighlightedEventName(null)
      }

      if (direction === 'prev' && !isMatchFound) setNoMorePrev(true)
      if (direction === 'next' && !isMatchFound) setNoMoreNext(true)
      
      if (isMatchFound) {
        if (direction === 'prev') setNoMoreNext(false)
        if (direction === 'next') setNoMorePrev(false)
      }
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData({ targetPage: 1, resetSearch: true })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedUser])

  if (!selectedUser) {
    return (
      <div className="user-explorer-pane">
        <div className="card ui-card">
          <h4>User Explorer</h4>
          <SearchableSelect
            options={userOptions}
            value={selectedUser}
            onChange={setSelectedUser}
            placeholder="Search user_id..."
            onSearch={setSearchTerm}
          />
          <p>Select a user to explore</p>
        </div>
      </div>
    )
  }

  return (
    <div className="user-explorer-pane">
      <div className="card ui-card">
        <h4>User Explorer</h4>
        <SearchableSelect
          options={userOptions}
          value={selectedUser}
          onChange={(value) => {
            setSelectedUser(value)
            setPage(1)
            setCurrentEventTime(null)
            setHighlightedEventTime(null)
            setHighlightedEventName(null)
            setEventSearchTerm('')
            setJumpDatetime('')
            setNoMorePrev(false)
            setNoMoreNext(false)
          }}
          placeholder="Search user_id..."
          onSearch={setSearchTerm}
        />
      </div>

      <UserSummaryCard summary={summary} />

      <EventControls
        eventOptions={eventOptions}
        eventSearchTerm={eventSearchTerm}
        onEventSearchTermChange={setEventSearchTerm}
        onNavigate={(direction) => {
          if (direction === 'jump') {
            const normalizeDatetime = (input) => {
              if (!input) return null;
              if (input.length === 10) return input + " 00:00:00";
              return input;
            }
            fetchData({ targetPage: 1, jump: normalizeDatetime(jumpDatetime) })
            return
          }
          fetchData({ direction })
        }}
        jumpDatetime={jumpDatetime}
        onJumpDatetimeChange={setJumpDatetime}
        onReset={() => {
          setEventSearchTerm('')
          setJumpDatetime('')
          setPage(1)
          setCurrentEventTime(null)
          setHighlightedEventTime(null)
          setHighlightedEventName(null)
          setNoMorePrev(false)
          setNoMoreNext(false)
          fetchData({ targetPage: 1, resetSearch: true })
        }}
        disabled={loading}
        disablePrev={noMorePrev}
        disableNext={noMoreNext}
      />

      <EventTable events={events} highlightedTime={highlightedEventTime} highlightedName={highlightedEventName} />
      <PaginationBar page={page} totalPages={totalPages} onChangePage={(nextPage) => fetchData({ targetPage: nextPage })} disabled={loading} />
    </div>
  )
}
