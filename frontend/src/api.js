const BASE_URL =
  import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

const SKIP_AUTH = ['/login']

async function request(path, options = {}) {
  const userId = localStorage.getItem('user_id')
  
  // Clean path for matching (remove query params)
  const cleanPath = path.split('?')[0]
  
  let authPath = path
  if (!SKIP_AUTH.includes(cleanPath) && userId) {
    const separator = path.includes('?') ? '&' : '?'
    authPath = `${path}${separator}user_id=${userId}`
  }
  
  const response = await fetch(`${BASE_URL}${authPath}`, options)
  const data = await response.json()

  if (!response.ok) {
    let message = 'Request failed'
    if (data?.detail) {
      message = typeof data.detail === 'object' ? JSON.stringify(data.detail) : data.detail
    }
    throw new Error(message)
  }

  return data
}

export async function login(email) {
  return request('/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email }),
  })
}

function normalizeMaxDay(value) {
  const n = Number(value)
  if (!Number.isFinite(n) || n <= 0) {
    return 7
  }

  return Math.floor(n)
}

export async function uploadCSV(file) {
  const formData = new FormData()
  formData.append('file', file)

  return request('/upload', {
    method: 'POST',
    body: formData,
  })
}

export async function mapColumns(payload) {
  return request('/map-columns', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function createCohort(payload) {
  return request('/cohorts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function listCohorts() {
  return request('/cohorts', { method: 'GET' })
}

export async function getCohortDetail(cohortId) {
  return request(`/cohorts/${cohortId}`, { method: 'GET' })
}

export async function updateCohort(cohortId, payload) {
  return request(`/cohorts/${cohortId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function createSavedCohort(payload) {
  return request('/saved-cohorts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function getSavedCohorts() {
  return request('/saved-cohorts', { method: 'GET' })
}

export async function updateSavedCohort(id, payload) {
  return request(`/saved-cohorts/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function deleteSavedCohort(id) {
  return request(`/saved-cohorts/${id}`, { method: 'DELETE' })
}

export async function estimateCohort(payload) {
  return request('/cohorts/estimate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function applyFilters(payload) {
  return request('/apply-filters', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function getScope() {
  return request('/scope', { method: 'GET' })
}

export async function getColumns() {
  return request('/columns', { method: 'GET' })
}

export async function getColumnValues(column, eventName, search, limit, signal) {
  let path = `/column-values?column=${encodeURIComponent(column)}`
  if (eventName) {
    path += `&event_name=${encodeURIComponent(eventName)}`
  }
  if (search !== undefined && search !== null) {
    path += `&search=${encodeURIComponent(search)}`
  }
  if (limit) {
    path += `&limit=${encodeURIComponent(limit)}`
  }

  return request(path, { method: 'GET', signal })
}

export async function getDateRange() {
  return request('/date-range', { method: 'GET' })
}

export async function deleteCohort(cohortId) {
  return request(`/cohorts/${cohortId}`, {
    method: 'DELETE',
  })
}

export async function toggleCohortHide(cohortId) {
  return request(`/cohorts/${cohortId}/hide`, {
    method: 'PATCH',
  })
}

export async function randomSplitCohort(cohortId) {
  return request(`/cohorts/${cohortId}/random_split`, {
    method: 'POST',
  })
}

export async function splitCohort(cohortId, payload) {
  return request(`/cohorts/${cohortId}/split`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function previewSplit(cohortId, payload) {
  return request(`/cohorts/${cohortId}/split/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}


export async function getRetention(maxDay, retentionEvent = 'any', includeCI = false, confidence = 0.95, retentionType = 'classic', granularity = 'day') {
  const query = new URLSearchParams({
    max_day: String(maxDay),
    include_ci: String(includeCI),
    confidence: String(confidence),
    retention_type: retentionType,
    granularity: granularity,
  })

  let path = `/retention?${query.toString()}`

  if (retentionEvent !== 'any') {
    path += `&retention_event=${encodeURIComponent(retentionEvent)}`
  }

  return request(path, {
    method: 'GET',
  })
}

export async function listEvents() {
  return request('/events', {
    method: 'GET',
  })
}

export async function getUsage(event, maxDay, retentionEvent, propertyFilter = null) {
  if (retentionEvent === undefined || retentionEvent === null || retentionEvent === '') {
    throw new Error('Retention event must be selected before loading usage metrics')
  }

  let path = `/usage?event=${encodeURIComponent(event)}&max_day=${maxDay}`
  if (retentionEvent !== 'any') {
    path += `&retention_event=${encodeURIComponent(retentionEvent)}`
  }
  const hasValues = Array.isArray(propertyFilter?.values)
    ? propertyFilter.values.length > 0
    : (propertyFilter?.value !== undefined && propertyFilter?.value !== null && propertyFilter?.value !== '')
  if (propertyFilter?.property && hasValues) {
    path += `&property=${encodeURIComponent(propertyFilter.property)}`
    path += `&operator=${encodeURIComponent(propertyFilter.operator || '=')}`
    if (Array.isArray(propertyFilter.values)) {
      propertyFilter.values.forEach(v => {
        path += `&value=${encodeURIComponent(v)}`
      })
    } else {
      path += `&value=${encodeURIComponent(propertyFilter.value)}`
    }
  }

  return request(path, { method: 'GET' })
}

export async function getEventProperties(event) {
  return request(`/events/${encodeURIComponent(event)}/properties`, { method: 'GET' })
}

export async function getEventPropertyValues(event, property, limit = 25) {
  return request(
    `/events/${encodeURIComponent(event)}/properties/${encodeURIComponent(property)}/values?limit=${limit}`,
    { method: 'GET' }
  )
}


export async function getRevenueEvents() {
  return request('/revenue-events', { method: 'GET' })
}

export async function getRevenueConfigEvents() {
  return request('/revenue-config-events', { method: 'GET' })
}

export async function updateRevenueConfig(revenueConfig) {
  return request('/update-revenue-config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ revenue_config: revenueConfig }),
  })
}

export async function getMonetization(maxDay) {
  const safeMaxDay = normalizeMaxDay(maxDay)
  return request(`/monetization?max_day=${safeMaxDay}`, { method: 'GET' })
}

export async function getUsageFrequency(event, propertyFilter = null) {
  let path = `/usage-frequency?event=${encodeURIComponent(event)}`
  const hasValues = Array.isArray(propertyFilter?.values)
    ? propertyFilter.values.length > 0
    : (propertyFilter?.value !== undefined && propertyFilter?.value !== null && propertyFilter?.value !== '')
  if (propertyFilter?.property && hasValues) {
    path += `&property=${encodeURIComponent(propertyFilter.property)}`
    path += `&operator=${encodeURIComponent(propertyFilter.operator || '=')}`
    if (Array.isArray(propertyFilter.values)) {
      propertyFilter.values.forEach(v => {
        path += `&value=${encodeURIComponent(v)}`
      })
    } else {
      path += `&value=${encodeURIComponent(propertyFilter.value)}`
    }
  }

  return request(path, { method: 'GET' })
}

export async function compareCohorts(payload) {
  return request('/compare-cohorts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

// ---------------------------------------------------------------------------
// Flow Analytics
// ---------------------------------------------------------------------------

export async function getFlowL1(startEvent, direction = 'forward', depth = 20, propertyFilter = null, limit = 3, options = {}) {
  const query = new URLSearchParams({
    start_event: startEvent,
    direction,
    depth: String(depth),
    include_top_k: 'true',
    limit: String(limit),
  })

  query.set('property_operator', propertyFilter?.operator || '=')
  if (propertyFilter?.column && propertyFilter?.values?.length) {
    query.set('property_column', propertyFilter.column)
    ;(propertyFilter.values || []).forEach(v => {
      query.append('property_values', String(v))
    })
  }
  return request(`/flow/l1?${query.toString()}`, { method: 'GET', ...options })
}

export async function getFlowL2(startEvent, parentPath, direction = 'forward', depth = 20, propertyFilter = null, limit = 3, options = {}) {
  const query = new URLSearchParams({
    start_event: startEvent,
    direction,
    depth: String(depth),
    include_top_k: 'true',
    limit: String(limit),
  })
  ;(parentPath || []).forEach(node => query.append('parent_path', node))

  query.set('property_operator', propertyFilter?.operator || '=')
  if (propertyFilter?.column && propertyFilter?.values?.length) {
    query.set('property_column', propertyFilter.column)
    ;(propertyFilter.values || []).forEach(v => {
      query.append('property_values', String(v))
    })
  }
  return request(`/flow/l2?${query.toString()}`, { method: 'GET', ...options })
}


export async function searchUsers(query = '', limit = 20, cohortId = null) {
  let path = `/users/search?query=${encodeURIComponent(query)}&limit=${encodeURIComponent(limit)}`
  if (cohortId !== null && cohortId !== undefined && cohortId !== 'all') {
    path += `&cohort_id=${encodeURIComponent(cohortId)}`
  }
  return request(path, { method: 'GET' })
}

export async function getUserExplorer(params) {
  const query = new URLSearchParams()
  query.set('target_user_id', params.userId)
  query.set('page', String(params.page ?? 1))
  query.set('page_size', String(params.pageSize ?? 50))

  if (params.eventSearch) {
    query.set('event_search', params.eventSearch)
  }
  if (params.direction) {
    query.set('direction', params.direction)
  }
  if (params.fromEventTime) {
    query.set('from_event_time', params.fromEventTime)
  }
  if (params.jumpDatetime) {
    query.set('jump_datetime', params.jumpDatetime)
  }

  return request(`/user-explorer?${query.toString()}`, { method: 'GET' })
}

// ---------------------------------------------------------------------------
// Paths (Sequence Analysis)
// ---------------------------------------------------------------------------

export async function createPath(payload) {
  return request('/paths', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function updatePath(pathId, payload) {
  return request(`/paths/${pathId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function listPaths() {
  return request('/paths', { method: 'GET' })
}

export async function deletePath(pathId) {
  return request(`/paths/${pathId}`, { method: 'DELETE' })
}

export async function runPaths(steps, maxStepGapMinutes = null, pathId = null) {
  return request('/paths/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 
      steps, 
      max_step_gap_minutes: maxStepGapMinutes,
      path_id: pathId
    }),
  })
}

export async function createPathsDropOffCohort(cohortId, stepIndex, steps, cohortName, maxStepGapMinutes = null, pathId = null) {
  return request('/paths/create-dropoff-cohort', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 
      cohort_id: cohortId, 
      step_index: stepIndex, 
      steps,
      max_step_gap_minutes: maxStepGapMinutes,
      path_id: pathId,
      cohort_name: cohortName
    }),
  })
}

export async function createPathsReachedCohort(cohortId, stepIndex, steps, cohortName, maxStepGapMinutes = null, pathId = null) {
  return request('/paths/create-reached-cohort', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 
      cohort_id: cohortId, 
      step_index: stepIndex, 
      steps,
      max_step_gap_minutes: maxStepGapMinutes,
      path_id: pathId,
      cohort_name: cohortName
    }),
  })
}
export async function runImpactAnalysis(payload) {
  return request('/impact/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function runImpactStats(payload, signal) {
  return request('/impact/stats', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal,
  })
}
