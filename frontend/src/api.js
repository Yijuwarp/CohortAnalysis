const BASE_URL =
  import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

async function request(path, options = {}) {
  const response = await fetch(`${BASE_URL}${path}`, options)
  const data = await response.json()

  if (!response.ok) {
    const message = data?.detail || 'Request failed'
    throw new Error(message)
  }

  return data
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

export async function getColumnValues(column, eventName) {
  let path = `/column-values?column=${encodeURIComponent(column)}`
  if (eventName) {
    path += `&event_name=${encodeURIComponent(eventName)}`
  }

  return request(path, { method: 'GET' })
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
  if (propertyFilter?.property) {
    path += `&property=${encodeURIComponent(propertyFilter.property)}`
    path += `&operator=${encodeURIComponent(propertyFilter.operator || '=')}`
    if (propertyFilter.value !== undefined && propertyFilter.value !== null && propertyFilter.value !== '') {
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
  if (propertyFilter?.property) {
    path += `&property=${encodeURIComponent(propertyFilter.property)}`
    path += `&operator=${encodeURIComponent(propertyFilter.operator || '=')}`
    if (propertyFilter.value !== undefined && propertyFilter.value !== null && propertyFilter.value !== '') {
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
// Funnels
// ---------------------------------------------------------------------------

export async function createFunnel(payload) {
  return request('/funnels', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function updateFunnel(funnelId, payload) {
  return request(`/funnels/${funnelId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function listFunnels() {
  return request('/funnels', { method: 'GET' })
}

export async function deleteFunnel(funnelId) {
  return request(`/funnels/${funnelId}`, { method: 'DELETE' })
}

export async function runFunnel(funnelId) {
  return request('/funnels/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ funnel_id: funnelId }),
  })
}

// ---------------------------------------------------------------------------
// Flow Analytics
// ---------------------------------------------------------------------------

export async function getFlowL1(startEvent, direction = 'forward', depth = 20, propertyFilter = null) {
  const query = new URLSearchParams({
    start_event: startEvent,
    direction,
    depth: String(depth),
    include_top_k: 'true',
  })

  query.set('property_operator', propertyFilter?.operator || '=')
  if (propertyFilter?.column && propertyFilter?.values?.length) {
    query.set('property_column', propertyFilter.column)
    ;(propertyFilter.values || []).forEach(v => {
      query.append('property_values', String(v))
    })
  }
  return request(`/flow/l1?${query.toString()}`, { method: 'GET' })
}

export async function getFlowL2(startEvent, parentPath, direction = 'forward', depth = 20, propertyFilter = null) {
  const query = new URLSearchParams({
    start_event: startEvent,
    direction,
    depth: String(depth),
    include_top_k: 'true',
  })
  ;(parentPath || []).forEach(node => query.append('parent_path', node))

  query.set('property_operator', propertyFilter?.operator || '=')
  if (propertyFilter?.column && propertyFilter?.values?.length) {
    query.set('property_column', propertyFilter.column)
    ;(propertyFilter.values || []).forEach(v => {
      query.append('property_values', String(v))
    })
  }
  return request(`/flow/l2?${query.toString()}`, { method: 'GET' })
}

export async function getFlowGraph(startEvent, direction = 'forward', depth = 3, propertyFilter = null) {
  const query = new URLSearchParams({
    start_event: startEvent,
    direction,
    depth: String(depth),
    include_top_k: 'true',
  })
  query.set('property_operator', propertyFilter?.operator || '=')
  if (propertyFilter?.column && propertyFilter?.values?.length) {
    query.set('property_column', propertyFilter.column)
    ;(propertyFilter.values || []).forEach(v => query.append('property_values', String(v)))
  }
  return request(`/flow/graph?${query.toString()}`, { method: 'GET' })
}

export async function searchUsers(query = '', limit = 20) {
  return request(`/users/search?query=${encodeURIComponent(query)}&limit=${encodeURIComponent(limit)}`, {
    method: 'GET',
  })
}

export async function getUserExplorer(params) {
  const query = new URLSearchParams()
  query.set('user_id', params.userId)
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
