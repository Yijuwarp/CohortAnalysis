const COLORS = ['#2563eb', '#dc2626', '#16a34a', '#7c3aed', '#f59e0b']

export function getCohortColor(cohortId, index) {
  return COLORS[index % COLORS.length]
}
