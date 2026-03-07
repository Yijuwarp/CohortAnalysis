const MIN_B = 0.01
const MAX_B = 1.5

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

function modelValue(day, A, B) {
  if (day <= 0) return 0
  return A * Math.pow(day, B)
}

function fitAtB(days, values, B) {
  let numerator = 0
  let denominator = 0

  for (let index = 0; index < days.length; index += 1) {
    const day = days[index]
    if (day <= 0) continue
    const basis = Math.pow(day, B)
    numerator += values[index] * basis
    denominator += basis * basis
  }

  const estimatedA = denominator > 0 ? numerator / denominator : 0
  const rawA = Math.max(0, Number.isFinite(estimatedA) ? estimatedA : 0)
  const A = rawA

  let sse = 0
  for (let index = 0; index < days.length; index += 1) {
    const day = days[index]
    const prediction = modelValue(day, A, B)
    const residual = values[index] - prediction
    sse += residual * residual
  }

  return { A, sse }
}

export function fitPowerLaw(days, values) {
  const safeDays = []
  const safeValues = []

  for (let index = 0; index < days.length; index += 1) {
    const day = Number(days[index])
    const value = Number(values[index])

    if (Number.isFinite(day) && Number.isFinite(value) && day >= 0) {
      safeDays.push(day)
      safeValues.push(value)
    }
  }

  if (safeDays.length === 0) {
    return {
      A: 0,
      B: 0.5,
      fittedValues: [],
      residualVariance: 0,
    }
  }

  let bestB = 0.5
  let bestA = 0
  let bestSse = Number.POSITIVE_INFINITY
  let lowB = MIN_B
  let highB = MAX_B

  for (let round = 0; round < 7; round += 1) {
    const steps = 64
    for (let step = 0; step < steps; step += 1) {
      const ratio = step / (steps - 1)
      const candidateB = lowB + (highB - lowB) * ratio
      const { A, sse } = fitAtB(safeDays, safeValues, candidateB)
      if (sse < bestSse) {
        bestSse = sse
        bestB = candidateB
        bestA = A
      }
    }

    const margin = (highB - lowB) / 4
    lowB = clamp(bestB - margin, MIN_B, MAX_B)
    highB = clamp(bestB + margin, MIN_B, MAX_B)
  }

  const fittedValues = safeDays.map((day) => modelValue(day, bestA, bestB))
  const degreesOfFreedom = safeDays.length - 2
  const residualVariance = degreesOfFreedom > 0 ? bestSse / degreesOfFreedom : 0

  return {
    A: bestA,
    B: bestB,
    fittedValues,
    residualVariance,
  }
}

export function generateProjection({
  A,
  B,
  lastObservedDay,
  horizonDays,
  residualVariance = 0,
}) {
  const projectedCurve = {}
  const upperCI = {}
  const lowerCI = {}
  const safeA = Number.isFinite(Number(A)) ? Number(A) : 0
  const safeB = clamp(Number.isFinite(Number(B)) ? Number(B) : MIN_B, MIN_B, MAX_B)
  const safeLastObservedDay = Math.max(0, Number(lastObservedDay) || 0)
  const safeHorizonDays = Math.max(safeLastObservedDay, Number(horizonDays) || safeLastObservedDay)
  const baseMargin = 1.96 * Math.sqrt(Math.max(0, Number(residualVariance) || 0))

  for (let day = safeLastObservedDay + 1; day <= safeHorizonDays; day += 1) {
    const prediction = modelValue(day, safeA, safeB)
    const distance = day - safeLastObservedDay
    const scaledMargin = baseMargin * Math.sqrt(1 + distance / 10)
    projectedCurve[day] = Number.isFinite(prediction) ? prediction : 0
    upperCI[day] = projectedCurve[day] + scaledMargin
    lowerCI[day] = Math.max(0, projectedCurve[day] - scaledMargin)
  }

  return {
    projectedCurve,
    upperCI,
    lowerCI,
  }
}
