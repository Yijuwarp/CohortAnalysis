const MIN_K = 1e-6
const MAX_K = 20

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

function modelValue(day, L, k) {
  return L * (1 - Math.exp(-k * day))
}

function fitAtK(days, values, k, maxObserved) {
  let numerator = 0
  let denominator = 0

  for (let index = 0; index < days.length; index += 1) {
    const day = days[index]
    const basis = 1 - Math.exp(-k * day)
    numerator += values[index] * basis
    denominator += basis * basis
  }

  const estimatedL = denominator > 0 ? numerator / denominator : maxObserved
  const rawL = Math.max(maxObserved, Number.isFinite(estimatedL) ? estimatedL : maxObserved)
  const L = clamp(rawL, maxObserved, Math.max(maxObserved, maxObserved * 10))

  let sse = 0
  for (let index = 0; index < days.length; index += 1) {
    const prediction = modelValue(days[index], L, k)
    const residual = values[index] - prediction
    sse += residual * residual
  }

  return { L, sse }
}

export function fitSaturatingExponential(days, values) {
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
      L: 0,
      k: 0.01,
      fittedValues: [],
      residualVariance: 0,
    }
  }

  const maxObserved = Math.max(0, ...safeValues)
  let bestK = 0.05
  let bestL = Math.max(maxObserved, 0)
  let bestSse = Number.POSITIVE_INFINITY
  let lowK = 1e-4
  let highK = 2

  for (let round = 0; round < 7; round += 1) {
    const steps = 64
    const logLow = Math.log(lowK)
    const logHigh = Math.log(highK)

    for (let step = 0; step < steps; step += 1) {
      const ratio = step / (steps - 1)
      const candidateK = Math.exp(logLow + (logHigh - logLow) * ratio)
      const { L, sse } = fitAtK(safeDays, safeValues, candidateK, maxObserved)
      if (sse < bestSse) {
        bestSse = sse
        bestK = candidateK
        bestL = L
      }
    }

    lowK = clamp(bestK / 3, MIN_K, MAX_K)
    highK = clamp(bestK * 3, MIN_K, MAX_K)
  }

  const fittedValues = safeDays.map((day) => modelValue(day, bestL, bestK))
  const degreesOfFreedom = safeDays.length - 2
  const residualVariance = degreesOfFreedom > 0 ? bestSse / degreesOfFreedom : 0

  return {
    L: bestL,
    k: bestK,
    fittedValues,
    residualVariance,
  }
}

export function generateProjection({
  L,
  k,
  lastObservedDay,
  horizonDays,
  residualVariance = 0,
}) {
  const projectedCurve = {}
  const upperCI = {}
  const lowerCI = {}
  const safeL = Number.isFinite(Number(L)) ? Number(L) : 0
  const safeK = Math.max(MIN_K, Number.isFinite(Number(k)) ? Number(k) : MIN_K)
  const safeLastObservedDay = Math.max(0, Number(lastObservedDay) || 0)
  const safeHorizonDays = Math.max(safeLastObservedDay, Number(horizonDays) || safeLastObservedDay)
  const baseMargin = 1.96 * Math.sqrt(Math.max(0, Number(residualVariance) || 0))

  for (let day = safeLastObservedDay + 1; day <= safeHorizonDays; day += 1) {
    const prediction = modelValue(day, safeL, safeK)
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
