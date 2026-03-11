import { useEffect, useState } from 'react'
import { generateProjection } from '../utils/ltvPrediction'

export default function TunePredictionPane({
  isOpen,
  onClose,
  onCancel,
  predictions,
  displayRows,
  onLiveUpdate,
}) {
  const [localParams, setLocalParams] = useState({})

  useEffect(() => {
    if (!predictions) {
      setLocalParams({})
      return
    }
    const initialParams = {}
    Object.entries(predictions).forEach(([cohortId, pred]) => {
      initialParams[cohortId] = {
        A: Number(pred.A).toFixed(2),
        B: Number(pred.B).toFixed(2),
      }
    })
    setLocalParams(initialParams)
  }, [predictions])

  const applyLiveUpdate = (nextParams) => {
    if (!predictions) return

    const updatedPredictions = JSON.parse(JSON.stringify(predictions))

    Object.entries(nextParams).forEach(([cohortId, params]) => {
      const A = Number(params.A)
      const B = Number(params.B)
      const originalPrediction = predictions[cohortId]
      if (!originalPrediction || !Number.isFinite(A) || !Number.isFinite(B)) return

      const projection = generateProjection({
        A,
        B,
        lastObservedDay: originalPrediction.lastObservedDay,
        horizonDays: 365,
        residualVariance: originalPrediction.residualVariance || 0,
      })

      updatedPredictions[cohortId] = {
        ...originalPrediction,
        A,
        B,
        projectedCurve: projection.projectedCurve,
        upperCI: projection.upperCI,
        lowerCI: projection.lowerCI,
      }
    })

    onLiveUpdate(updatedPredictions)
  }

  const handleParamChange = (cohortId, key, value) => {
    setLocalParams((prev) => {
      const nextParams = {
        ...prev,
        [cohortId]: {
          ...prev[cohortId],
          [key]: value,
        },
      }
      applyLiveUpdate(nextParams)
      return nextParams
    })
  }

  if (!isOpen) return null

  return (
    <aside className="tuning-pane-inline open">
      <div className="tuning-pane-header">
        <h3>Tune Prediction</h3>
        <button type="button" className="tuning-close" onClick={onClose}>&times;</button>
      </div>

      <div className="tuning-pane-content">
            {displayRows.map((row) => {
              const cohortId = row.cohort_id
              const params = localParams[cohortId]
              if (!params) return null
              return (
                <div key={cohortId} className="tuning-cohort-block">
                  <h4>{row.cohort_name}</h4>
                  <div className="tuning-input-group">
                    <div className="tuning-input-row">
                      <label>A</label>
                      <input type="number" step="0.01" value={params.A} onChange={(e) => handleParamChange(cohortId, 'A', e.target.value)} />
                    </div>
                    <div className="tuning-input-row">
                      <label>B</label>
                      <input type="number" step="0.01" min="0.01" max="1.5" value={params.B} onChange={(e) => handleParamChange(cohortId, 'B', e.target.value)} />
                    </div>
                  </div>
                </div>
              )
            })}
          </div>

      <div className="tuning-actions">
        <button type="button" className="button button-secondary" onClick={onCancel}>Cancel</button>
      </div>
    </aside>
  )
}
