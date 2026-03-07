import React, { useEffect, useState } from 'react'
import { generateProjection } from '../utils/ltvPrediction'

export default function TunePredictionPane({
    isOpen,
    onClose,
    predictions,
    displayRows,
    effectiveMaxDay,
    predictionHorizon,
    onUpdate,
}) {
    const [localParams, setLocalParams] = useState({})

    // Sync local params when predictions or open state changes
    useEffect(() => {
        if (isOpen && predictions) {
            const initialParams = {}
            Object.entries(predictions).forEach(([cohortId, pred]) => {
                initialParams[cohortId] = {
                    A: Number(pred.A).toFixed(2),
                    B: Number(pred.B).toFixed(2),
                }
            })
            setLocalParams(initialParams)
        }
    }, [isOpen, predictions])

    const handleUpdate = () => {
        if (!predictions) return

        const updatedPredictions = structuredClone(predictions)

        // Recompute generated curves for each cohort using localParams
        Object.entries(localParams).forEach(([cohortId, params]) => {
            const A = Number(params.A)
            const B = Number(params.B)
            const originalPrediction = predictions[cohortId]

            if (!originalPrediction) return

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

        onUpdate(updatedPredictions)
        onClose()
    }

    const handleParamChange = (cohortId, paramName, value) => {
        setLocalParams((prev) => ({
            ...prev,
            [cohortId]: {
                ...prev[cohortId],
                [paramName]: value,
            },
        }))
    }

    if (!isOpen) return null

    return (
        <>
            <div className="tuning-pane-overlay open" onClick={onClose} aria-hidden="true" />
            <div className="tuning-pane open">
                <div className="tuning-pane-header">
                    <h3>Tune Prediction Parameters</h3>
                    <button type="button" className="tuning-close" onClick={onClose}>
                        &times;
                    </button>
                </div>

                <div className="tuning-pane-content">
                    <p className="secondary-text">
                        Adjust the mathematically derived parameters for the Power Law formula <br />
                        <code>y = A * day^B</code>
                    </p>

                    {displayRows.map((row) => {
                        const cohortId = row.cohort_id
                        const params = localParams[cohortId]

                        if (!params) return null

                        return (
                            <div key={cohortId} className="tuning-cohort-block">
                                <h4>{row.cohort_name}</h4>
                                <div className="tuning-input-group">
                                    <div className="tuning-input-row">
                                        <label style={{ flex: '0 0 110px' }}>Multiplier (A):</label>
                                        <input
                                            type="number"
                                            step="0.01"
                                            value={params.A}
                                            onChange={(e) => handleParamChange(cohortId, 'A', e.target.value)}
                                        />
                                    </div>
                                    <div className="tuning-input-row">
                                        <label style={{ flex: '0 0 110px' }}>Exponent (B):</label>
                                        <input
                                            type="number"
                                            step="0.01"
                                            min="0.01"
                                            max="1.5"
                                            value={params.B}
                                            onChange={(e) => handleParamChange(cohortId, 'B', e.target.value)}
                                        />
                                    </div>
                                </div>
                            </div>
                        )
                    })}
                </div>

                <div className="tuning-actions">
                    <button type="button" className="button button-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button type="button" className="button button-primary" onClick={handleUpdate}>
                        Update Projections
                    </button>
                </div>
            </div>
        </>
    )
}
