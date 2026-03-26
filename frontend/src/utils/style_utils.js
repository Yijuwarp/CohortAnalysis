/**
 * Computes a dynamic style for a cell based on its data availability ratio.
 * 1.0 -> white (fully available)
 * 0.5 -> yellow (partial)
 * 0.0 -> red (unavailable)
 * 
 * @param {number} ratio - The availability ratio (eligible_users / cohort_size)
 * @returns {object} - React style object
 */
export function getAvailabilityStyle(ratio) {
  if (ratio >= 0.999) return {};

  // Clamp ratio between 0 and 1
  const r = Math.max(0, Math.min(1, ratio));

  // Hue: yellow (60) → red (0)
  const hue = r * 60;

  // Saturation increases as availability drops (70% → 90%)
  const saturation = 70 + (1 - r) * 20;

  // Lightness decreases slightly as availability drops (95% → 75%)
  const lightness = 95 - (1 - r) * 20;

  // Alpha for subtle layering (0.15 → 0.4)
  const alpha = 0.15 + (1 - r) * 0.25;

  return {
    backgroundColor: `hsla(${hue}, ${saturation}%, ${lightness}%, ${alpha})`
  };
}
