/** The existing holdings schema stores six decimal places. Never silently round
 * user input or edit quantities beyond the Number-based table's reliable range. */
export function thesisQuantityWire(value: number): string {
  if (!Number.isFinite(value) || Math.abs(value) > 1_000_000_000) {
    throw new Error("This quantity cannot be edited safely in the thesis table.");
  }
  const rounded = Math.round(value * 1e6);
  if (Math.abs(value * 1e6 - rounded) > 0.00001) {
    throw new Error("Thesis quantities support at most six decimal places.");
  }
  return value.toFixed(6);
}
