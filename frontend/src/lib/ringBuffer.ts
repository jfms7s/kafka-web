/**
 * `buf` followed by `items`, keeping only the last `cap` entries (oldest dropped first).
 * Returns a new array; neither input is modified.
 */
export function pushCapped<T>(buf: readonly T[], items: readonly T[], cap = 5000): T[] {
  if (items.length >= cap) return items.slice(items.length - cap)
  const keepFromBuf = Math.max(0, buf.length + items.length - cap)
  return [...buf.slice(keepFromBuf), ...items]
}
