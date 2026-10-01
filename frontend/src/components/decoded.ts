import type { Decoded, MessageView } from '../api/hooks/messages'

export const PREVIEW_LENGTH = 120

/** The text a decoded key/value is searched and previewed by ('' for null). */
export const decodedText = (d: Decoded): string => d.data ?? ''

/** At most `PREVIEW_LENGTH` characters of `text`, marked with an ellipsis when cut. */
export function truncate(text: string, max = PREVIEW_LENGTH): string {
  return text.length > max ? `${text.slice(0, max)}…` : text
}

/** ISO-8601 UTC, or '—' when the message has no timestamp. */
export function formatTimestamp(timestamp: number | null): string {
  if (timestamp === null) return '—'
  const date = new Date(timestamp)
  return Number.isNaN(date.getTime()) ? String(timestamp) : date.toISOString()
}

/** A message's identity within one topic. */
export const messageId = (m: MessageView) => `${m.partition}:${m.offset}`
