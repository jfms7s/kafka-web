import type { Decoded } from '../api/hooks/messages'
import { decodedText, truncate } from './decoded'

export function Base64Badge() {
  return (
    <span className="rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-800">
      base64
    </span>
  )
}

const NULL_LABEL = <span className="text-slate-400 italic">null</span>

/** One line: what a key or value looks like in a table cell. */
export function DecodedPreview({ value }: { value: Decoded }) {
  if (value.encoding === 'null') return NULL_LABEL
  return (
    <span className="flex min-w-0 items-center gap-2">
      {value.encoding === 'base64' && <Base64Badge />}
      <span className="truncate">{truncate(decodedText(value))}</span>
    </span>
  )
}

/** The whole thing: pretty-printed when JSON, raw text otherwise, flagged when binary. */
export function DecodedValue({ value }: { value: Decoded }) {
  if (value.encoding === 'null') return NULL_LABEL
  const body = value.is_json ? JSON.stringify(value.json_value, null, 2) : decodedText(value)
  return (
    <div className="space-y-1">
      {value.encoding === 'base64' && <Base64Badge />}
      <pre className="max-h-96 overflow-auto rounded bg-slate-100 p-2 font-mono text-xs break-all whitespace-pre-wrap">
        {body}
      </pre>
    </div>
  )
}
