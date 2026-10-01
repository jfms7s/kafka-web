import type { Decoded } from "../api/hooks/messages";
import { decodedText, truncate } from "./decoded";

export function Base64Badge() {
  return (
    <span className="rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-800">
      base64
    </span>
  );
}

const NULL_LABEL = <span className="text-slate-400 italic">null</span>;

/** One line: what a key or value looks like in a table cell. */
export function DecodedPreview({ value }: { value: Decoded }) {
  if (value.encoding === "null") return NULL_LABEL;
  return (
    <span className="flex min-w-0 items-center gap-2">
      {value.encoding === "base64" && <Base64Badge />}
      <span className="truncate">{truncate(decodedText(value))}</span>
    </span>
  );
}

/**
 * An integer literal of 16+ digits: `JSON.parse` rounds those beyond 2^53, so re-serialising
 * `json_value` would show a different number than the one in the message. (Also matches digits
 * inside strings, which only costs the pretty-printing.)
 */
const LARGE_INTEGER = /(^|[^\d.eE+-])-?\d{16,}(?![\d.eE])/;

/** The whole thing: pretty-printed when JSON (and lossless), raw text otherwise, flagged when binary. */
export function DecodedValue({ value }: { value: Decoded }) {
  if (value.encoding === "null") return NULL_LABEL;
  const raw = decodedText(value);
  const body =
    value.is_json && !LARGE_INTEGER.test(raw)
      ? JSON.stringify(value.json_value, null, 2)
      : raw;
  return (
    <div className="space-y-1">
      {value.encoding === "base64" && <Base64Badge />}
      <pre className="max-h-96 overflow-auto rounded bg-slate-100 p-2 font-mono text-xs break-all whitespace-pre-wrap">
        {body}
      </pre>
    </div>
  );
}
