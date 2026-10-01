import { useMemo, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { useTopicConfig } from '../api/hooks/topics'
import { LIVE_BUFFER_SIZE, useLiveStream } from '../api/hooks/useLiveStream'
import type { LiveParams, LiveStatus } from '../api/hooks/useLiveStream'
import { MessageTable } from '../components/MessageTable'

type StartMode = LiveParams['start']

interface Form {
  start: StartMode
  offset: string
  timestamp: string // datetime-local value
  partition: string // '' = all partitions
}

const INITIAL: Form = { start: 'latest', offset: '', timestamp: '', partition: '' }

const INPUT = 'rounded border border-slate-300 px-2 py-1.5 text-sm'
const BUTTON = 'rounded px-4 py-1.5 text-sm disabled:opacity-40'

const STATUS_STYLE: Record<LiveStatus, string> = {
  idle: 'bg-slate-100 text-slate-600',
  connecting: 'bg-amber-100 text-amber-800',
  live: 'bg-green-100 text-green-800',
  closed: 'bg-slate-200 text-slate-700',
  error: 'bg-red-100 text-red-800',
}

const isInteger = (text: string) => /^-?\d+$/.test(text.trim())
const epochMs = (local: string) => new Date(local).getTime()

/** The first thing missing before a stream can start, or null when the form is complete. */
function missingInput(form: Form): string | null {
  if (form.start === 'offset') {
    if (form.partition === '') return 'start=offset needs a partition.'
    if (!isInteger(form.offset)) return 'start=offset needs an offset.'
  }
  if (form.start === 'timestamp' && Number.isNaN(epochMs(form.timestamp))) {
    return 'start=timestamp needs a date and time.'
  }
  return null
}

/** Only the parameters that apply to the chosen start mode. */
function toParams(form: Form): LiveParams {
  const params: LiveParams = { start: form.start }
  if (form.start === 'offset') params.offset = Number(form.offset)
  if (form.start === 'timestamp') params.timestamp = epochMs(form.timestamp)
  if (form.partition !== '') params.partition = Number(form.partition)
  return params
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="flex flex-col gap-1 text-sm text-slate-600">
      {label}
      {children}
    </label>
  )
}

export function LiveTab({ cluster, topic }: { cluster: string; topic: string }) {
  const [form, setForm] = useState<Form>(INITIAL)
  const [autoScroll, setAutoScroll] = useState(true)
  const partitions = useTopicConfig(cluster, topic).data?.partitions
  const params = useMemo(() => toParams(form), [form])
  const live = useLiveStream({ cluster, topic, params })

  const set = <K extends keyof Form>(key: K, value: Form[K]) =>
    setForm((current) => ({ ...current, [key]: value }))

  const running = live.status === 'connecting' || live.status === 'live'
  const missing = missingInput(form)
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (missing === null && !running) live.start()
  }

  const count = live.messages.length
  return (
    <div className="space-y-4">
      <form onSubmit={submit} className="flex flex-wrap items-end gap-4">
        <Field label="Start">
          <select
            value={form.start}
            onChange={(e) => set('start', e.target.value as StartMode)}
            disabled={running}
            className={INPUT}
          >
            <option value="latest">latest (new messages only)</option>
            <option value="offset">offset</option>
            <option value="timestamp">timestamp</option>
          </select>
        </Field>
        {form.start === 'offset' && (
          <Field label="Offset">
            <input
              type="number"
              min={0}
              value={form.offset}
              onChange={(e) => set('offset', e.target.value)}
              disabled={running}
              className={`${INPUT} w-32`}
            />
          </Field>
        )}
        {form.start === 'timestamp' && (
          <Field label="Timestamp">
            <input
              type="datetime-local"
              value={form.timestamp}
              onChange={(e) => set('timestamp', e.target.value)}
              disabled={running}
              className={INPUT}
            />
          </Field>
        )}
        <Field label="Partition">
          <select
            value={form.partition}
            onChange={(e) => set('partition', e.target.value)}
            disabled={running}
            className={INPUT}
          >
            <option value="">All partitions</option>
            {partitions?.map((p) => (
              <option key={p.id} value={p.id}>
                {p.id}
              </option>
            ))}
          </select>
        </Field>
        <button
          type="submit"
          disabled={missing !== null || running}
          className={`${BUTTON} bg-slate-900 text-white hover:bg-slate-700`}
        >
          Start
        </button>
        <button
          type="button"
          onClick={live.stop}
          disabled={!running}
          className={`${BUTTON} border border-slate-300 hover:bg-slate-50`}
        >
          Stop
        </button>
        {missing && <p className="self-center text-sm text-slate-500">{missing}</p>}
      </form>

      <div className="flex flex-wrap items-center gap-4 text-sm">
        <span
          role="status"
          aria-label="Stream status"
          className={`rounded-full px-2.5 py-0.5 font-medium ${STATUS_STYLE[live.status]}`}
        >
          {live.status}
        </span>
        <span className="text-slate-600">
          {count} {count === 1 ? 'message' : 'messages'}
        </span>
        <span className="text-slate-500">
          (the browser keeps the last {LIVE_BUFFER_SIZE.toLocaleString('en-US')})
        </span>
        <span className="text-slate-600">Dropped by server: {live.dropped}</span>
        <label className="ml-auto flex items-center gap-2 text-slate-600">
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
          />
          Auto-scroll
        </label>
      </div>

      {live.error && (
        <p role="alert" className={live.status === 'error' ? 'text-red-700' : 'text-slate-700'}>
          {live.error.message}
        </p>
      )}
      {count > 0 && <MessageTable messages={live.messages} followTail={autoScroll} />}
    </div>
  )
}
