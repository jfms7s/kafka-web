import { useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { useSnapshot } from '../api/hooks/messages'
import type { MessageView, SnapshotQuery } from '../api/hooks/messages'
import { useTopicConfig } from '../api/hooks/topics'
import { decodedText } from '../components/decoded'
import { MessageTable } from '../components/MessageTable'

type StartMode = NonNullable<SnapshotQuery['start']>

interface Form {
  count: string
  timeout: string
  start: StartMode
  offset: string
  timestamp: string // datetime-local value
  partition: string // '' = all partitions
}

const INITIAL: Form = {
  count: '100',
  timeout: '10',
  start: 'latest',
  offset: '',
  timestamp: '',
  partition: '',
}

const INPUT = 'rounded border border-slate-300 px-2 py-1.5 text-sm'
const BUTTON =
  'rounded border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50 disabled:opacity-40'

const isInteger = (text: string) => /^-?\d+$/.test(text.trim())
const epochMs = (local: string) => new Date(local).getTime()

/** The first thing missing before this form can be fetched, or null when it is complete. */
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

/** Only the parameters that apply to the chosen start mode: stale inputs never leak through. */
function toQuery(form: Form): SnapshotQuery {
  const query: SnapshotQuery = { start: form.start }
  if (form.count !== '') query.count = Number(form.count)
  if (form.timeout !== '') query.timeout = Number(form.timeout)
  if (form.start === 'offset') query.offset = Number(form.offset)
  if (form.start === 'timestamp') query.timestamp = epochMs(form.timestamp)
  if (form.partition !== '') query.partition = Number(form.partition)
  return query
}

function matches(message: MessageView, needle: string): boolean {
  return (
    decodedText(message.key).toLowerCase().includes(needle) ||
    decodedText(message.value).toLowerCase().includes(needle)
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1 text-sm text-slate-600">
      {label}
      {children}
    </label>
  )
}

export function MessagesTab({ cluster, topic }: { cluster: string; topic: string }) {
  const [form, setForm] = useState<Form>(INITIAL)
  const [filter, setFilter] = useState('')
  const [exportNote, setExportNote] = useState<string | null>(null)
  const partitions = useTopicConfig(cluster, topic).data?.partitions
  const snapshot = useSnapshot(cluster, topic)

  const set = <K extends keyof Form>(key: K, value: Form[K]) =>
    setForm((current) => ({ ...current, [key]: value }))

  const loaded = snapshot.data
  const needle = filter.trim().toLowerCase()
  const shown = useMemo(
    () => (loaded && needle ? loaded.filter((m) => matches(m, needle)) : loaded),
    [loaded, needle],
  )

  const missing = missingInput(form)
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (missing === null) snapshot.mutate(toQuery(form))
  }

  const json = () => JSON.stringify(shown ?? [], null, 2)
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(json())
      setExportNote('Copied to the clipboard.')
    } catch {
      setExportNote('Could not copy to the clipboard.')
    }
  }
  const download = () => {
    const url = URL.createObjectURL(new Blob([json()], { type: 'application/json' }))
    const link = document.createElement('a')
    link.href = url
    link.download = `${topic}-messages.json`
    document.body.append(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  const nothingToExport = !shown || shown.length === 0
  return (
    <div className="space-y-4">
      <form onSubmit={submit} className="flex flex-wrap items-end gap-4">
        <Field label="Count">
          <input
            type="number"
            min={1}
            max={10000}
            value={form.count}
            onChange={(e) => set('count', e.target.value)}
            className={`${INPUT} w-24`}
          />
        </Field>
        <Field label="Timeout (s)">
          <input
            type="number"
            min={1}
            max={60}
            value={form.timeout}
            onChange={(e) => set('timeout', e.target.value)}
            className={`${INPUT} w-24`}
          />
        </Field>
        <Field label="Start">
          <select
            value={form.start}
            onChange={(e) => set('start', e.target.value as StartMode)}
            className={INPUT}
          >
            <option value="latest">latest</option>
            <option value="earliest">earliest</option>
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
              className={INPUT}
            />
          </Field>
        )}
        <Field label="Partition">
          <select
            value={form.partition}
            onChange={(e) => set('partition', e.target.value)}
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
          disabled={missing !== null || snapshot.isPending}
          className="rounded bg-slate-900 px-4 py-1.5 text-sm text-white hover:bg-slate-700 disabled:opacity-40"
        >
          Fetch
        </button>
        {missing && <p className="self-center text-sm text-slate-500">{missing}</p>}
      </form>

      {snapshot.isPending && <p className="text-slate-500">Fetching messages…</p>}
      {snapshot.error && (
        <p role="alert" className="text-red-700">
          Could not fetch messages: {snapshot.error.message}
        </p>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <input
          type="search"
          aria-label="Filter messages"
          placeholder="Filter loaded messages…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className={`${INPUT} w-72`}
        />
        {loaded && (
          <p className="text-sm text-slate-500">
            {needle
              ? `${shown?.length ?? 0} of ${loaded.length} messages`
              : `${loaded.length} ${loaded.length === 1 ? 'message' : 'messages'}`}
          </p>
        )}
        <div className="ml-auto flex items-center gap-2">
          <span role="status" className="text-sm text-slate-500">
            {exportNote}
          </span>
          <button type="button" onClick={() => void copy()} disabled={nothingToExport} className={BUTTON}>
            Copy JSON
          </button>
          <button type="button" onClick={download} disabled={nothingToExport} className={BUTTON}>
            Download JSON
          </button>
        </div>
      </div>
      {loaded?.length === 0 && <p className="text-slate-600">No messages found.</p>}
      {loaded && loaded.length > 0 && shown?.length === 0 && (
        <p className="text-slate-600">No loaded message matches “{filter.trim()}”.</p>
      )}
      {shown && shown.length > 0 && <MessageTable messages={shown} />}
    </div>
  )
}
