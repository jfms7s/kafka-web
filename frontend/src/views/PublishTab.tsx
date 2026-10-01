import { useEffect, useId, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { ApiError } from '../api/client'
import { useCluster } from '../api/hooks/clusters'
import { MAX_UPLOAD_BYTES, usePublishBulk, usePublishMessage } from '../api/hooks/publish'
import type { BatchResult } from '../api/hooks/publish'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { useToast } from '../components/Toasts'

const INPUT = 'w-full rounded border border-slate-300 px-2 py-1.5 text-sm'
const BUTTON =
  'rounded border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40'
const MAX_FAILURES_SHOWN = 200
const SNIFF_BYTES = 4096

type FieldErrors = Partial<Record<string, string>>

interface FieldProps {
  label: string
  hint?: string
  error?: string
  /** Receives the props that tie the control to its label, hint and error. */
  children: (props: {
    id: string
    'aria-invalid'?: true
    'aria-describedby'?: string
  }) => ReactNode
}

function Field({ label, hint, error, children }: FieldProps) {
  const id = useId()
  const hintId = `${id}-hint`
  const errorId = `${id}-error`
  const describedBy = [hint ? hintId : null, error ? errorId : null].filter(Boolean).join(' ')
  return (
    <div className="flex flex-col gap-1 text-sm text-slate-600">
      <label htmlFor={id}>{label}</label>
      {children({
        id,
        'aria-invalid': error ? true : undefined,
        'aria-describedby': describedBy || undefined,
      })}
      {hint && (
        <p id={hintId} className="text-xs text-slate-500">
          {hint}
        </p>
      )}
      {error && (
        <p id={errorId} className="text-sm text-red-700">
          {error}
        </p>
      )}
    </div>
  )
}

/** Same rule as the server: a first non-blank character of `[` means JSON, anything else CSV. */
const BOM = String.fromCharCode(0xfeff)
const looksLikeCsv = (head: string) =>
  !(head.startsWith(BOM) ? head.slice(1) : head).trimStart().startsWith('[')

function readHead(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result))
    reader.onerror = () => reject(reader.error)
    reader.readAsText(file.slice(0, SNIFF_BYTES))
  })
}

export function PublishTab({ cluster, topic }: { cluster: string; topic: string }) {
  const { data, error, isPending } = useCluster(cluster)

  if (isPending) return <p className="text-slate-500">Loading…</p>
  if (error) return <p className="text-red-700">Could not load the cluster: {error.message}</p>
  if (data.read_only) {
    return (
      <p className="rounded border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900">
        Cluster <span className="font-mono">{cluster}</span> is read-only, so messages cannot be
        published to it. Clear its read-only setting in the cluster configuration to publish.
      </p>
    )
  }
  return (
    <div className="space-y-10">
      <SinglePublish cluster={cluster} topic={topic} />
      <BulkUpload cluster={cluster} topic={topic} />
    </div>
  )
}

const SINGLE_FIELDS = ['key', 'value', 'headers', 'partition']

function SinglePublish({ cluster, topic }: { cluster: string; topic: string }) {
  const toast = useToast()
  const publish = usePublishMessage(cluster, topic)
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [headers, setHeaders] = useState('')
  const [partition, setPartition] = useState('')
  const [errors, setErrors] = useState<FieldErrors>({})

  const submit = (event: FormEvent) => {
    event.preventDefault()
    const problems: FieldErrors = {}
    if (value === '') problems.value = 'Value is required'
    if (partition.trim() !== '' && !/^\d+$/.test(partition.trim())) {
      problems.partition = 'Partition must be a whole number, 0 or more'
    }
    setErrors(problems)
    if (Object.keys(problems).length > 0) return

    publish.mutate(
      {
        ...(key !== '' && { key }),
        value,
        ...(headers.trim() !== '' && { headers }),
        ...(partition.trim() !== '' && { partition: Number(partition) }),
      },
      {
        onSuccess: (result) =>
          toast.success(`Published to partition ${result.partition}, offset ${result.offset}.`),
        onError: (failure) => {
          if (failure instanceof ApiError && failure.field && SINGLE_FIELDS.includes(failure.field)) {
            setErrors({ [failure.field]: failure.message })
          } else {
            toast.error(failure)
          }
        },
      },
    )
  }

  return (
    <section aria-labelledby="publish-single">
      <h2 id="publish-single" className="text-lg font-semibold text-slate-900">
        Publish a message
      </h2>
      <form onSubmit={submit} className="mt-3 max-w-2xl space-y-4">
        <Field label="Key" error={errors.key}>
          {(props) => (
            <input {...props} value={key} onChange={(e) => setKey(e.target.value)} className={`${INPUT} font-mono`} />
          )}
        </Field>
        <Field label="Value" error={errors.value}>
          {(props) => (
            <textarea
              {...props}
              rows={6}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              className={`${INPUT} font-mono`}
            />
          )}
        </Field>
        <Field label="Headers" hint="JSON object or key=value lines" error={errors.headers}>
          {(props) => (
            <textarea
              {...props}
              rows={3}
              value={headers}
              onChange={(e) => setHeaders(e.target.value)}
              className={`${INPUT} font-mono`}
            />
          )}
        </Field>
        <Field label="Partition" hint="Optional; leave blank to let the partitioner choose" error={errors.partition}>
          {(props) => (
            <input
              {...props}
              type="text"
              inputMode="numeric"
              value={partition}
              onChange={(e) => setPartition(e.target.value)}
              className={`${INPUT} w-32`}
            />
          )}
        </Field>
        <button type="submit" disabled={publish.isPending} className={BUTTON}>
          {publish.isPending ? 'Publishing…' : 'Publish'}
        </button>
      </form>
    </section>
  )
}

const BULK_FIELDS = ['key_column', 'value_column', 'file']

function BulkUpload({ cluster, topic }: { cluster: string; topic: string }) {
  const toast = useToast()
  const upload = usePublishBulk(cluster, topic)
  const [file, setFile] = useState<File | null>(null)
  const [csv, setCsv] = useState(false)
  const [keyColumn, setKeyColumn] = useState('')
  const [valueColumn, setValueColumn] = useState('')
  const [errors, setErrors] = useState<FieldErrors>({})
  const [result, setResult] = useState<BatchResult | null>(null)
  const [confirming, setConfirming] = useState(false)

  // Whether the columns apply depends on the file's content, which only the browser can peek at.
  useEffect(() => {
    if (!file) return
    let current = true
    readHead(file).then(
      (head) => current && setCsv(looksLikeCsv(head)),
      () => current && setCsv(false),
    )
    return () => {
      current = false
    }
  }, [file])

  const tooLarge = file !== null && file.size > MAX_UPLOAD_BYTES
  const fileError = errors.file ?? (tooLarge ? `${file.name} is larger than 10 MB` : undefined)

  const chooseFile = (chosen: File | null) => {
    setFile(chosen)
    setCsv(false)
    setErrors({})
    setResult(null)
  }

  const send = (typed: string) => {
    setConfirming(false)
    if (!file) return
    setErrors({})
    setResult(null)
    upload.mutate(
      {
        file,
        confirm: typed,
        keyColumn: csv ? keyColumn.trim() : undefined,
        valueColumn: csv ? valueColumn.trim() : undefined,
      },
      {
        onSuccess: setResult,
        onError: (failure) => {
          if (failure instanceof ApiError && failure.field && BULK_FIELDS.includes(failure.field)) {
            setErrors({ [failure.field]: failure.message })
          } else {
            toast.error(failure)
          }
        },
      },
    )
  }

  return (
    <section aria-labelledby="publish-bulk">
      <h2 id="publish-bulk" className="text-lg font-semibold text-slate-900">
        Bulk upload
      </h2>
      <p className="mt-1 text-sm text-slate-600">
        A CSV file (one message per row) or a JSON array of{' '}
        <code className="font-mono">{'{key?, value, headers?}'}</code> objects, up to 10 MB.
      </p>
      <form
        onSubmit={(event) => {
          event.preventDefault()
          setConfirming(true)
        }}
        className="mt-3 max-w-2xl space-y-4"
      >
        <Field label="CSV or JSON file" error={fileError}>
          {(props) => (
            <input
              {...props}
              type="file"
              accept=".csv,.json,text/csv,application/json"
              onChange={(e) => chooseFile(e.target.files?.[0] ?? null)}
              className="text-sm"
            />
          )}
        </Field>
        {file && csv && (
          <div className="flex flex-wrap gap-4">
            <Field label="Key column" hint="Optional" error={errors.key_column}>
              {(props) => (
                <input {...props} value={keyColumn} onChange={(e) => setKeyColumn(e.target.value)} className={INPUT} />
              )}
            </Field>
            <Field
              label="Value column"
              hint="Optional; without it each row is sent as a JSON object"
              error={errors.value_column}
            >
              {(props) => (
                <input {...props} value={valueColumn} onChange={(e) => setValueColumn(e.target.value)} className={INPUT} />
              )}
            </Field>
          </div>
        )}
        <button type="submit" disabled={!file || tooLarge || upload.isPending} className={BUTTON}>
          {upload.isPending ? 'Uploading…' : 'Upload'}
        </button>
      </form>

      {result && <UploadResults result={result} />}

      <ConfirmDialog
        open={confirming}
        title="Upload messages"
        description={`Every row of ${file?.name ?? 'the file'} will be published to ${topic} on ${cluster}. This cannot be undone.`}
        expected={topic}
        confirmLabel="Upload"
        onConfirm={send}
        onCancel={() => setConfirming(false)}
      />
    </section>
  )
}

function UploadResults({ result }: { result: BatchResult }) {
  const failures = result.results.filter((row) => !row.ok)
  const shown = failures.slice(0, MAX_FAILURES_SHOWN)
  return (
    <section aria-label="Upload results" className="mt-6 space-y-3">
      <p className="flex gap-4 text-sm font-medium">
        <span className="text-green-800">{result.succeeded} succeeded</span>
        <span className={result.failed > 0 ? 'text-red-700' : 'text-slate-600'}>
          {result.failed} failed
        </span>
      </p>
      {shown.length > 0 && (
        <table className="w-full max-w-2xl border-collapse overflow-hidden rounded-lg border border-slate-200 bg-white text-sm">
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className="px-3 py-2 font-medium">Row</th>
              <th className="px-3 py-2 font-medium">Error</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((row) => (
              <tr key={row.row} className="border-t border-slate-100">
                <td className="px-3 py-2 font-mono">{row.row}</td>
                <td className="px-3 py-2">{row.error}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {failures.length > shown.length && (
        <p className="text-sm text-slate-600">… and {failures.length - shown.length} more failed rows.</p>
      )}
    </section>
  )
}
