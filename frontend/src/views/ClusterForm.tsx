import { useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { Link, useNavigate, useParams } from 'react-router'
import { ApiError } from '../api/client'
import {
  useCluster,
  useCreateCluster,
  useTestCluster,
  useUpdateCluster,
} from '../api/hooks/clusters'
import type { ClusterInput, ClusterView } from '../api/hooks/clusters'
import { useToast } from '../components/Toasts'

type Protocol = ClusterInput['security_protocol'] & string
type Mechanism = ClusterInput['sasl_mechanism'] & string

const PROTOCOLS: Protocol[] = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']
const MECHANISMS: Mechanism[] = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
const TRUSTSTORE_REQUIRED = 'Truststore is required for TLS'
const KEEP_HINT = 'Leave blank to keep the saved value'

const usesTls = (p: Protocol) => p === 'SSL' || p === 'SASL_SSL'
const usesSasl = (p: Protocol) => p === 'SASL_PLAINTEXT' || p === 'SASL_SSL'

interface FormState {
  name: string
  env: string
  region: string
  bootstrapServers: string
  protocol: Protocol
  mechanism: Mechanism
  username: string
  password: string
  pastedTruststore: string
  truststorePassword: string
  readOnly: boolean
  extra: string
}

type FieldErrors = Partial<Record<string, string>>
type TestResult = { ok: true } | { ok: false; message: string }

const formatExtra = (extra: Record<string, string>) =>
  Object.entries(extra)
    .map(([key, value]) => `${key}=${value}`)
    .join('\n')

/** Parse `key=value` lines; returns the failing line number instead on malformed input. */
function parseExtra(text: string): { value: Record<string, string> } | { badLine: number } {
  const value: Record<string, string> = {}
  const lines = text.split('\n')
  for (const [index, line] of lines.entries()) {
    if (!line.trim()) continue
    const eq = line.indexOf('=')
    const key = eq > 0 ? line.slice(0, eq).trim() : ''
    if (!key) return { badLine: index + 1 }
    value[key] = line.slice(eq + 1).trim()
  }
  return { value }
}

function readAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onerror = () => reject(reader.error ?? new Error('Could not read the file'))
    reader.onload = () => {
      const url = String(reader.result)
      resolve(url.slice(url.indexOf(',') + 1)) // strip the "data:<type>;base64," prefix
    }
    reader.readAsDataURL(file)
  })
}

export function ClusterForm() {
  const { name } = useParams()
  return name ? <EditLoader name={name} /> : <FormBody />
}

function EditLoader({ name }: { name: string }) {
  const { data, error, isPending } = useCluster(name)
  if (isPending) return <p className="text-slate-500">Loading cluster…</p>
  if (error) {
    return (
      <p className="text-red-700">
        Could not load cluster “{name}”: {error.message}.{' '}
        <Link to="/" className="underline">
          Back to clusters
        </Link>
      </p>
    )
  }
  return <FormBody key={name} saved={data} />
}

function initialState(saved?: ClusterView): FormState {
  return {
    name: saved?.name ?? '',
    env: saved?.env ?? '',
    region: saved?.region ?? '',
    bootstrapServers: saved?.bootstrap_servers ?? '',
    protocol: saved?.security_protocol ?? 'PLAINTEXT',
    mechanism: saved?.sasl_mechanism ?? 'PLAIN',
    username: saved?.sasl_username ?? '',
    password: '',
    pastedTruststore: '',
    truststorePassword: '',
    readOnly: saved?.read_only ?? false,
    extra: formatExtra(saved?.extra ?? {}),
  }
}

function FormBody({ saved }: { saved?: ClusterView }) {
  const editing = saved !== undefined
  const navigate = useNavigate()
  const toast = useToast()
  const create = useCreateCluster()
  const update = useUpdateCluster(saved?.name ?? '')
  const testConnection = useTestCluster()

  const [form, setForm] = useState<FormState>(() => initialState(saved))
  const [file, setFile] = useState<{ name: string; base64: string } | null>(null)
  const [reading, setReading] = useState(false)
  const [readOnlyTouched, setReadOnlyTouched] = useState(false)
  const [errors, setErrors] = useState<FieldErrors>({})
  const [testResult, setTestResult] = useState<TestResult | null>(null)

  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((current) => ({ ...current, [key]: value }))

  const tls = usesTls(form.protocol)
  const sasl = usesSasl(form.protocol)
  const hasSavedTruststore = editing && (saved.truststore?.length ?? 0) > 0

  function changeEnv(env: string) {
    setForm((current) => ({
      ...current,
      env,
      readOnly: !editing && !readOnlyTouched && env === 'prd' ? true : current.readOnly,
    }))
  }

  async function chooseFile(chosen: File | undefined) {
    if (!chosen) {
      setFile(null)
      return
    }
    setReading(true)
    try {
      setFile({ name: chosen.name, base64: await readAsBase64(chosen) })
    } catch (error) {
      toast.error(error)
    } finally {
      setReading(false)
    }
  }

  /** Validate and build the request body; null (with `errors` set) when invalid. */
  function build(): ClusterInput | null {
    const problems: FieldErrors = {}
    const truststore = file?.base64 ?? form.pastedTruststore.trim()
    if (tls && !truststore && !hasSavedTruststore) problems.truststore = TRUSTSTORE_REQUIRED
    const extra = parseExtra(form.extra)
    if ('badLine' in extra) problems.extra = `Line ${extra.badLine} must look like key=value`
    setErrors(problems)
    if (Object.keys(problems).length > 0 || !('value' in extra)) return null

    const input: ClusterInput = {
      name: form.name,
      env: form.env.trim(),
      region: form.region.trim() || null,
      bootstrap_servers: form.bootstrapServers.trim(),
      security_protocol: form.protocol,
      read_only: form.readOnly,
      extra: extra.value,
    }
    if (sasl) {
      input.sasl_mechanism = form.mechanism
      input.sasl_username = form.username
      if (form.password) input.sasl_password = form.password
    }
    if (tls) {
      if (truststore) input.truststore_base64 = truststore
      if (form.truststorePassword) input.truststore_password = form.truststorePassword
    }
    return input
  }

  function reportServerError(error: unknown) {
    if (error instanceof ApiError && error.field) {
      setErrors({ [error.field]: error.message })
    } else {
      toast.error(error)
    }
  }

  async function submit(event: FormEvent) {
    event.preventDefault()
    const input = build()
    if (!input) return
    try {
      await (editing ? update.mutateAsync(input) : create.mutateAsync(input))
      await navigate('/')
    } catch (error) {
      reportServerError(error)
    }
  }

  async function runTest() {
    const input = build()
    if (!input) return
    setTestResult(null)
    try {
      await testConnection.mutateAsync({ input, existing: saved?.name })
      setTestResult({ ok: true })
    } catch (error) {
      if (error instanceof ApiError && error.field) setErrors({ [error.field]: error.message })
      setTestResult({ ok: false, message: error instanceof Error ? error.message : 'Test failed' })
    }
  }

  const fieldProps = (field: string) => ({
    id: `f-${field}`,
    'aria-invalid': errors[field] ? true : undefined,
    'aria-describedby': errors[field] ? `f-${field}-error` : undefined,
  })
  const fieldError = (field: string) =>
    errors[field] ? (
      <p id={`f-${field}-error`} className="mt-1 text-sm text-red-700">
        {errors[field]}
      </p>
    ) : null

  const saving = create.isPending || update.isPending

  return (
    <form onSubmit={submit} className="max-w-2xl space-y-5">
      <h1 className="text-2xl font-semibold text-slate-900">
        {editing ? `Edit cluster ${saved.name}` : 'Add cluster'}
      </h1>

      <Field label="Name" htmlFor="f-name" error={fieldError('name')}>
        <input
          {...fieldProps('name')}
          required
          disabled={editing}
          pattern="[a-z0-9][a-z0-9\-]{0,62}"
          title="Lowercase letters, digits and hyphens; cannot be changed later"
          value={form.name}
          onChange={(e) => set('name', e.target.value)}
          className={INPUT}
        />
      </Field>
      <Field label="Environment" htmlFor="f-env" error={fieldError('env')}>
        <input
          {...fieldProps('env')}
          required
          placeholder="qa, stg, prd…"
          value={form.env}
          onChange={(e) => changeEnv(e.target.value)}
          className={INPUT}
        />
      </Field>
      <Field label="Region" htmlFor="f-region" error={fieldError('region')}>
        <input
          {...fieldProps('region')}
          placeholder="optional"
          value={form.region}
          onChange={(e) => set('region', e.target.value)}
          className={INPUT}
        />
      </Field>
      <Field label="Bootstrap servers" htmlFor="f-bootstrap_servers" error={fieldError('bootstrap_servers')}>
        <input
          {...fieldProps('bootstrap_servers')}
          required
          placeholder="host1:9092,host2:9092"
          value={form.bootstrapServers}
          onChange={(e) => set('bootstrapServers', e.target.value)}
          className={INPUT}
        />
      </Field>
      <Field label="Security protocol" htmlFor="f-security_protocol" error={fieldError('security_protocol')}>
        <select
          {...fieldProps('security_protocol')}
          value={form.protocol}
          onChange={(e) => set('protocol', e.target.value as Protocol)}
          className={INPUT}
        >
          {PROTOCOLS.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
      </Field>

      {sasl && (
        <fieldset className="space-y-5 rounded border border-slate-200 p-4">
          <legend className="px-1 text-sm font-medium text-slate-600">SASL</legend>
          <Field label="SASL mechanism" htmlFor="f-sasl_mechanism" error={fieldError('sasl_mechanism')}>
            <select
              {...fieldProps('sasl_mechanism')}
              value={form.mechanism}
              onChange={(e) => set('mechanism', e.target.value as Mechanism)}
              className={INPUT}
            >
              {MECHANISMS.map((m) => (
                <option key={m}>{m}</option>
              ))}
            </select>
          </Field>
          <Field label="SASL username" htmlFor="f-sasl_username" error={fieldError('sasl_username')}>
            <input
              {...fieldProps('sasl_username')}
              required
              autoComplete="off"
              value={form.username}
              onChange={(e) => set('username', e.target.value)}
              className={INPUT}
            />
          </Field>
          <Field label="SASL password" htmlFor="f-sasl_password" error={fieldError('sasl_password')}>
            <input
              {...fieldProps('sasl_password')}
              type="password"
              required={!editing}
              autoComplete="new-password"
              placeholder={editing ? KEEP_HINT : ''}
              value={form.password}
              onChange={(e) => set('password', e.target.value)}
              className={INPUT}
            />
          </Field>
        </fieldset>
      )}

      {tls && (
        <fieldset className="space-y-5 rounded border border-slate-200 p-4">
          <legend className="px-1 text-sm font-medium text-slate-600">Truststore</legend>
          {hasSavedTruststore && (
            <div className="text-sm text-slate-700">
              <p className="font-medium">Saved truststore</p>
              <ul className="mt-1 list-disc pl-5">
                {saved.truststore?.map((cert) => (
                  <li key={`${cert.subject}${cert.not_after}`}>
                    {cert.subject} — expires {cert.not_after.slice(0, 10)}
                  </li>
                ))}
              </ul>
            </div>
          )}
          <Field label="Truststore file" htmlFor="f-truststore_file">
            <input
              id="f-truststore_file"
              type="file"
              onChange={(e) => void chooseFile(e.target.files?.[0])}
              className="block w-full text-sm"
            />
          </Field>
          <Field label="Truststore (base64)" htmlFor="f-truststore" error={fieldError('truststore')}>
            <textarea
              {...fieldProps('truststore')}
              rows={3}
              disabled={file !== null}
              placeholder={
                editing ? 'Leave blank to keep the saved truststore' : 'or paste the base64 content'
              }
              value={form.pastedTruststore}
              onChange={(e) => set('pastedTruststore', e.target.value)}
              className={`${INPUT} font-mono text-xs`}
            />
          </Field>
          <Field label="Truststore password" htmlFor="f-truststore_password" error={fieldError('truststore_password')}>
            <input
              {...fieldProps('truststore_password')}
              type="password"
              autoComplete="new-password"
              placeholder={editing ? 'Only needed with a new truststore; leave blank to keep' : 'if the store has one'}
              value={form.truststorePassword}
              onChange={(e) => set('truststorePassword', e.target.value)}
              className={INPUT}
            />
          </Field>
        </fieldset>
      )}

      <Field label="Extra properties (one key=value per line)" htmlFor="f-extra" error={fieldError('extra')}>
        <textarea
          {...fieldProps('extra')}
          rows={3}
          placeholder="raw librdkafka properties"
          value={form.extra}
          onChange={(e) => set('extra', e.target.value)}
          className={`${INPUT} font-mono text-xs`}
        />
      </Field>

      <label className="flex items-center gap-2 text-sm text-slate-800">
        <input
          type="checkbox"
          checked={form.readOnly}
          onChange={(e) => {
            setReadOnlyTouched(true)
            set('readOnly', e.target.checked)
          }}
        />
        Read-only
      </label>

      <div className="flex flex-wrap items-center gap-3">
        <button
          type="submit"
          disabled={saving || reading}
          className="rounded bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-700 disabled:opacity-50"
        >
          Save
        </button>
        <button
          type="button"
          onClick={() => void runTest()}
          disabled={testConnection.isPending || reading}
          className="rounded border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50 disabled:opacity-50"
        >
          Test connection
        </button>
        <Link to="/" className="px-2 py-2 text-sm text-slate-600 underline">
          Cancel
        </Link>
        <span role="status" className="text-sm">
          {testConnection.isPending && <span className="text-slate-500">Testing…</span>}
          {testResult?.ok === true && <span className="text-green-700">Connection OK</span>}
          {testResult?.ok === false && (
            <span className="text-red-700">Connection failed: {testResult.message}</span>
          )}
        </span>
      </div>
    </form>
  )
}

const INPUT =
  'mt-1 block w-full rounded border border-slate-300 px-3 py-2 text-sm disabled:bg-slate-100 disabled:text-slate-500'

function Field(props: { label: string; htmlFor: string; error?: ReactNode; children: ReactNode }) {
  return (
    <div>
      <label htmlFor={props.htmlFor} className="block text-sm font-medium text-slate-700">
        {props.label}
      </label>
      {props.children}
      {props.error}
    </div>
  )
}
