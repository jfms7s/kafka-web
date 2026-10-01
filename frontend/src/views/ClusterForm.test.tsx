import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '../components/Toasts'
import { ClusterForm } from './ClusterForm'

type Handler = (request: Request) => Response | Promise<Response>
let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>

function mockFetch(handler: Handler = () => Response.json({})) {
  fetchMock = vi.fn(async (request: Request) => handler(request))
  vi.stubGlobal('fetch', fetchMock)
}

const callsTo = (method: string, pathname: string) =>
  fetchMock.mock.calls
    .map(([request]) => request)
    .filter((r) => r.method === method && new URL(r.url).pathname === pathname)
const bodyOf = (request: Request) => request.clone().json() as Promise<Record<string, unknown>>

function renderForm(path = '/clusters/new', prime?: (client: QueryClient) => void) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  prime?.(client)
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <ToastProvider>
          <Routes>
            <Route path="/clusters/new" element={<ClusterForm />} />
            <Route path="/clusters/:name/edit" element={<ClusterForm />} />
            <Route path="/" element={<p>cluster list</p>} />
          </Routes>
        </ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

const SAVED = {
  name: 'stg-eu',
  env: 'stg',
  region: 'eu-west-1',
  bootstrap_servers: 'b1:9094',
  security_protocol: 'SASL_SSL',
  sasl_mechanism: 'SCRAM-SHA-512',
  sasl_username: 'svc',
  has_sasl_password: true,
  read_only: false,
  extra: {},
  truststore: [{ subject: 'CN=Corp Root CA', not_after: '2030-05-17T12:00:00Z' }],
  usable: true,
  unusable_reason: null,
  connected: false,
}

const editHandler: Handler = () => Response.json(SAVED)

async function fillBasics(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText('Name'), 'dev')
  await user.type(screen.getByLabelText('Environment'), 'dev')
  await user.type(screen.getByLabelText('Bootstrap servers'), 'localhost:9092')
}

beforeEach(() => mockFetch())
afterEach(() => vi.unstubAllGlobals())

describe('ClusterForm field visibility', () => {
  it('PLAINTEXT hides SASL and truststore fields', () => {
    renderForm()
    expect(screen.getByLabelText('Security protocol')).toHaveValue('PLAINTEXT')
    expect(screen.queryByLabelText('SASL mechanism')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('SASL username')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('SASL password')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Truststore file')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Truststore (base64)')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Truststore password')).not.toBeInTheDocument()
  })

  it('SASL_SSL shows both SASL and truststore fields', async () => {
    const user = renderForm()
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SASL_SSL')
    expect(screen.getByLabelText('SASL mechanism')).toBeInTheDocument()
    expect(screen.getByLabelText('SASL username')).toBeInTheDocument()
    expect(screen.getByLabelText('SASL password')).toBeInTheDocument()
    expect(screen.getByLabelText('Truststore file')).toBeInTheDocument()
    expect(screen.getByLabelText('Truststore (base64)')).toBeInTheDocument()
    expect(screen.getByLabelText('Truststore password')).toBeInTheDocument()
  })

  it('SSL shows the truststore but not SASL', async () => {
    const user = renderForm()
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SSL')
    expect(screen.getByLabelText('Truststore file')).toBeInTheDocument()
    expect(screen.queryByLabelText('SASL mechanism')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('SASL password')).not.toBeInTheDocument()
  })

  it('SASL_PLAINTEXT shows SASL but not the truststore', async () => {
    const user = renderForm()
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SASL_PLAINTEXT')
    expect(screen.getByLabelText('SASL password')).toBeInTheDocument()
    expect(screen.queryByLabelText('Truststore file')).not.toBeInTheDocument()
  })
})

describe('ClusterForm create', () => {
  it('checks read-only when the env is typed as prd', async () => {
    const user = renderForm()
    const readOnly = screen.getByLabelText('Read-only')
    expect(readOnly).not.toBeChecked()
    await user.type(screen.getByLabelText('Environment'), 'prd')
    expect(readOnly).toBeChecked()
  })

  it('posts only ClusterInput fields', async () => {
    const user = renderForm()
    await fillBasics(user)
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('POST', '/api/clusters')).toHaveLength(1))
    expect(await bodyOf(callsTo('POST', '/api/clusters')[0])).toEqual({
      name: 'dev',
      env: 'dev',
      region: null,
      bootstrap_servers: 'localhost:9092',
      security_protocol: 'PLAINTEXT',
      read_only: false,
      extra: {},
    })
    expect(await screen.findByText('cluster list')).toBeInTheDocument()
  })

  it('refuses SSL without a truststore and does not call the API', async () => {
    const user = renderForm()
    await fillBasics(user)
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SSL')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    expect(await screen.findByText('Truststore is required for TLS')).toBeInTheDocument()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('sends an uploaded truststore file as base64', async () => {
    const user = renderForm()
    await fillBasics(user)
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SSL')
    const bytes = new Uint8Array([0xfe, 0xed, 0xfe, 0xed, 1, 2, 3])
    await user.upload(
      screen.getByLabelText('Truststore file'),
      new File([bytes], 'truststore.jks', { type: 'application/octet-stream' }),
    )
    await user.type(screen.getByLabelText('Truststore password'), 'changeit')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('POST', '/api/clusters')).toHaveLength(1))
    const body = await bodyOf(callsTo('POST', '/api/clusters')[0])
    expect(body.truststore_base64).toBe('/u3+7QECAw==')
    expect(body.truststore_password).toBe('changeit')
  })

  it('sends a pasted base64 truststore', async () => {
    const user = renderForm()
    await fillBasics(user)
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SSL')
    await user.type(screen.getByLabelText('Truststore (base64)'), 'QUJD')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('POST', '/api/clusters')).toHaveLength(1))
    expect((await bodyOf(callsTo('POST', '/api/clusters')[0])).truststore_base64).toBe('QUJD')
  })

  it('sends SASL credentials and parsed extra properties', async () => {
    const user = renderForm()
    await fillBasics(user)
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SASL_PLAINTEXT')
    await user.selectOptions(screen.getByLabelText('SASL mechanism'), 'SCRAM-SHA-256')
    await user.type(screen.getByLabelText('SASL username'), 'svc')
    await user.type(screen.getByLabelText('SASL password'), 'pw')
    await user.type(screen.getByLabelText(/Extra properties/), 'client.id=kw{enter}  fetch.max.bytes = 10')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('POST', '/api/clusters')).toHaveLength(1))
    expect(await bodyOf(callsTo('POST', '/api/clusters')[0])).toMatchObject({
      sasl_mechanism: 'SCRAM-SHA-256',
      sasl_username: 'svc',
      sasl_password: 'pw',
      extra: { 'client.id': 'kw', 'fetch.max.bytes': '10' },
    })
  })

  it('renders a server field error next to the truststore field', async () => {
    mockFetch(() =>
      Response.json(
        { code: 'truststore_invalid_password', message: 'Incorrect truststore password', field: 'truststore' },
        { status: 422 },
      ),
    )
    const user = renderForm()
    await fillBasics(user)
    await user.selectOptions(screen.getByLabelText('Security protocol'), 'SSL')
    await user.type(screen.getByLabelText('Truststore (base64)'), 'QUJD')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    const message = await screen.findByText('Incorrect truststore password')
    expect(screen.getByLabelText('Truststore (base64)')).toHaveAccessibleDescription(
      'Incorrect truststore password',
    )
    expect(message).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument() // field error, not a toast
  })

  it('toasts an error that has no field', async () => {
    mockFetch(() =>
      Response.json({ code: 'cluster_exists', message: "Cluster 'dev' already exists" }, { status: 409 }),
    )
    const user = renderForm()
    await fillBasics(user)
    await user.click(screen.getByRole('button', { name: 'Save' }))
    expect(await screen.findByRole('alert')).toHaveTextContent("Cluster 'dev' already exists")
  })
})

describe('ClusterForm edit', () => {
  beforeEach(() => mockFetch(editHandler))

  it('loads the saved values, locks the name and never pre-fills secrets', async () => {
    renderForm('/clusters/stg-eu/edit')
    expect(await screen.findByDisplayValue('b1:9094')).toBeInTheDocument()
    expect(screen.getByLabelText('Name')).toHaveValue('stg-eu')
    expect(screen.getByLabelText('Name')).toBeDisabled()
    expect(screen.getByLabelText('SASL username')).toHaveValue('svc')
    expect(screen.getByLabelText('SASL password')).toHaveValue('')
    expect(screen.getByLabelText('SASL password')).toHaveAttribute(
      'placeholder',
      expect.stringMatching(/leave blank to keep/i),
    )
    expect(screen.getByLabelText('Truststore password')).toHaveValue('')
    expect(screen.getByLabelText('Truststore (base64)')).toHaveValue('')
    expect(screen.getByLabelText('Truststore (base64)')).toHaveAttribute(
      'placeholder',
      expect.stringMatching(/leave blank to keep/i),
    )
  })

  it('lists the saved truststore certificates', async () => {
    renderForm('/clusters/stg-eu/edit')
    expect(await screen.findByText('CN=Corp Root CA — expires 2030-05-17')).toBeInTheDocument()
  })

  it('does not require a new truststore and omits blank secrets on PUT', async () => {
    const user = renderForm('/clusters/stg-eu/edit')
    await screen.findByDisplayValue('b1:9094')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('PUT', '/api/clusters/stg-eu')).toHaveLength(1))
    const body = await bodyOf(callsTo('PUT', '/api/clusters/stg-eu')[0])
    expect(body).toEqual({
      name: 'stg-eu',
      env: 'stg',
      region: 'eu-west-1',
      bootstrap_servers: 'b1:9094',
      security_protocol: 'SASL_SSL',
      sasl_mechanism: 'SCRAM-SHA-512',
      sasl_username: 'svc',
      read_only: false,
      extra: {},
    })
  })

  it('does not auto-check read-only when the env is edited to prd', async () => {
    const user = renderForm('/clusters/stg-eu/edit')
    const env = await screen.findByLabelText('Environment')
    await user.clear(env)
    await user.type(env, 'prd')
    expect(screen.getByLabelText('Read-only')).not.toBeChecked()
  })
})

describe('ClusterForm saved SASL password', () => {
  it('refuses to save a SASL cluster that has no saved password and none typed', async () => {
    mockFetch(() => Response.json({ ...SAVED, has_sasl_password: false }))
    const user = renderForm('/clusters/stg-eu/edit')
    const password = await screen.findByLabelText('SASL password')
    expect(password).not.toHaveAttribute('placeholder', expect.stringMatching(/leave blank/i))
    expect(password).toBeRequired()

    // The browser's own `required` check stops a click on Save; submit directly to reach ours.
    await user.click(screen.getByRole('button', { name: 'Save' }))
    expect(callsTo('PUT', '/api/clusters/stg-eu')).toHaveLength(0)
    fireEvent.submit(password.closest('form')!)

    expect(await screen.findByText('Password is required (none is saved)')).toBeInTheDocument()
    expect(callsTo('PUT', '/api/clusters/stg-eu')).toHaveLength(0)
  })

  it('accepts a typed password when none is saved', async () => {
    mockFetch(() => Response.json({ ...SAVED, has_sasl_password: false }))
    const user = renderForm('/clusters/stg-eu/edit')
    await user.type(await screen.findByLabelText('SASL password'), 'new-secret')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('PUT', '/api/clusters/stg-eu')).toHaveLength(1))
    expect(await bodyOf(callsTo('PUT', '/api/clusters/stg-eu')[0])).toMatchObject({
      sasl_password: 'new-secret',
    })
  })

  it('requires a password when an existing non-SASL cluster is switched to SASL', async () => {
    mockFetch(() =>
      Response.json({
        ...SAVED,
        security_protocol: 'PLAINTEXT',
        sasl_mechanism: null,
        sasl_username: null,
        has_sasl_password: false,
        truststore: null,
      }),
    )
    const user = renderForm('/clusters/stg-eu/edit')
    await user.selectOptions(await screen.findByLabelText('Security protocol'), 'SASL_PLAINTEXT')
    await user.type(screen.getByLabelText('SASL username'), 'svc')
    expect(screen.getByLabelText('SASL password')).toBeRequired()
    fireEvent.submit(screen.getByLabelText('SASL password').closest('form')!)
    expect(await screen.findByText('Password is required (none is saved)')).toBeInTheDocument()
    expect(callsTo('PUT', '/api/clusters/stg-eu')).toHaveLength(0)
  })

  it('keeps the keep-blank hint when a password is saved', async () => {
    mockFetch(editHandler)
    renderForm('/clusters/stg-eu/edit')
    expect(await screen.findByLabelText('SASL password')).toHaveAttribute(
      'placeholder',
      expect.stringMatching(/leave blank to keep/i),
    )
    expect(screen.getByLabelText('SASL password')).not.toBeRequired()
  })
})

describe('ClusterForm edit with a stale cache', () => {
  it('seeds the form from a fresh fetch, not from the cached cluster', async () => {
    // The cache still says read_only: false; the server (e.g. after another tab) says true.
    mockFetch(() => Response.json({ ...SAVED, read_only: true }))
    const user = renderForm('/clusters/stg-eu/edit', (client) =>
      client.setQueryData(['clusters', 'stg-eu'], { ...SAVED, read_only: false }),
    )
    expect(screen.queryByLabelText('Read-only')).not.toBeInTheDocument()
    expect(screen.getByText(/loading/i)).toBeInTheDocument()

    expect(await screen.findByLabelText('Read-only')).toBeChecked()
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(callsTo('PUT', '/api/clusters/stg-eu')).toHaveLength(1))
    expect(await bodyOf(callsTo('PUT', '/api/clusters/stg-eu')[0])).toMatchObject({
      read_only: true,
    })
  })

  it('shows the error when the refetch fails instead of the stale cached values', async () => {
    mockFetch(() => Response.json({ code: 'boom', message: 'Server exploded' }, { status: 500 }))
    renderForm('/clusters/stg-eu/edit', (client) =>
      client.setQueryData(['clusters', 'stg-eu'], { ...SAVED, read_only: false }),
    )
    expect(await screen.findByText(/Server exploded/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Read-only')).not.toBeInTheDocument()
  })

  it('keeps the form and what was typed when a later background refetch changes the data', async () => {
    mockFetch(editHandler)
    const clientRef: { current?: QueryClient } = {}
    const user = renderForm('/clusters/stg-eu/edit', (client) => (clientRef.current = client))
    const env = await screen.findByLabelText('Environment')
    await user.clear(env)
    await user.type(env, 'qa')

    mockFetch(() => Response.json({ ...SAVED, env: 'other', read_only: true }))
    await clientRef.current!.invalidateQueries({ queryKey: ['clusters', 'stg-eu'] })

    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    expect(screen.getByLabelText('Environment')).toHaveValue('qa')
    expect(screen.getByLabelText('Read-only')).not.toBeChecked()
  })
})

describe('ClusterForm test connection', () => {
  it('posts the form values to /api/clusters/test and shows success', async () => {
    mockFetch(() => Response.json({ ok: true }))
    const user = renderForm()
    await fillBasics(user)
    await user.click(screen.getByRole('button', { name: 'Test connection' }))
    expect(await screen.findByText('Connection OK')).toBeInTheDocument()
    const calls = callsTo('POST', '/api/clusters/test')
    expect(calls).toHaveLength(1)
    expect(new URL(calls[0].url).search).toBe('')
    expect(await bodyOf(calls[0])).toMatchObject({ name: 'dev', bootstrap_servers: 'localhost:9092' })
    expect(callsTo('POST', '/api/clusters')).toHaveLength(0)
  })

  it('shows the failure message inline', async () => {
    mockFetch(() =>
      Response.json({ code: 'kafka_timeout', message: 'Timed out connecting to the broker' }, { status: 504 }),
    )
    const user = renderForm()
    await fillBasics(user)
    await user.click(screen.getByRole('button', { name: 'Test connection' }))
    expect(await screen.findByText(/Timed out connecting to the broker/)).toBeInTheDocument()
    expect(screen.queryByText('Connection OK')).not.toBeInTheDocument()
  })

  it('passes ?existing=<name> in edit mode', async () => {
    mockFetch((request) =>
      new URL(request.url).pathname === '/api/clusters/test' ? Response.json({ ok: true }) : Response.json(SAVED),
    )
    const user = renderForm('/clusters/stg-eu/edit')
    await screen.findByDisplayValue('b1:9094')
    await user.click(screen.getByRole('button', { name: 'Test connection' }))
    expect(await screen.findByText('Connection OK')).toBeInTheDocument()
    expect(new URL(callsTo('POST', '/api/clusters/test')[0].url).search).toBe('?existing=stg-eu')
  })
})
