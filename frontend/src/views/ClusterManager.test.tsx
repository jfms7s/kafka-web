import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ClusterView } from '../api/hooks/clusters'
import { ToastProvider } from '../components/Toasts'
import { ClusterManager } from './ClusterManager'

const base: ClusterView = {
  name: 'dev',
  env: 'dev',
  region: null,
  bootstrap_servers: 'localhost:9092',
  security_protocol: 'PLAINTEXT',
  sasl_mechanism: null,
  sasl_username: null,
  has_sasl_password: false,
  read_only: false,
  extra: {},
  truststore: null,
  usable: true,
  unusable_reason: null,
  connected: false,
}
const idle = base
const connected: ClusterView = { ...base, name: 'stg-eu', env: 'stg', region: 'eu-west-1', connected: true, read_only: true }
const broken: ClusterView = {
  ...base,
  name: 'prd-eu',
  env: 'prd',
  usable: false,
  unusable_reason: 'Truststore file is missing',
}

let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>
function setup(clusters: ClusterView[], extra?: (request: Request) => Response | undefined) {
  fetchMock = vi.fn(async (request: Request) => {
    const custom = extra?.(request)
    if (custom) return custom
    if (request.method === 'GET') return Response.json(clusters)
    return request.method === 'DELETE' ? new Response(null, { status: 204 }) : Response.json({})
  })
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ToastProvider>
          <Routes>
            <Route path="/" element={<ClusterManager />} />
            <Route path="/c/:cluster/topics" element={<p>topics page</p>} />
            <Route path="/clusters/new" element={<p>new cluster page</p>} />
            <Route path="/clusters/:name/edit" element={<p>edit page</p>} />
          </Routes>
        </ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}
const requests = (method: string) =>
  fetchMock.mock.calls.map(([r]) => r).filter((r) => r.method === method && new URL(r.url).pathname !== '/api/clusters')

const card = (name: string) => screen.getByRole('article', { name })

afterEach(() => vi.unstubAllGlobals())

describe('ClusterManager', () => {
  it('shows an empty state with a link to add a cluster', async () => {
    const user = setup([])
    expect(await screen.findByText(/no clusters yet/i)).toBeInTheDocument()
    await user.click(screen.getByRole('link', { name: 'Add cluster' }))
    expect(screen.getByText('new cluster page')).toBeInTheDocument()
  })

  it('renders one card per cluster with env, region, bootstrap servers and state', async () => {
    setup([idle, connected, broken])
    await screen.findByRole('article', { name: 'dev' })
    expect(within(card('dev')).getByText('idle')).toBeInTheDocument()
    expect(within(card('dev')).getByText('localhost:9092')).toBeInTheDocument()
    expect(within(card('stg-eu')).getByText('connected')).toBeInTheDocument()
    expect(within(card('stg-eu')).getByText('stg 🔒')).toBeInTheDocument()
    expect(within(card('stg-eu')).getByText('eu-west-1')).toBeInTheDocument()
    expect(within(card('prd-eu')).getByText('unusable')).toBeInTheDocument()
    expect(within(card('prd-eu')).getByText('Truststore file is missing')).toBeInTheDocument()
  })

  it('offers Connect to idle clusters and Disconnect to connected ones', async () => {
    setup([idle, connected])
    await screen.findByRole('article', { name: 'dev' })
    expect(within(card('dev')).getByRole('button', { name: 'Connect' })).toBeInTheDocument()
    expect(within(card('dev')).queryByRole('button', { name: 'Disconnect' })).not.toBeInTheDocument()
    expect(within(card('stg-eu')).getByRole('button', { name: 'Disconnect' })).toBeInTheDocument()
    expect(within(card('stg-eu')).queryByRole('button', { name: 'Connect' })).not.toBeInTheDocument()
  })

  it('connects and disconnects through the API', async () => {
    const user = setup([idle, connected])
    await screen.findByRole('article', { name: 'dev' })
    await user.click(within(card('dev')).getByRole('button', { name: 'Connect' }))
    await waitFor(() => expect(requests('POST').map((r) => new URL(r.url).pathname)).toContain('/api/clusters/dev/connect'))
    await user.click(within(card('stg-eu')).getByRole('button', { name: 'Disconnect' }))
    await waitFor(() => expect(requests('POST').map((r) => new URL(r.url).pathname)).toContain('/api/clusters/stg-eu/disconnect'))
  })

  it('disables Connect and Open for an unusable cluster', async () => {
    setup([broken])
    await screen.findByRole('article', { name: 'prd-eu' })
    expect(within(card('prd-eu')).getByRole('button', { name: 'Connect' })).toBeDisabled()
    expect(within(card('prd-eu')).queryByRole('link', { name: 'Open' })).not.toBeInTheDocument()
    expect(within(card('prd-eu')).getByRole('link', { name: 'Edit' })).toBeInTheDocument()
  })

  it('opens a cluster at its topics page', async () => {
    const user = setup([idle])
    await screen.findByRole('article', { name: 'dev' })
    await user.click(within(card('dev')).getByRole('link', { name: 'Open' }))
    expect(screen.getByText('topics page')).toBeInTheDocument()
  })

  it('links Edit to the edit form', async () => {
    const user = setup([idle])
    await screen.findByRole('article', { name: 'dev' })
    await user.click(within(card('dev')).getByRole('link', { name: 'Edit' }))
    expect(screen.getByText('edit page')).toBeInTheDocument()
  })

  it('deletes only after the cluster name is typed', async () => {
    const user = setup([idle, connected])
    await screen.findByRole('article', { name: 'dev' })
    await user.click(within(card('dev')).getByRole('button', { name: 'Delete' }))
    const dialog = screen.getByRole('dialog')
    const confirm = within(dialog).getByRole('button', { name: 'Delete cluster' })
    expect(confirm).toBeDisabled()
    await user.type(within(dialog).getByLabelText(/type .* to confirm/i), 'dev')
    await user.click(confirm)
    await waitFor(() => expect(requests('DELETE').map((r) => new URL(r.url).pathname)).toEqual(['/api/clusters/dev']))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('cancelling the delete dialog deletes nothing', async () => {
    const user = setup([idle])
    await screen.findByRole('article', { name: 'dev' })
    await user.click(within(card('dev')).getByRole('button', { name: 'Delete' }))
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(requests('DELETE')).toHaveLength(0)
  })

  it('toasts a failed connect', async () => {
    const user = setup([idle], (request) =>
      request.method === 'POST'
        ? Response.json({ code: 'kafka_timeout', message: 'Timed out' }, { status: 504 })
        : undefined,
    )
    await screen.findByRole('article', { name: 'dev' })
    await user.click(within(card('dev')).getByRole('button', { name: 'Connect' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Timed out')
  })

  it('shows a load error', async () => {
    setup([], () => Response.json({ code: 'config_file_invalid', message: 'clusters.yaml is malformed' }, { status: 500 }))
    expect(await screen.findByText(/clusters\.yaml is malformed/)).toBeInTheDocument()
  })
})
