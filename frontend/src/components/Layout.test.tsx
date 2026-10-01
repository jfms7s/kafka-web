import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ClusterView, ConnectionView } from '../api/hooks/clusters'
import { Layout } from './Layout'

const cluster = (name: string, env: string, readOnly = false): ClusterView => ({
  name,
  env,
  region: null,
  bootstrap_servers: 'b:9092',
  security_protocol: 'PLAINTEXT',
  sasl_mechanism: null,
  sasl_username: null,
  has_sasl_password: false,
  read_only: readOnly,
  extra: {},
  truststore: null,
  usable: true,
  unusable_reason: null,
  connected: true,
})
const connection = (name: string, env: string, readOnly = false): ConnectionView => ({
  name,
  env,
  region: null,
  bootstrap_servers: 'b:9092',
  read_only: readOnly,
  connected_at: '2026-10-01T10:00:00Z',
  active_streams: 0,
})

const CLUSTERS = [cluster('qa-1', 'qa'), cluster('prd-eu', 'prd', true)]
const CONNECTIONS = [connection('qa-1', 'qa'), connection('prd-eu', 'prd', true)]

function setup(path: string) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (request: Request) => {
      const pathname = new URL(request.url).pathname
      if (pathname === '/api/status') return Response.json({ connections: CONNECTIONS })
      const named = CLUSTERS.find((c) => pathname === `/api/clusters/${c.name}`)
      return named ? Response.json(named) : Response.json(CLUSTERS)
    }),
  )
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<p>home page</p>} />
            <Route path="/c/:cluster/topics" element={<p>topics page</p>} />
            <Route path="/c/:cluster/groups" element={<p>groups page</p>} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

afterEach(() => vi.unstubAllGlobals())

describe('Layout', () => {
  it('shows the page without cluster navigation outside a cluster', async () => {
    setup('/')
    expect(screen.getByText('home page')).toBeInTheDocument()
    expect(screen.queryByRole('navigation', { name: 'Cluster' })).not.toBeInTheDocument()
    expect(screen.queryByTestId('prd-strip')).not.toBeInTheDocument()
  })

  it('shows Topics and Consumer Groups navigation inside a cluster', async () => {
    const user = setup('/c/qa-1/topics')
    const nav = screen.getByRole('navigation', { name: 'Cluster' })
    expect(within(nav).getByRole('link', { name: 'Topics' })).toHaveAttribute('href', '/c/qa-1/topics')
    await user.click(within(nav).getByRole('link', { name: 'Consumer Groups' }))
    expect(screen.getByText('groups page')).toBeInTheDocument()
  })

  it('lists connected clusters in the switcher and navigates on change', async () => {
    const user = setup('/c/qa-1/topics')
    const switcher = await screen.findByRole('combobox', { name: 'Cluster' })
    await screen.findByRole('option', { name: 'prd-eu (prd) 🔒' })
    expect(switcher).toHaveValue('qa-1')
    await user.selectOptions(switcher, 'prd-eu')
    expect(await screen.findByText('topics page')).toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: 'Cluster' })).toHaveValue('prd-eu')
  })

  it('shows the red strip only while viewing a prd cluster', async () => {
    setup('/c/prd-eu/topics')
    expect(await screen.findByTestId('prd-strip')).toBeInTheDocument()
  })

  it('shows no red strip for a non-prd cluster', async () => {
    setup('/c/qa-1/topics')
    await screen.findByText('qa')
    expect(screen.queryByTestId('prd-strip')).not.toBeInTheDocument()
  })

  it('shows the current cluster env badge', async () => {
    setup('/c/prd-eu/topics')
    expect(await screen.findByText('prd 🔒')).toBeInTheDocument()
  })
})
