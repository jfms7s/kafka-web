import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '../components/Toasts'
import { ConsumerGroups } from './ConsumerGroups'

const groups = [
  { group_id: 'billing', state: 'stable', type: 'consumer', is_simple: false },
  { group_id: 'Audit-Batch', state: 'empty', type: 'classic', is_simple: false },
  { group_id: 'audit', state: 'empty', type: 'classic', is_simple: false },
]
const topics = [
  { name: '__consumer_offsets', partitions: 50, replication_factor: 1, internal: true },
  { name: 'orders', partitions: 3, replication_factor: 1, internal: false },
  { name: 'payments', partitions: 1, replication_factor: 1, internal: false },
]

type Handler = (request: Request) => Response | Promise<Response>
let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>

function setup({ readOnly = false, create }: { readOnly?: boolean; create?: Handler } = {}) {
  fetchMock = vi.fn(async (request: Request) => {
    const path = new URL(request.url).pathname
    if (path === '/api/clusters/dev') return Response.json({ name: 'dev', read_only: readOnly })
    if (path === '/api/clusters/dev/topics') return Response.json({ topics })
    if (path === '/api/clusters/dev/groups' && request.method === 'GET') {
      return Response.json({ groups })
    }
    if (path === '/api/clusters/dev/groups' && request.method === 'POST') {
      return (await create?.(request)) ?? Response.json({ group_id: 'fresh' }, { status: 201 })
    }
    return new Response('unexpected', { status: 500 })
  })
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/c/dev/groups']}>
        <ToastProvider>
          <Routes>
            <Route path="/c/:cluster/groups" element={<ConsumerGroups />} />
            <Route path="/c/:cluster/groups/:group" element={<p>detail page</p>} />
          </Routes>
        </ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

const ids = () =>
  screen
    .getAllByRole('row')
    .slice(1)
    .map((row) => within(row).getAllByRole('cell')[0].textContent)

const posts = () => fetchMock.mock.calls.map(([r]) => r).filter((r) => r.method === 'POST')

afterEach(() => vi.unstubAllGlobals())

describe('ConsumerGroups list', () => {
  it('lists groups with state and type, linking to the detail', async () => {
    const user = setup()

    const link = await screen.findByRole('link', { name: 'billing' })
    const row = link.closest('tr')!
    expect(within(row).getByText('stable')).toBeInTheDocument()
    expect(within(row).getByText('consumer')).toBeInTheDocument()
    expect(screen.getByText('3 groups')).toBeInTheDocument()
    await user.click(link)
    expect(await screen.findByText('detail page')).toBeInTheDocument()
  })

  it('filters by case-insensitive substring and shows the count', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'billing' })

    await user.type(screen.getByRole('searchbox', { name: 'Filter groups' }), 'AUDIT')

    expect(ids()).toEqual(['Audit-Batch', 'audit'])
    expect(screen.getByText('2 of 3 groups')).toBeInTheDocument()
  })

  it('says when nothing matches', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'billing' })

    await user.type(screen.getByRole('searchbox', { name: 'Filter groups' }), 'zzz')

    expect(screen.getByText('No groups match “zzz”.')).toBeInTheDocument()
  })

  it('shows the API error message', async () => {
    fetchMock = vi.fn(async () =>
      Response.json({ code: 'broker_unreachable', message: 'all brokers down' }, { status: 502 }),
    )
    vi.stubGlobal('fetch', fetchMock)
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter initialEntries={['/c/dev/groups']}>
          <ToastProvider>
            <Routes>
              <Route path="/c/:cluster/groups" element={<ConsumerGroups />} />
            </Routes>
          </ToastProvider>
        </MemoryRouter>
      </QueryClientProvider>,
    )

    expect(await screen.findByText(/all brokers down/)).toBeInTheDocument()
  })
})

describe('ConsumerGroups create', () => {
  it('creates a group from the form and refreshes the list', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'billing' })

    await user.click(screen.getByRole('button', { name: 'Create group' }))
    await user.type(screen.getByRole('textbox', { name: 'Group id' }), 'fresh')
    const topic = screen.getByRole('combobox', { name: 'Topic' })
    expect(within(topic).queryByRole('option', { name: '__consumer_offsets' })).toBeNull()
    await user.selectOptions(topic, 'payments')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Start' }), 'latest')
    await user.click(screen.getByRole('button', { name: 'Create' }))

    await waitFor(() => expect(posts()).toHaveLength(1))
    expect(await posts()[0].json()).toEqual({ group_id: 'fresh', topic: 'payments', start: 'latest' })
    expect(await screen.findByText(/Created group fresh/)).toBeInTheDocument()
    await waitFor(() =>
      expect(fetchMock.mock.calls.filter(([r]) => r.method === 'GET' && r.url.endsWith('/groups'))).toHaveLength(2),
    )
  })

  it('needs a group id and a topic before sending anything', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'billing' })
    await user.click(screen.getByRole('button', { name: 'Create group' }))

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(screen.getByText('Group id is required')).toBeInTheDocument()
    expect(posts()).toHaveLength(0)
  })

  it('toasts an API failure and keeps the form open', async () => {
    const user = setup({
      create: () => Response.json({ code: 'group_exists', message: 'Group already exists' }, { status: 409 }),
    })
    await screen.findByRole('link', { name: 'billing' })
    await user.click(screen.getByRole('button', { name: 'Create group' }))
    await user.type(screen.getByRole('textbox', { name: 'Group id' }), 'billing')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Topic' }), 'orders')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(await screen.findByText('Group already exists')).toBeInTheDocument()
    expect(screen.getByRole('textbox', { name: 'Group id' })).toHaveValue('billing')
  })

  it('is not offered on a read-only cluster', async () => {
    setup({ readOnly: true })
    await screen.findByRole('link', { name: 'billing' })

    expect(screen.queryByRole('button', { name: 'Create group' })).toBeNull()
  })
})
