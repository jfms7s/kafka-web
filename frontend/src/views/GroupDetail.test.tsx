import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '../components/Toasts'
import { GroupDetail } from './GroupDetail'

const detail = (state: string) => ({
  group_id: 'billing',
  state,
  type: 'classic',
  members:
    state === 'empty'
      ? []
      : [
          {
            member_id: 'member-1',
            client_id: 'svc-a',
            host: '/10.0.0.7',
            assignments: [
              { topic: 'orders', partition: 0 },
              { topic: 'orders', partition: 1 },
            ],
          },
        ],
  offsets: [
    { topic: 'orders', partition: 0, committed: 7, end: 10, lag: 3 },
    { topic: 'orders', partition: 1, committed: 4, end: 4, lag: 0 },
    { topic: 'payments', partition: 0, committed: 1, end: 9, lag: 8 },
    { topic: 'gone', partition: 0, committed: 5, end: null, lag: null },
  ],
})

type Handler = (request: Request) => Response | Promise<Response>
let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>

interface Options {
  readOnly?: boolean
  state?: string
  reset?: Handler
  preview?: Handler
  remove?: Handler
}

/** A preview that moves every requested partition to offset 42 (or its `offset`). */
async function echoPreview(request: Request): Promise<Response> {
  const body = await request.clone().json() // the test may read the body too
  const parts: { partition: number; offset?: number }[] = body.partitions ?? [{ partition: 0 }]
  return Response.json({
    targets: parts.map((p) => ({
      topic: body.topic,
      partition: p.partition,
      current: 7,
      target: p.offset ?? 42,
      clamped: false,
    })),
  })
}

function setup({ readOnly = false, state = 'empty', reset, preview, remove }: Options = {}) {
  fetchMock = vi.fn(async (request: Request) => {
    const url = new URL(request.url)
    const path = url.pathname
    if (path === '/api/clusters/dev') return Response.json({ name: 'dev', read_only: readOnly })
    if (path === '/api/clusters/dev/groups/billing' && request.method === 'GET') {
      return Response.json(detail(state))
    }
    if (path === '/api/clusters/dev/groups/ghost') {
      return Response.json({ code: 'group_not_found', message: 'No such group' }, { status: 404 })
    }
    if (path.endsWith('/reset/preview')) return (preview ?? echoPreview)(request)
    if (path.endsWith('/reset')) {
      return (await reset?.(request)) ?? Response.json({ offsets: [] })
    }
    if (request.method === 'DELETE') return (await remove?.(request)) ?? new Response(null, { status: 204 })
    return new Response('unexpected', { status: 500 })
  })
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/c/dev/groups/billing']}>
        <ToastProvider>
          <Routes>
            <Route path="/c/:cluster/groups/:group" element={<GroupDetail />} />
            <Route path="/c/:cluster/groups" element={<p>group list page</p>} />
          </Routes>
        </ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

const writes = () =>
  fetchMock.mock.calls
    .map(([r]) => r)
    .filter((r) => r.method !== 'GET' && !r.url.endsWith('/reset/preview'))
const previews = () =>
  fetchMock.mock.calls.map(([r]) => r).filter((r) => r.url.endsWith('/reset/preview'))
const detailGets = () =>
  fetchMock.mock.calls.filter(([r]) => r.method === 'GET' && r.url.endsWith('/groups/billing'))

afterEach(() => vi.unstubAllGlobals())

describe('GroupDetail', () => {
  it('shows state, members with their assignments, offsets and the total lag', async () => {
    setup({ state: 'stable' })

    const members = await screen.findByRole('table', { name: 'Members' })
    expect(screen.getByRole('heading', { name: 'billing' })).toBeInTheDocument()
    expect(screen.getByText('stable')).toBeInTheDocument()
    const memberRow = within(members).getByText('member-1').closest('tr')!
    expect(within(memberRow).getByText('svc-a')).toBeInTheDocument()
    expect(within(memberRow).getByText('/10.0.0.7')).toBeInTheDocument()
    expect(within(memberRow).getByText('orders-0, orders-1')).toBeInTheDocument()

    const offsets = screen.getByRole('table', { name: 'Offsets' })
    const row = within(offsets).getAllByRole('row')[1]
    expect(within(row).getAllByRole('cell').map((c) => c.textContent)).toEqual([
      'orders', '0', '7', '10', '3',
    ])
    expect(screen.getByText('Total lag: 11')).toBeInTheDocument()
    // unknown end offset: lag is shown as unknown, not as 0
    const unknown = within(offsets).getByText('gone').closest('tr')!
    expect(within(unknown).getAllByRole('cell').map((c) => c.textContent)).toEqual([
      'gone', '0', '5', '—', '—',
    ])
  })

  it('says so when the group has no members', async () => {
    setup()

    expect(await screen.findByText('No active members.')).toBeInTheDocument()
  })

  it('shows the API error for an unknown group', async () => {
    fetchMock = vi.fn(async () =>
      Response.json({ code: 'group_not_found', message: 'No such group' }, { status: 404 }),
    )
    vi.stubGlobal('fetch', fetchMock)
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter initialEntries={['/c/dev/groups/ghost']}>
          <ToastProvider>
            <Routes>
              <Route path="/c/:cluster/groups/:group" element={<GroupDetail />} />
            </Routes>
          </ToastProvider>
        </MemoryRouter>
      </QueryClientProvider>,
    )

    expect(await screen.findByText(/No such group/)).toBeInTheDocument()
  })
})

describe('GroupDetail write guard', () => {
  it('enables reset and delete for an empty group on a writable cluster', async () => {
    setup()

    expect(await screen.findByRole('button', { name: 'Reset offsets' })).toBeEnabled()
    expect(screen.getByRole('button', { name: 'Delete group' })).toBeEnabled()
  })

  it('disables both on a read-only cluster and explains why', async () => {
    setup({ readOnly: true })

    const reset = await screen.findByRole('button', { name: 'Reset offsets' })
    expect(reset).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Delete group' })).toBeDisabled()
    expect(screen.getAllByText(/cluster is read-only/i).length).toBeGreaterThan(0)
    expect(reset.closest('span[title]')).toHaveAttribute('title', expect.stringMatching(/read-only/i))
  })

  it.each(['stable', 'preparing_rebalancing'])('disables both for a %s group and explains why', async (state) => {
    setup({ state })

    const reset = await screen.findByRole('button', { name: 'Reset offsets' })
    expect(reset).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Delete group' })).toBeDisabled()
    expect(screen.getAllByText(new RegExp(`must be empty.*${state}`, 'i')).length).toBeGreaterThan(0)
  })
})

describe('GroupDetail reset', () => {
  async function openReset(user: ReturnType<typeof userEvent.setup>) {
    await user.click(await screen.findByRole('button', { name: 'Reset offsets' }))
    return screen.findByRole('dialog')
  }

  it('offers only the committed topics and sends the typed group id as confirm', async () => {
    const user = setup()
    const topic = await screen.findByRole('combobox', { name: 'Topic' })
    expect(within(topic).getAllByRole('option').map((o) => o.textContent)).toEqual([
      'gone', 'orders', 'payments',
    ])
    await user.selectOptions(topic, 'payments')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Strategy' }), 'earliest')

    const dialog = await openReset(user)
    const confirmButton = within(dialog).getByRole('button', { name: 'Reset offsets' })
    expect(confirmButton).toBeDisabled()
    await user.type(within(dialog).getByRole('textbox'), 'billing')
    await user.click(confirmButton)

    await waitFor(() => expect(writes()).toHaveLength(1))
    const request = writes()[0]
    expect(request.method).toBe('POST')
    expect(new URL(request.url).pathname).toBe('/api/clusters/dev/groups/billing/reset')
    expect(await request.json()).toEqual({ topic: 'payments', strategy: 'earliest', confirm: 'billing' })
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    expect(await screen.findByText(/Offsets of billing reset/)).toBeInTheDocument()
    await waitFor(() => expect(detailGets().length).toBeGreaterThan(1))
  })

  it('sends nothing when the confirmation is cancelled', async () => {
    const user = setup()
    const dialog = await openReset(user)

    await user.type(within(dialog).getByRole('textbox'), 'bill')
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }))

    expect(screen.queryByRole('dialog')).toBeNull()
    expect(writes()).toHaveLength(0)
  })

  it('asks for a timestamp only for the timestamp strategy and sends epoch milliseconds', async () => {
    const user = setup()
    await screen.findByRole('combobox', { name: 'Strategy' })
    expect(screen.queryByLabelText('Timestamp')).toBeNull()

    await user.selectOptions(screen.getByRole('combobox', { name: 'Strategy' }), 'timestamp')
    const field = screen.getByLabelText('Timestamp')
    await user.clear(field)
    await user.type(field, '2024-01-02T03:04')
    const dialog = await openReset(user)
    await user.type(within(dialog).getByRole('textbox'), 'billing')
    await user.click(within(dialog).getByRole('button', { name: 'Reset offsets' }))

    await waitFor(() => expect(writes()).toHaveLength(1))
    expect(await writes()[0].json()).toEqual({
      topic: 'gone',
      strategy: 'timestamp',
      timestamp: new Date('2024-01-02T03:04').getTime(),
      confirm: 'billing',
    })
  })

  it('will not open the confirmation for the timestamp strategy without a timestamp', async () => {
    const user = setup()
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Strategy' }), 'timestamp')

    await user.click(screen.getByRole('button', { name: 'Reset offsets' }))

    expect(screen.getByText('Timestamp is required')).toBeInTheDocument()
    expect(screen.queryByRole('dialog')).toBeNull()
  })

  it('toasts a failure (a consumer joined in the meantime) and closes the dialog', async () => {
    const user = setup({
      reset: () => Response.json({ code: 'group_not_empty', message: 'Group is not empty' }, { status: 409 }),
    })
    const dialog = await openReset(user)
    await user.type(within(dialog).getByRole('textbox'), 'billing')

    await user.click(within(dialog).getByRole('button', { name: 'Reset offsets' }))

    expect(await screen.findByText('Group is not empty')).toBeInTheDocument()
    expect(screen.queryByRole('dialog')).toBeNull()
  })
})

describe('GroupDetail precise reset', () => {
  it('pre-fills the partition table from the group offsets and "skip one" sets committed + 1', async () => {
    const user = setup()
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Topic' }), 'orders')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Strategy' }), 'to_offset')

    const table = screen.getByRole('table', { name: 'Partitions to reset' })
    expect(within(table).getAllByRole('row')).toHaveLength(3) // header + partitions 0 and 1
    const target0 = screen.getByLabelText('Target offset of partition 0')
    expect(target0).toHaveValue('7')
    expect(screen.getByLabelText('Target offset of partition 1')).toHaveValue('4')

    await user.click(screen.getByRole('button', { name: 'Skip one on partition 0' }))
    expect(target0).toHaveValue('8')
  })

  it('previews only the chosen partition and commits the same body after confirmation', async () => {
    const user = setup({
      preview: () =>
        Response.json({
          targets: [{ topic: 'orders', partition: 0, current: 7, target: 10, clamped: true }],
        }),
    })
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Topic' }), 'orders')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Strategy' }), 'to_offset')
    await user.click(screen.getByLabelText('Reset partition 1'))
    const target0 = screen.getByLabelText('Target offset of partition 0')
    await user.clear(target0)
    await user.type(target0, '99')

    await user.click(screen.getByRole('button', { name: 'Reset offsets' }))
    const dialog = await screen.findByRole('dialog')
    const expected = { topic: 'orders', strategy: 'to_offset', partitions: [{ partition: 0, offset: 99 }] }
    expect(await previews()[0].json()).toEqual(expected)
    const listed = within(dialog).getByRole('list', { name: 'Reset targets' })
    expect(listed).toHaveTextContent('partition 0: 7 → 10 (clamped to the log)')
    expect(writes()).toHaveLength(0)

    await user.type(within(dialog).getByRole('textbox'), 'billing')
    await user.click(within(dialog).getByRole('button', { name: 'Reset offsets' }))

    await waitFor(() => expect(writes()).toHaveLength(1))
    expect(await writes()[0].json()).toEqual({ ...expected, confirm: 'billing' })
  })

  it('sends a shift and the chosen subset for shift by', async () => {
    const user = setup()
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Topic' }), 'orders')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Strategy' }), 'shift_by')
    await user.type(screen.getByLabelText('Shift by'), '-1000')
    await user.click(screen.getByLabelText('Reset partition 0'))

    await user.click(screen.getByRole('button', { name: 'Reset offsets' }))

    await screen.findByRole('dialog')
    expect(await previews()[0].json()).toEqual({
      topic: 'orders',
      strategy: 'shift_by',
      shift: -1000,
      partitions: [{ partition: 1 }],
    })
  })

  it.each([
    ['shift_by', 'Shift must be a whole number'],
    ['to_offset', 'Partition 0 needs a target offset of 0 or more'],
  ])('refuses to preview %s with a bad number', async (strategy, message) => {
    const user = setup()
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Strategy' }), strategy)
    if (strategy === 'to_offset') await user.clear(screen.getByLabelText('Target offset of partition 0'))

    await user.click(screen.getByRole('button', { name: 'Reset offsets' }))

    expect(screen.getByText(message)).toBeInTheDocument()
    expect(previews()).toHaveLength(0)
  })

  it('refuses to preview with no partition chosen', async () => {
    const user = setup()
    await user.click(await screen.findByLabelText('Reset partition 0'))

    await user.click(screen.getByRole('button', { name: 'Reset offsets' }))

    expect(screen.getByText('Choose at least one partition')).toBeInTheDocument()
    expect(previews()).toHaveLength(0)
  })

  it('toasts a refused preview and opens no confirmation', async () => {
    const user = setup({
      preview: () =>
        Response.json(
          { code: 'validation_failed', message: 'partitions: no committed offset', field: 'partitions' },
          { status: 422 },
        ),
    })

    await user.click(await screen.findByRole('button', { name: 'Reset offsets' }))

    expect(await screen.findByText('partitions: no committed offset')).toBeInTheDocument()
    expect(screen.queryByRole('dialog')).toBeNull()
  })
})

describe('GroupDetail delete', () => {
  it('sends the typed group id as confirm and returns to the list', async () => {
    const user = setup()

    await user.click(await screen.findByRole('button', { name: 'Delete group' }))
    const dialog = await screen.findByRole('dialog')
    await user.type(within(dialog).getByRole('textbox'), 'billing')
    await user.click(within(dialog).getByRole('button', { name: 'Delete group' }))

    expect(await screen.findByText('group list page')).toBeInTheDocument()
    expect(writes()).toHaveLength(1)
    const request = writes()[0]
    expect(request.method).toBe('DELETE')
    const url = new URL(request.url)
    expect(url.pathname).toBe('/api/clusters/dev/groups/billing')
    expect(url.searchParams.get('confirm')).toBe('billing')
  })

  it('toasts a failure and stays on the page', async () => {
    const user = setup({
      remove: () => Response.json({ code: 'group_not_empty', message: 'Group is not empty' }, { status: 409 }),
    })
    await user.click(await screen.findByRole('button', { name: 'Delete group' }))
    const dialog = await screen.findByRole('dialog')
    await user.type(within(dialog).getByRole('textbox'), 'billing')

    await user.click(within(dialog).getByRole('button', { name: 'Delete group' }))

    expect(await screen.findByText('Group is not empty')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'billing' })).toBeInTheDocument()
  })
})
