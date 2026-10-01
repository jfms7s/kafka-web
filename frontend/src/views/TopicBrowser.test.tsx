import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '../components/Toasts'
import { TopicBrowser } from './TopicBrowser'

const topics = [
  { name: '__consumer_offsets', partitions: 50, replication_factor: 3, internal: true },
  { name: 'orders', partitions: 3, replication_factor: 2, internal: false },
  { name: 'orders.v1_x-y', partitions: 6, replication_factor: 2, internal: false },
  { name: 'payments', partitions: 1, replication_factor: 1, internal: false },
]

let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>
function setup(respond?: () => Response) {
  fetchMock = vi.fn(async () => respond?.() ?? Response.json({ topics }))
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/c/dev/topics']}>
        <ToastProvider>
          <Routes>
            <Route path="/c/:cluster/topics" element={<TopicBrowser />} />
            <Route path="/c/:cluster/topics/:topic" element={<p>detail page</p>} />
          </Routes>
        </ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

const names = () =>
  screen
    .getAllByRole('row')
    .slice(1)
    .map((row) => within(row).getAllByRole('cell')[0].textContent)

afterEach(() => vi.unstubAllGlobals())

describe('TopicBrowser', () => {
  it('lists topics with partitions, replication factor and an internal badge', async () => {
    setup()

    expect(await screen.findByRole('link', { name: 'orders' })).toBeInTheDocument()
    const row = screen.getByRole('link', { name: '__consumer_offsets' }).closest('tr')!
    expect(within(row).getByText('50')).toBeInTheDocument()
    expect(within(row).getByText('3')).toBeInTheDocument()
    expect(within(row).getByText('internal')).toBeInTheDocument()
    expect(screen.getAllByText('internal')).toHaveLength(1)
    expect(screen.getByText('4 topics')).toBeInTheDocument()
    expect(new URL(fetchMock.mock.calls[0][0].url).pathname).toBe('/api/clusters/dev/topics')
  })

  it('filters by case-insensitive substring and shows the count', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'payments' })

    await user.type(screen.getByRole('searchbox', { name: 'Filter topics' }), 'ORD')

    expect(names()).toEqual(['orders', 'orders.v1_x-y'])
    expect(screen.getByText('2 of 4 topics')).toBeInTheDocument()
  })

  it('says when nothing matches', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'payments' })

    await user.type(screen.getByRole('searchbox', { name: 'Filter topics' }), 'zzz')

    expect(screen.getByText('No topics match “zzz”.')).toBeInTheDocument()
    expect(screen.getByText('0 of 4 topics')).toBeInTheDocument()
  })

  it('refetches on refresh', async () => {
    const user = setup()
    await screen.findByRole('link', { name: 'payments' })
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await user.click(screen.getByRole('button', { name: 'Refresh' }))

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
  })

  it('navigates to the topic detail with an encoded name', async () => {
    const user = setup()

    const link = await screen.findByRole('link', { name: 'orders.v1_x-y' })
    expect(link).toHaveAttribute('href', '/c/dev/topics/orders.v1_x-y')
    await user.click(link)

    expect(await screen.findByText('detail page')).toBeInTheDocument()
  })

  it('shows the API error message', async () => {
    setup(() =>
      Response.json({ code: 'broker_unreachable', message: 'all brokers down' }, { status: 502 }),
    )

    expect(await screen.findByText(/all brokers down/)).toBeInTheDocument()
  })
})
