import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ConfigTab } from './ConfigTab'

const config = {
  name: 'orders.v1_x-y',
  replication_factor: 3,
  partitions: [
    { id: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
    { id: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
  ],
  entries: [
    {
      name: 'retention.ms',
      value: '604800000',
      display_value: '7d',
      is_default: false,
      source: 'DYNAMIC_TOPIC_CONFIG',
      sensitive: false,
    },
    {
      name: 'cleanup.policy',
      value: 'delete',
      display_value: 'delete',
      is_default: true,
      source: 'DEFAULT_CONFIG',
      sensitive: false,
    },
    {
      name: 'secret.thing',
      value: null,
      display_value: null,
      is_default: true,
      source: 'DEFAULT_CONFIG',
      sensitive: true,
    },
  ],
}

let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>
function setup(respond?: () => Response) {
  fetchMock = vi.fn(async () => respond?.() ?? Response.json(config))
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <ConfigTab cluster="dev" topic="orders.v1_x-y" />
    </QueryClientProvider>,
  )
}

afterEach(() => vi.unstubAllGlobals())

describe('ConfigTab', () => {
  it('requests the config of the URL-encoded topic', async () => {
    setup()
    await screen.findByText('retention.ms')

    expect(new URL(fetchMock.mock.calls[0][0].url).pathname).toBe(
      '/api/clusters/dev/topics/orders.v1_x-y/config',
    )
  })

  it('shows humanized values with the raw value in the title', async () => {
    setup()

    const row = (await screen.findByText('retention.ms')).closest('tr')!
    const value = within(row).getByText('7d')
    expect(value).toHaveAttribute('title', '604800000')
  })

  it('highlights non-default rows only', async () => {
    setup()
    await screen.findByText('retention.ms')

    expect(screen.getByText('retention.ms').closest('tr')).toHaveAttribute('data-default', 'false')
    expect(screen.getByText('cleanup.policy').closest('tr')).toHaveAttribute('data-default', 'true')
    expect(screen.getByText('retention.ms').closest('tr')).toHaveClass('bg-amber-50')
    expect(screen.getByText('cleanup.policy').closest('tr')).not.toHaveClass('bg-amber-50')
  })

  it('hides sensitive values', async () => {
    setup()

    const row = (await screen.findByText('secret.thing')).closest('tr')!
    expect(within(row).getByText('hidden')).toBeInTheDocument()
  })

  it('summarises partitions and replication', async () => {
    setup()

    expect(await screen.findByText('2 partitions · replication factor 3')).toBeInTheDocument()
    const row = screen.getByRole('cell', { name: '2, 3' }).closest('tr')!
    expect(within(row).getAllByRole('cell').map((c) => c.textContent)).toEqual(['1', '2', '2, 3, 1', '2, 3'])
  })

  it('shows the API error message', async () => {
    setup(() => Response.json({ code: 'topic_not_found', message: 'gone' }, { status: 404 }))

    expect(await screen.findByText(/gone/)).toBeInTheDocument()
  })
})
