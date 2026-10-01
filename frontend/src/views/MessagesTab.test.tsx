import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { mockVirtualLayout } from '../test/layout'
import { MessagesTab } from './MessagesTab'

const config = {
  name: 'orders',
  replication_factor: 1,
  partitions: [
    { id: 0, leader: 1, replicas: [1], isr: [1] },
    { id: 1, leader: 1, replicas: [1], isr: [1] },
  ],
  entries: [],
}

const text = (data: string, json?: unknown) => ({
  encoding: 'utf-8',
  data,
  is_json: json !== undefined,
  json_value: json ?? null,
})
const NULL = { encoding: 'null', data: null, is_json: false, json_value: null }

const messages = [
  {
    partition: 0,
    offset: 0,
    timestamp: 1_700_000_000_000,
    timestamp_type: 'create',
    key: text('alpha'),
    value: text('{"a":1}', { a: 1 }),
    headers: [{ key: 'trace', value: text('abc') }],
  },
  {
    partition: 1,
    offset: 4,
    timestamp: 1_700_000_001_000,
    timestamp_type: 'create',
    key: text('beta'),
    value: { encoding: 'base64', data: '//4A', is_json: false, json_value: null },
    headers: [],
  },
  {
    partition: 1,
    offset: 5,
    timestamp: null,
    timestamp_type: 'none',
    key: NULL,
    value: NULL,
    headers: [],
  },
]

let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>

function setup(respond?: (url: URL) => Response) {
  fetchMock = vi.fn(async (request: Request) => {
    const url = new URL(request.url)
    if (url.pathname.endsWith('/config')) return Response.json(config)
    return respond?.(url) ?? Response.json({ messages })
  })
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MessagesTab cluster="dev" topic="orders" />
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

const messageRequests = () =>
  fetchMock.mock.calls.map(([r]) => new URL(r.url)).filter((u) => u.pathname.endsWith('/messages'))

function readBlob(blob: Blob | undefined): Promise<string> {
  return new Promise((resolve, reject) => {
    if (!blob) return reject(new Error('nothing was downloaded'))
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result))
    reader.onerror = () => reject(reader.error)
    reader.readAsText(blob)
  })
}

async function fetchMessages(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Fetch' }))
  await screen.findByText('alpha')
}

mockVirtualLayout()
afterEach(() => vi.unstubAllGlobals())

describe('MessagesTab form', () => {
  it('fetches nothing until Fetch is pressed, then sends the defaults', async () => {
    const user = setup()
    await screen.findByRole('option', { name: '1' }) // partitions loaded
    expect(messageRequests()).toHaveLength(0)

    await fetchMessages(user)

    const [url] = messageRequests()
    expect(url.pathname).toBe('/api/clusters/dev/topics/orders/messages')
    expect(Object.fromEntries(url.searchParams)).toEqual({
      count: '100',
      timeout: '10',
      start: 'latest',
    })
  })

  it('shows the offset input only for start=offset and the timestamp input only for timestamp', async () => {
    const user = setup()
    const start = screen.getByLabelText('Start')
    expect(screen.queryByLabelText('Offset')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Timestamp')).not.toBeInTheDocument()

    await user.selectOptions(start, 'offset')
    expect(screen.getByLabelText('Offset')).toBeInTheDocument()
    expect(screen.queryByLabelText('Timestamp')).not.toBeInTheDocument()

    await user.selectOptions(start, 'timestamp')
    expect(screen.queryByLabelText('Offset')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Timestamp')).toBeInTheDocument()

    await user.selectOptions(start, 'earliest')
    expect(screen.queryByLabelText('Offset')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Timestamp')).not.toBeInTheDocument()
  })

  it('builds the query for start=offset with a partition', async () => {
    const user = setup()
    await screen.findByRole('option', { name: '1' })

    await user.selectOptions(screen.getByLabelText('Start'), 'offset')
    await user.selectOptions(screen.getByLabelText('Partition'), '1')
    await user.type(screen.getByLabelText('Offset'), '42')
    await user.clear(screen.getByLabelText('Count'))
    await user.type(screen.getByLabelText('Count'), '25')
    await user.clear(screen.getByLabelText('Timeout (s)'))
    await user.type(screen.getByLabelText('Timeout (s)'), '5')
    await fetchMessages(user)

    expect(Object.fromEntries(messageRequests()[0].searchParams)).toEqual({
      count: '25',
      timeout: '5',
      start: 'offset',
      offset: '42',
      partition: '1',
    })
  })

  it('converts the datetime-local value to epoch milliseconds', async () => {
    const user = setup()

    await user.selectOptions(screen.getByLabelText('Start'), 'timestamp')
    await user.type(screen.getByLabelText('Timestamp'), '2024-05-01T10:30')
    await fetchMessages(user)

    const { searchParams } = messageRequests()[0]
    expect(searchParams.get('start')).toBe('timestamp')
    // TZ is pinned to Asia/Kolkata (UTC+5:30, no DST) in vite.config.ts: 10:30 local is 05:00Z.
    expect(searchParams.get('timestamp')).toBe('1714539600000')
    expect(searchParams.has('offset')).toBe(false)
  })

  it('does not leak an offset typed earlier into a later start mode', async () => {
    const user = setup()
    await screen.findByRole('option', { name: '1' })
    await user.selectOptions(screen.getByLabelText('Start'), 'offset')
    await user.selectOptions(screen.getByLabelText('Partition'), '0')
    await user.type(screen.getByLabelText('Offset'), '7')

    await user.selectOptions(screen.getByLabelText('Start'), 'earliest')
    await fetchMessages(user)

    const { searchParams } = messageRequests()[0]
    expect(searchParams.get('start')).toBe('earliest')
    expect(searchParams.has('offset')).toBe(false)
  })

  it('lists the topic partitions and sends the chosen one', async () => {
    const user = setup()
    const select = await screen.findByLabelText('Partition')
    await screen.findByRole('option', { name: '1' })

    expect(within(select).getAllByRole('option').map((o) => o.textContent)).toEqual([
      'All partitions',
      '0',
      '1',
    ])
    await user.selectOptions(select, '0')
    await fetchMessages(user)

    expect(messageRequests()[0].searchParams.get('partition')).toBe('0')
  })

  it('needs a partition and an offset before start=offset can be fetched', async () => {
    const user = setup()
    await screen.findByRole('option', { name: '1' })
    await user.selectOptions(screen.getByLabelText('Start'), 'offset')

    expect(screen.getByRole('button', { name: 'Fetch' })).toBeDisabled()
    await user.selectOptions(screen.getByLabelText('Partition'), '0')
    expect(screen.getByRole('button', { name: 'Fetch' })).toBeDisabled()
    await user.type(screen.getByLabelText('Offset'), '3')
    expect(screen.getByRole('button', { name: 'Fetch' })).toBeEnabled()
  })

  it('needs a timestamp before start=timestamp can be fetched', async () => {
    const user = setup()
    await user.selectOptions(screen.getByLabelText('Start'), 'timestamp')

    expect(screen.getByRole('button', { name: 'Fetch' })).toBeDisabled()
  })

  it('shows the API error and keeps the form usable', async () => {
    const user = setup(() =>
      Response.json({ code: 'topic_not_found', message: 'Topic gone' }, { status: 404 }),
    )

    await user.click(screen.getByRole('button', { name: 'Fetch' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Topic gone')
    expect(screen.getByRole('button', { name: 'Fetch' })).toBeEnabled()
  })
})

describe('MessagesTab results', () => {
  it('renders a row per message with partition, offset, ISO timestamp, key and value preview', async () => {
    const user = setup()
    await fetchMessages(user)

    const row = screen.getByText('alpha').closest('button')
    expect(row).not.toBeNull()
    expect(within(row as HTMLElement).getByText('2023-11-14T22:13:20.000Z')).toBeInTheDocument()
    expect(within(row as HTMLElement).getByText('{"a":1}')).toBeInTheDocument()
    expect(screen.getByText('3 messages')).toBeInTheDocument()
  })

  it('truncates long value previews to 120 characters', async () => {
    const long = 'x'.repeat(300)
    const user = setup(() =>
      Response.json({ messages: [{ ...messages[0], value: text(long) }] }),
    )
    await fetchMessages(user)

    expect(screen.getByText('x'.repeat(120) + '…')).toBeInTheDocument()
    expect(screen.queryByText(long)).not.toBeInTheDocument()
  })

  it('marks base64 values with a badge and null values as null', async () => {
    const user = setup()
    await fetchMessages(user)

    expect(screen.getAllByText('base64')).toHaveLength(1)
    expect(screen.getAllByText('null').length).toBeGreaterThanOrEqual(2) // null key and value
  })

  it('narrows the loaded rows with the filter box (key or value, case-insensitive)', async () => {
    const user = setup()
    await fetchMessages(user)

    await user.type(screen.getByLabelText('Filter messages'), 'ALPH')
    expect(screen.getByText('alpha')).toBeInTheDocument()
    expect(screen.queryByText('beta')).not.toBeInTheDocument()
    expect(screen.getByText('1 of 3 messages')).toBeInTheDocument()

    await user.clear(screen.getByLabelText('Filter messages'))
    await user.type(screen.getByLabelText('Filter messages'), '"a":1')
    expect(screen.getByText('alpha')).toBeInTheDocument()
    expect(screen.queryByText('beta')).not.toBeInTheDocument()
  })

  it('says so when the filter matches nothing and when nothing was returned', async () => {
    const user = setup()
    await fetchMessages(user)
    await user.type(screen.getByLabelText('Filter messages'), 'zzz')
    expect(screen.getByText('No loaded message matches “zzz”.')).toBeInTheDocument()
  })

  it('shows an empty-result message', async () => {
    const user = setup(() => Response.json({ messages: [] }))

    await user.click(screen.getByRole('button', { name: 'Fetch' }))

    expect(await screen.findByText('No messages found.')).toBeInTheDocument()
  })

  it('expands a row to pretty JSON and headers, and collapses it again', async () => {
    const user = setup()
    await fetchMessages(user)

    await user.click(screen.getByText('alpha'))

    const detail = screen.getByRole('region', { name: 'Message 0:0' })
    const blocks = Array.from(detail.querySelectorAll('pre')).map((pre) => pre.textContent)
    expect(blocks).toContain(JSON.stringify({ a: 1 }, null, 2))
    expect(within(detail).getByText('trace')).toBeInTheDocument()
    expect(within(detail).getByText('abc')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /alpha/ }))
    expect(screen.queryByRole('region', { name: 'Message 0:0' })).not.toBeInTheDocument()
  })

  it('shows raw text for non-JSON values and the base64 payload for binary ones', async () => {
    const user = setup()
    await fetchMessages(user)

    await user.click(screen.getByText('beta'))

    const detail = screen.getByRole('region', { name: 'Message 1:4' })
    expect(within(detail).getByText('base64')).toBeInTheDocument()
    expect(within(detail).getByText('//4A')).toBeInTheDocument()
  })

  it('copies the displayed messages as JSON', async () => {
    const user = setup()
    const writeText = vi.spyOn(navigator.clipboard, 'writeText')
    await fetchMessages(user)
    await user.type(screen.getByLabelText('Filter messages'), 'alpha')

    await user.click(screen.getByRole('button', { name: 'Copy JSON' }))

    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1))
    expect(JSON.parse(writeText.mock.calls[0][0])).toEqual([messages[0]])
  })

  it('downloads the displayed messages as <topic>-messages.json', async () => {
    const user = setup()
    let blob: Blob | undefined
    const urls = URL as unknown as Record<string, unknown>
    urls.createObjectURL = (b: Blob) => {
      blob = b
      return 'blob:fake'
    }
    urls.revokeObjectURL = () => {}
    const names: string[] = []
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
      this: HTMLAnchorElement,
    ) {
      names.push(this.download)
    })
    await fetchMessages(user)

    try {
      await user.click(screen.getByRole('button', { name: 'Download JSON' }))
    } finally {
      delete urls.createObjectURL
      delete urls.revokeObjectURL
    }

    expect(names).toEqual(['orders-messages.json'])
    expect(JSON.parse(await readBlob(blob))).toEqual(messages)
  })

  it('disables copy and download while there is nothing to export', () => {
    setup()

    expect(screen.getByRole('button', { name: 'Copy JSON' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Download JSON' })).toBeDisabled()
  })
})
