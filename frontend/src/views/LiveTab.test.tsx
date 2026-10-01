import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mockVirtualLayout } from '../test/layout'
import { LiveTab } from './LiveTab'

/** A WebSocket stand-in the test drives by hand: `open()`, `frame(...)`, `serverClose(...)`. */
class MockWebSocket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static instances: MockWebSocket[] = []

  readyState = MockWebSocket.CONNECTING
  sent: string[] = []
  closedWith: (number | undefined)[] = []
  onopen: ((event: Event) => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null
  onerror: ((event: Event) => void) | null = null

  readonly url: string

  constructor(url: string) {
    this.url = url
    MockWebSocket.instances.push(this)
  }

  send(data: string) {
    if (this.readyState !== MockWebSocket.OPEN) throw new Error('send on a socket that is not open')
    this.sent.push(data)
  }

  close(code?: number) {
    this.closedWith.push(code)
    this.readyState = MockWebSocket.CLOSED
  }

  open() {
    act(() => {
      this.readyState = MockWebSocket.OPEN
      this.onopen?.(new Event('open'))
    })
  }

  frame(data: unknown) {
    act(() => this.onmessage?.(new MessageEvent('message', { data: JSON.stringify(data) })))
  }

  serverClose(code: number) {
    act(() => {
      this.readyState = MockWebSocket.CLOSED
      this.onclose?.(new CloseEvent('close', { code }))
    })
  }
}

const config = {
  name: 'orders',
  replication_factor: 1,
  partitions: [
    { id: 0, leader: 1, replicas: [1], isr: [1] },
    { id: 1, leader: 1, replicas: [1], isr: [1] },
  ],
  entries: [],
}

const text = (data: string) => ({ encoding: 'utf-8', data, is_json: false, json_value: null })
const message = (offset: number, key = `key-${offset}`) => ({
  partition: 0,
  offset,
  timestamp: 1_700_000_000_000 + offset,
  timestamp_type: 'create',
  key: text(key),
  value: text(`value-${offset}`),
  headers: [],
})

const socket = () => {
  const last = MockWebSocket.instances.at(-1)
  if (!last) throw new Error('no WebSocket was opened')
  return last
}

function setup(props = { cluster: 'dev', topic: 'orders' }) {
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(config)))
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const view = render(
    <QueryClientProvider client={client}>
      <LiveTab {...props} />
    </QueryClientProvider>,
  )
  const rerender = (next: typeof props) =>
    view.rerender(
      <QueryClientProvider client={client}>
        <LiveTab {...next} />
      </QueryClientProvider>,
    )
  return { user: userEvent.setup(), unmount: view.unmount, rerender }
}

/**
 * jsdom reports 0 for scrollHeight/clientHeight, so the virtualizer would clamp every scroll
 * target to 0. Here the scroll container is 600px tall and as high as its content wrapper.
 */
function mockScrollExtent() {
  const scroller = (el: Element): HTMLElement | null =>
    el instanceof HTMLElement && el.dataset.testid === 'message-scroll' ? el : null
  vi.spyOn(Element.prototype, 'clientHeight', 'get').mockImplementation(function (this: Element) {
    return scroller(this) ? 600 : 0
  })
  vi.spyOn(Element.prototype, 'scrollHeight', 'get').mockImplementation(function (this: Element) {
    const content = scroller(this)?.firstElementChild
    return content instanceof HTMLElement ? parseFloat(content.style.height) : 0
  })
}

const status = () => screen.getByRole('status')
const startButton = () => screen.getByRole('button', { name: 'Start' })
const stopButton = () => screen.getByRole('button', { name: 'Stop' })

// jsdom implements no Element.scrollTo; the virtualizer scrolls through it when it exists.
let scrollTo: ReturnType<typeof vi.fn>

beforeEach(() => {
  MockWebSocket.instances = []
  vi.stubGlobal('WebSocket', MockWebSocket)
  scrollTo = vi.fn()
  HTMLElement.prototype.scrollTo = scrollTo
})
afterEach(() => {
  vi.unstubAllGlobals()
  Reflect.deleteProperty(HTMLElement.prototype, 'scrollTo')
})

describe('LiveTab', () => {
  mockVirtualLayout()

  it('is idle until started', () => {
    setup()

    expect(status()).toHaveTextContent('idle')
    expect(MockWebSocket.instances).toHaveLength(0)
    expect(stopButton()).toBeDisabled()
  })

  it('Start opens a socket for the topic on the page origin, latest by default', async () => {
    const { user } = setup({ cluster: 'my cluster', topic: 'orders.v1' })

    await user.click(startButton())

    expect(socket().url).toBe(
      `ws://${location.host}/api/clusters/my%20cluster/topics/orders.v1/stream?start=latest`,
    )
    expect(status()).toHaveTextContent('connecting')
    socket().open()
    expect(status()).toHaveTextContent('live')
    expect(startButton()).toBeDisabled()
    expect(stopButton()).toBeEnabled()
  })

  it('sends the chosen start mode and partition', async () => {
    const { user } = setup()
    await screen.findByRole('option', { name: '1' })

    await user.selectOptions(screen.getByLabelText('Start'), 'offset')
    expect(startButton()).toBeDisabled() // offset needs a partition and an offset
    await user.selectOptions(screen.getByLabelText('Partition'), '1')
    await user.type(screen.getByLabelText('Offset'), '12')
    await user.click(startButton())

    const query = new URL(socket().url).searchParams
    expect(Object.fromEntries(query)).toEqual({ start: 'offset', offset: '12', partition: '1' })
  })

  it('sends a timestamp start as epoch milliseconds', async () => {
    const { user } = setup()

    await user.selectOptions(screen.getByLabelText('Start'), 'timestamp')
    expect(startButton()).toBeDisabled()
    await user.type(screen.getByLabelText('Timestamp'), '2023-11-15T03:43')
    await user.click(startButton())

    // Asia/Kolkata (vite.config.ts): UTC+5:30, so 03:43 local is 22:13 UTC the day before.
    const query = new URL(socket().url).searchParams
    expect(query.get('start')).toBe('timestamp')
    expect(query.get('timestamp')).toBe(String(Date.UTC(2023, 10, 14, 22, 13)))
  })

  it('renders streamed messages in arrival order', async () => {
    const { user } = setup()
    await user.click(startButton())
    socket().open()

    socket().frame({ type: 'messages', items: [message(1, 'alpha'), message(2, 'beta')] })
    socket().frame({ type: 'messages', items: [message(3, 'gamma')] })

    const keys = screen.getAllByText(/^(alpha|beta|gamma)$/).map((el) => el.textContent)
    expect(keys).toEqual(['alpha', 'beta', 'gamma'])
    expect(screen.getByText('3 messages')).toBeInTheDocument()
  })

  it('adds up dropped frames into the server-dropped counter', async () => {
    const { user } = setup()
    await user.click(startButton())
    socket().open()
    expect(screen.getByText('Dropped by server: 0')).toBeInTheDocument()

    socket().frame({ type: 'dropped', count: 3 })
    socket().frame({ type: 'dropped', count: 4 })

    expect(screen.getByText('Dropped by server: 7')).toBeInTheDocument()
  })

  it('Stop sends "stop" and closes the socket', async () => {
    const { user } = setup()
    await user.click(startButton())
    socket().open()

    await user.click(stopButton())

    expect(socket().sent).toEqual(['stop'])
    expect(socket().closedWith).toEqual([1000])
    expect(status()).toHaveTextContent('closed')
    expect(startButton()).toBeEnabled()
  })

  it('unmounting sends "stop" and closes the socket', async () => {
    const { user, unmount } = setup()
    await user.click(startButton())
    socket().open()

    unmount()

    expect(socket().sent).toEqual(['stop'])
    expect(socket().closedWith).toEqual([1000])
  })

  it('unmounting while still connecting closes the socket without sending', async () => {
    const { user, unmount } = setup()
    await user.click(startButton())

    unmount()

    expect(socket().sent).toEqual([])
    expect(socket().closedWith).toEqual([1000])
  })

  it('switching topic stops the stream and clears what it showed', async () => {
    const { user, rerender } = setup()
    await user.click(startButton())
    socket().open()
    socket().frame({ type: 'messages', items: [message(1, 'alpha')] })

    rerender({ cluster: 'dev', topic: 'payments' })

    expect(socket().sent).toEqual(['stop'])
    expect(socket().closedWith).toEqual([1000])
    expect(screen.queryByText('alpha')).not.toBeInTheDocument()
    expect(status()).toHaveTextContent('idle')
  })

  it('a new Start replaces the buffer and the dropped counter', async () => {
    const { user } = setup()
    await user.click(startButton())
    socket().open()
    socket().frame({ type: 'messages', items: [message(1, 'alpha')] })
    socket().frame({ type: 'dropped', count: 2 })
    await user.click(stopButton())

    await user.click(startButton())

    expect(MockWebSocket.instances).toHaveLength(2)
    expect(screen.queryByText('alpha')).not.toBeInTheDocument()
    expect(screen.getByText('Dropped by server: 0')).toBeInTheDocument()
  })

  it('shows an error frame and ends the stream', async () => {
    const { user } = setup()
    await user.click(startButton())
    socket().open()

    socket().frame({ type: 'error', code: 'topic_not_found', message: 'Topic gone' })
    socket().serverClose(1011)

    expect(screen.getByRole('alert')).toHaveTextContent('Topic gone')
    expect(status()).toHaveTextContent('error')
    expect(startButton()).toBeEnabled()
  })

  it('explains a stream closed because the cluster changed', async () => {
    const { user } = setup()
    await user.click(startButton())
    socket().open()

    socket().frame({ type: 'closed', reason: 'cluster_changed' })
    socket().serverClose(1001)

    expect(status()).toHaveTextContent('closed')
    expect(screen.getByRole('alert')).toHaveTextContent(/cluster was edited, deleted or disconnected/)
  })

  it('reports a connection that drops without a frame', async () => {
    const { user } = setup()
    await user.click(startButton())

    socket().serverClose(1006)

    expect(status()).toHaveTextContent('error')
    expect(screen.getByRole('alert')).toHaveTextContent(/connection/i)
  })

  it('follows new messages while auto-scroll is on (the default)', async () => {
    mockScrollExtent()
    const { user } = setup()
    expect(screen.getByLabelText('Auto-scroll')).toBeChecked()
    await user.click(startButton())
    socket().open()

    socket().frame({ type: 'messages', items: Array.from({ length: 40 }, (_, i) => message(i)) })

    await waitFor(() =>
      expect(scrollTo).toHaveBeenCalledWith(expect.objectContaining({ top: 40 * 36 - 600 })),
    )
  })

  it('stays put when auto-scroll is off', async () => {
    mockScrollExtent()
    const { user } = setup()
    await user.click(screen.getByLabelText('Auto-scroll'))
    await user.click(startButton())
    socket().open()

    socket().frame({ type: 'messages', items: Array.from({ length: 40 }, (_, i) => message(i)) })
    await new Promise((resolve) => setTimeout(resolve, 50))

    expect(scrollTo).not.toHaveBeenCalledWith(expect.objectContaining({ top: 40 * 36 - 600 }))
  })
})
