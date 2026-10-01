import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { ReactNode } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  useClusters,
  useConnect,
  useCreateCluster,
  useDeleteCluster,
  useDisconnect,
  useStatus,
  useTestCluster,
  useUpdateCluster,
} from './clusters'

afterEach(() => vi.unstubAllGlobals())

function setup(respond: (request: Request) => Response) {
  const fetchMock = vi.fn(async (request: Request) => respond(request))
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const invalidate = vi.spyOn(client, 'invalidateQueries')
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  )
  const lastRequest = () => fetchMock.mock.calls.at(-1)![0]
  return { wrapper, invalidate, lastRequest }
}

const BODY = { name: 'dev', env: 'dev', bootstrap_servers: 'b:9092' }

const invalidatedKeys = (invalidate: ReturnType<typeof vi.spyOn>) =>
  invalidate.mock.calls.map((call) => (call[0] as { queryKey: unknown[] }).queryKey)

describe('cluster queries', () => {
  it('useClusters fetches GET /api/clusters', async () => {
    const { wrapper, lastRequest } = setup(() => Response.json([]))
    const { result } = renderHook(() => useClusters(), { wrapper })
    await waitFor(() => expect(result.current.data).toEqual([]))
    expect(new URL(lastRequest().url).pathname).toBe('/api/clusters')
  })

  it('useStatus fetches GET /api/status', async () => {
    const { wrapper, lastRequest } = setup(() => Response.json({ connections: [] }))
    const { result } = renderHook(() => useStatus(), { wrapper })
    await waitFor(() => expect(result.current.data).toEqual({ connections: [] }))
    expect(new URL(lastRequest().url).pathname).toBe('/api/status')
  })
})

const NO_CONTENT = () => new Response(null, { status: 204 })
const OK = () => Response.json({})

// Each case wraps a hook into a zero-argument trigger.
const cases = [
  ['create', () => { const m = useCreateCluster(); return () => m.mutateAsync(BODY) }, 'POST', '/api/clusters', () => Response.json({}, { status: 201 })],
  ['update', () => { const m = useUpdateCluster('dev'); return () => m.mutateAsync(BODY) }, 'PUT', '/api/clusters/dev', OK],
  ['delete', () => { const m = useDeleteCluster(); return () => m.mutateAsync('dev') }, 'DELETE', '/api/clusters/dev', NO_CONTENT],
  ['connect', () => { const m = useConnect(); return () => m.mutateAsync('dev') }, 'POST', '/api/clusters/dev/connect', OK],
  ['disconnect', () => { const m = useDisconnect(); return () => m.mutateAsync('dev') }, 'POST', '/api/clusters/dev/disconnect', NO_CONTENT],
  ['test', () => { const m = useTestCluster(); return () => m.mutateAsync({ input: BODY }) }, 'POST', '/api/clusters/test', () => Response.json({ ok: true })],
] as const

describe('cluster mutations', () => {
  it.each(cases)('%s calls its endpoint and invalidates clusters and status', async (_name, useTrigger, method, path, respond) => {
    const { wrapper, invalidate, lastRequest } = setup(respond)
    const { result } = renderHook(() => useTrigger(), { wrapper })
    await act(async () => {
      await result.current()
    })
    expect(lastRequest().method).toBe(method)
    expect(new URL(lastRequest().url).pathname).toBe(path)
    const keys = invalidatedKeys(invalidate)
    expect(keys).toContainEqual(['clusters'])
    expect(keys).toContainEqual(['status'])
  })

  it('test sends ?existing=<name> in edit mode', async () => {
    const { wrapper, lastRequest } = setup(() => Response.json({ ok: true }))
    const { result } = renderHook(() => useTestCluster(), { wrapper })
    await act(async () => {
      await result.current.mutateAsync({ input: BODY, existing: 'dev' })
    })
    expect(new URL(lastRequest().url).search).toBe('?existing=dev')
  })
})
