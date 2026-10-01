import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, api, unwrap } from './client'

const resolved = <T>(result: { data?: T; error?: unknown; response: Response }) =>
  Promise.resolve(result)

afterEach(() => vi.unstubAllGlobals())

describe('unwrap', () => {
  it('resolves with data', async () => {
    const data = await unwrap(resolved({ data: { ok: true }, response: new Response('{}') }))
    expect(data).toEqual({ ok: true })
  })

  it('resolves undefined for an empty success (204)', async () => {
    const data = await unwrap(resolved({ response: new Response(null, { status: 204 }) }))
    expect(data).toBeUndefined()
  })

  it('throws ApiError built from a {code,message,field} body', async () => {
    const response = new Response('{}', { status: 422 })
    const error = { code: 'truststore_required', message: 'truststore: required', field: 'truststore' }
    const failure = await unwrap(resolved({ error, response })).catch((e: unknown) => e)
    expect(failure).toBeInstanceOf(ApiError)
    expect(failure).toMatchObject({
      status: 422,
      code: 'truststore_required',
      message: 'truststore: required',
      field: 'truststore',
    })
  })

  it('leaves field undefined when the body has none', async () => {
    const error = { code: 'not_found', message: 'Cluster not found' }
    const failure = await unwrap(resolved({ error, response: new Response('', { status: 404 }) })).catch(
      (e: unknown) => e,
    )
    expect(failure).toMatchObject({ status: 404, code: 'not_found' })
    expect((failure as ApiError).field).toBeUndefined()
  })

  it('uses code http_<status> for a non-JSON body', async () => {
    const response = new Response('<html>Bad gateway</html>', { status: 500 })
    const failure = await unwrap(resolved({ error: '<html>Bad gateway</html>', response })).catch(
      (e: unknown) => e,
    )
    expect(failure).toBeInstanceOf(ApiError)
    expect(failure).toMatchObject({ status: 500, code: 'http_500' })
  })
})

describe('api', () => {
  it('calls same-origin /api paths through the current global fetch', async () => {
    const fetchMock = vi.fn(async () => Response.json([]))
    vi.stubGlobal('fetch', fetchMock)
    const data = await unwrap(api.GET('/api/clusters'))
    expect(data).toEqual([])
    const request = (fetchMock.mock.calls[0] as unknown as [Request])[0]
    expect(new URL(request.url).pathname).toBe('/api/clusters')
  })
})
