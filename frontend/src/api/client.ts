import createClient from 'openapi-fetch'
import type { paths } from './schema'

// Browsers resolve a relative Request URL against the page; Node-based runtimes (the test
// environment) throw on it, so resolve explicitly. A no-op in the browser.
class SameOriginRequest extends Request {
  constructor(input: RequestInfo | URL, init?: RequestInit) {
    super(typeof input === 'string' ? new URL(input, globalThis.location.href) : input, init)
  }
}

// `fetch` is looked up per call (not captured at creation) so tests can stub the global.
export const api = createClient<paths>({
  baseUrl: '',
  Request: SameOriginRequest,
  fetch: (request) => globalThis.fetch(request),
})

export class ApiError extends Error {
  readonly status: number
  readonly code: string
  readonly field?: string

  constructor(status: number, code: string, message: string, field?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.field = field
  }
}

interface ErrorBody {
  code: string
  message: string
  field?: string | null
}

function isErrorBody(value: unknown): value is ErrorBody {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as ErrorBody).code === 'string' &&
    typeof (value as ErrorBody).message === 'string'
  )
}

export async function unwrap<T>(
  result: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  const { data, error, response } = await result
  if (error === undefined && response.ok) return data as T
  if (isErrorBody(error)) {
    throw new ApiError(response.status, error.code, error.message, error.field ?? undefined)
  }
  const message = typeof error === 'string' && error ? error : response.statusText
  throw new ApiError(response.status, `http_${response.status}`, message || `HTTP ${response.status}`)
}
