import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { ToastProvider } from './components/Toasts'

function renderAt(path: string) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (request: Request) =>
      new URL(request.url).pathname === '/api/status'
        ? Response.json({ connections: [] })
        : Response.json([]),
    ),
  )
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <MemoryRouter initialEntries={[path]}>
        <ToastProvider>
          <App />
        </ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

afterEach(() => vi.unstubAllGlobals())

describe('App routes', () => {
  it('/ shows the cluster manager', async () => {
    renderAt('/')
    expect(await screen.findByRole('heading', { name: 'Clusters' })).toBeInTheDocument()
  })

  it('/clusters/new shows the cluster form', async () => {
    renderAt('/clusters/new')
    expect(await screen.findByRole('heading', { name: 'Add cluster' })).toBeInTheDocument()
  })

  it('/c/:cluster/topics shows the placeholder', async () => {
    renderAt('/c/dev/topics')
    expect(await screen.findByText('Topics — coming in Task 5')).toBeInTheDocument()
  })

  it('/c/:cluster/groups shows the placeholder', async () => {
    renderAt('/c/dev/groups')
    expect(await screen.findByText('Consumer Groups — coming in a later task')).toBeInTheDocument()
  })

  it('an unknown route says not found', async () => {
    renderAt('/nope')
    expect(await screen.findByText(/Page not found/)).toBeInTheDocument()
  })
})
