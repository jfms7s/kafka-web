import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { ToastProvider } from './components/Toasts'

function renderAt(path: string) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (request: Request) => {
      const path = new URL(request.url).pathname
      if (path === '/api/status') return Response.json({ connections: [] })
      if (path.endsWith('/topics')) return Response.json({ topics: [] })
      if (path.endsWith('/groups')) return Response.json({ groups: [] })
      return Response.json([])
    }),
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

  it('/c/:cluster/topics shows the topic browser', async () => {
    renderAt('/c/dev/topics')
    expect(await screen.findByRole('heading', { name: 'Topics' })).toBeInTheDocument()
    expect(await screen.findByText('0 topics')).toBeInTheDocument()
  })

  it('/c/:cluster/topics/:topic shows the topic detail', async () => {
    renderAt('/c/dev/topics/orders')
    expect(await screen.findByRole('heading', { name: 'orders' })).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: 'Config' })).toBeInTheDocument()
  })

  it('/c/:cluster/groups shows the consumer groups', async () => {
    renderAt('/c/dev/groups')
    expect(await screen.findByRole('heading', { name: 'Consumer Groups' })).toBeInTheDocument()
    expect(await screen.findByText('0 groups')).toBeInTheDocument()
  })

  it('/c/:cluster/groups/:group shows the group detail', async () => {
    renderAt('/c/dev/groups/billing')
    expect(await screen.findByRole('heading', { name: 'billing' })).toBeInTheDocument()
  })

  it('an unknown route says not found', async () => {
    renderAt('/nope')
    expect(await screen.findByText(/Page not found/)).toBeInTheDocument()
  })
})
