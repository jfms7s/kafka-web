import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '../components/Toasts'
import { TopicDetail } from './TopicDetail'

function Search() {
  return <output data-testid="search">{useLocation().search}</output>
}

function setup(entry: string) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (request: Request) =>
      new URL(request.url).pathname === '/api/clusters/dev'
        ? Response.json({ name: 'dev', read_only: false })
        : Response.json({ name: 't', replication_factor: 1, partitions: [], entries: [] }),
    ),
  )
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <ToastProvider>
        <MemoryRouter initialEntries={[entry]}>
          <Routes>
            <Route
              path="/c/:cluster/topics/:topic"
              element={
                <>
                  <TopicDetail />
                  <Search />
                </>
              }
            />
          </Routes>
        </MemoryRouter>
      </ToastProvider>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

afterEach(() => vi.unstubAllGlobals())

describe('TopicDetail', () => {
  it('shows the decoded topic name and the four tabs', () => {
    setup('/c/dev/topics/orders.v1_x-y')

    expect(screen.getByRole('heading', { name: 'orders.v1_x-y' })).toBeInTheDocument()
    expect(screen.getAllByRole('tab').map((t) => t.textContent)).toEqual([
      'Messages',
      'Live',
      'Config',
      'Publish',
    ])
  })

  it('renders the tab named in ?tab= and falls back for unknown values', async () => {
    setup('/c/dev/topics/t?tab=config')
    expect(screen.getByRole('tab', { name: 'Config' })).toHaveAttribute('aria-selected', 'true')
    expect(await screen.findByText(/0 partitions/)).toBeInTheDocument()
  })

  it('shows the snapshot form on the Messages tab', () => {
    setup('/c/dev/topics/t')

    expect(screen.getByRole('button', { name: 'Fetch' })).toBeInTheDocument()
  })

  it('falls back to the first tab for an unknown ?tab=', () => {
    setup('/c/dev/topics/t?tab=bogus')

    expect(screen.getByRole('tab', { name: 'Messages' })).toHaveAttribute('aria-selected', 'true')
  })

  it('switches tabs through the URL', async () => {
    const user = setup('/c/dev/topics/t?tab=config')

    await user.click(screen.getByRole('tab', { name: 'Publish' }))

    expect(screen.getByTestId('search')).toHaveTextContent('?tab=publish')
    expect(screen.getByRole('tab', { name: 'Publish' })).toHaveAttribute('aria-selected', 'true')
    expect(await screen.findByRole('heading', { name: 'Publish a message' })).toBeInTheDocument()
  })

  it('shows the live stream controls on the Live tab', () => {
    setup('/c/dev/topics/t?tab=live')

    expect(screen.getByRole('tab', { name: 'Live' })).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByRole('button', { name: 'Start' })).toBeInTheDocument()
  })
})
