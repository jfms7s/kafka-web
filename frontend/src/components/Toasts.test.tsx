import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '../api/client'
import { ToastProvider, useToast } from './Toasts'

function Trigger({ error }: { error: unknown }) {
  const toast = useToast()
  return <button onClick={() => toast.error(error)}>fire</button>
}

function Celebrate({ message }: { message: string }) {
  const toast = useToast()
  return <button onClick={() => toast.success(message)}>celebrate</button>
}

afterEach(() => vi.useRealTimers())

describe('Toasts', () => {
  it('shows the message and code of an ApiError', async () => {
    render(
      <ToastProvider>
        <Trigger error={new ApiError(409, 'cluster_unusable', 'Truststore file is missing')} />
      </ToastProvider>,
    )
    await userEvent.click(screen.getByRole('button', { name: 'fire' }))
    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('Truststore file is missing')
    expect(alert).toHaveTextContent('cluster_unusable')
  })

  it('shows a generic message for a non-API error', async () => {
    render(
      <ToastProvider>
        <Trigger error={new TypeError('Failed to fetch')} />
      </ToastProvider>,
    )
    await userEvent.click(screen.getByRole('button', { name: 'fire' }))
    expect(screen.getByRole('alert')).toHaveTextContent('Failed to fetch')
  })

  it('can be dismissed', async () => {
    render(
      <ToastProvider>
        <Trigger error={new ApiError(500, 'internal_error', 'boom')} />
      </ToastProvider>,
    )
    await userEvent.click(screen.getByRole('button', { name: 'fire' }))
    await userEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('disappears on its own', () => {
    vi.useFakeTimers()
    render(
      <ToastProvider>
        <Trigger error={new ApiError(500, 'internal_error', 'boom')} />
      </ToastProvider>,
    )
    act(() => screen.getByRole('button', { name: 'fire' }).click())
    expect(screen.getByRole('alert')).toBeInTheDocument()
    act(() => vi.advanceTimersByTime(10_000))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('shows a success message as a status, not an alert', async () => {
    render(
      <ToastProvider>
        <Celebrate message="Published to partition 1" />
      </ToastProvider>,
    )
    await userEvent.click(screen.getByRole('button', { name: 'celebrate' }))
    expect(screen.getByRole('status')).toHaveTextContent('Published to partition 1')
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })
})
