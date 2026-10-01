import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ConfirmDialog } from './ConfirmDialog'

function setup(open = true) {
  const onConfirm = vi.fn()
  const onCancel = vi.fn()
  render(
    <ConfirmDialog
      open={open}
      title="Delete cluster"
      description="This removes the cluster."
      expected="stg-eu"
      confirmLabel="Delete"
      onConfirm={onConfirm}
      onCancel={onCancel}
    />,
  )
  return { onConfirm, onCancel, user: userEvent.setup() }
}

describe('ConfirmDialog', () => {
  it('renders nothing when closed', () => {
    setup(false)
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('shows the title, description and the expected text', () => {
    setup()
    expect(screen.getByRole('dialog', { name: 'Delete cluster' })).toBeInTheDocument()
    expect(screen.getByText('This removes the cluster.')).toBeInTheDocument()
    expect(screen.getByText('stg-eu')).toBeInTheDocument()
  })

  it('keeps confirm disabled initially and for wrong text', async () => {
    const { user } = setup()
    const confirm = screen.getByRole('button', { name: 'Delete' })
    expect(confirm).toBeDisabled()
    await user.type(screen.getByLabelText(/type .* to confirm/i), 'stg')
    expect(confirm).toBeDisabled()
  })

  it('enables confirm when the expected text is typed and calls onConfirm with it', async () => {
    const { user, onConfirm } = setup()
    await user.type(screen.getByLabelText(/type .* to confirm/i), 'stg-eu')
    const confirm = screen.getByRole('button', { name: 'Delete' })
    expect(confirm).toBeEnabled()
    await user.click(confirm)
    expect(onConfirm).toHaveBeenCalledWith('stg-eu')
  })

  it('calls onCancel from the Cancel button', async () => {
    const { user, onCancel, onConfirm } = setup()
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(onCancel).toHaveBeenCalled()
    expect(onConfirm).not.toHaveBeenCalled()
  })
})
