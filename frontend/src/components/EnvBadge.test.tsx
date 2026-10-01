import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { EnvBadge } from './EnvBadge'

describe('EnvBadge', () => {
  it.each([
    ['prd', 'bg-red-'],
    ['stg', 'bg-amber-'],
    ['qa', 'bg-sky-'],
    ['dev', 'bg-slate-'],
  ])('colours %s', (env, cls) => {
    render(<EnvBadge env={env} />)
    expect(screen.getByText(env).className).toContain(cls)
  })

  it('appends a lock when read-only', () => {
    render(<EnvBadge env="prd" readOnly />)
    expect(screen.getByText('prd 🔒')).toBeInTheDocument()
  })

  it('has no lock otherwise', () => {
    render(<EnvBadge env="prd" />)
    expect(screen.getByText('prd')).toBeInTheDocument()
  })
})
