import { useId, useState } from 'react'

interface Props {
  open: boolean
  title: string
  description: string
  expected: string
  confirmLabel: string
  onConfirm: (typed: string) => void
  onCancel: () => void
}

export function ConfirmDialog(props: Props) {
  // Remount on open so the typed text never survives from a previous confirmation.
  return props.open ? <Dialog {...props} /> : null
}

function Dialog({ title, description, expected, confirmLabel, onConfirm, onCancel }: Props) {
  const [typed, setTyped] = useState('')
  const titleId = useId()
  const inputId = useId()
  const matches = typed === expected

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-4">
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="w-full max-w-md rounded-lg bg-white p-6 shadow-xl"
        onKeyDown={(e) => e.key === 'Escape' && onCancel()}
      >
        <h2 id={titleId} className="text-lg font-semibold text-slate-900">
          {title}
        </h2>
        <p className="mt-2 text-sm text-slate-600">{description}</p>
        <label htmlFor={inputId} className="mt-4 block text-sm text-slate-700">
          Type <code className="rounded bg-slate-100 px-1 font-mono">{expected}</code> to confirm
        </label>
        <input
          id={inputId}
          autoFocus
          autoComplete="off"
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          className="mt-1 w-full rounded border border-slate-300 px-3 py-2 font-mono text-sm"
        />
        <div className="mt-6 flex justify-end gap-2">
          <button
            type="button"
            onClick={onCancel}
            className="rounded border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={!matches}
            onClick={() => onConfirm(typed)}
            className="rounded bg-red-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
