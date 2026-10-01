import { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { ApiError } from '../api/client'

const LIFETIME_MS = 8000

interface ToastItem {
  id: number
  message: string
  code?: string
}

interface ToastApi {
  error: (error: unknown) => void
}

const ToastContext = createContext<ToastApi | null>(null)

// eslint-disable-next-line react-refresh/only-export-components
export function useToast(): ToastApi {
  const api = useContext(ToastContext)
  if (!api) throw new Error('useToast must be used inside <ToastProvider>')
  return api
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([])
  const nextId = useRef(0)

  const dismiss = useCallback((id: number) => {
    setToasts((current) => current.filter((t) => t.id !== id))
  }, [])

  const error = useCallback(
    (cause: unknown) => {
      const id = nextId.current++
      const item: ToastItem =
        cause instanceof ApiError
          ? { id, message: cause.message, code: cause.code }
          : { id, message: cause instanceof Error ? cause.message : 'Something went wrong' }
      setToasts((current) => [...current, item])
      setTimeout(() => dismiss(id), LIFETIME_MS)
    },
    [dismiss],
  )

  const api = useMemo(() => ({ error }), [error])

  return (
    <ToastContext.Provider value={api}>
      {children}
      <div className="fixed right-4 bottom-4 z-50 flex w-96 max-w-[calc(100vw-2rem)] flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            role="alert"
            className="flex items-start gap-3 rounded border border-red-300 bg-red-50 p-3 text-sm text-red-900 shadow"
          >
            <div className="flex-1">
              <p>{t.message}</p>
              {t.code && <p className="mt-1 font-mono text-xs text-red-700">{t.code}</p>}
            </div>
            <button
              type="button"
              onClick={() => dismiss(t.id)}
              className="rounded px-2 text-red-700 hover:bg-red-100"
            >
              Dismiss
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}
