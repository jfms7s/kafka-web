import { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { ApiError } from '../api/client'

const LIFETIME_MS = 8000

interface ToastItem {
  id: number
  kind: 'error' | 'success'
  message: string
  code?: string
}

interface ToastApi {
  error: (error: unknown) => void
  success: (message: string) => void
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

  const show = useCallback(
    (item: Omit<ToastItem, 'id'>) => {
      const id = nextId.current++
      setToasts((current) => [...current, { id, ...item }])
      setTimeout(() => dismiss(id), LIFETIME_MS)
    },
    [dismiss],
  )

  const error = useCallback(
    (cause: unknown) =>
      show(
        cause instanceof ApiError
          ? { kind: 'error', message: cause.message, code: cause.code }
          : {
              kind: 'error',
              message: cause instanceof Error ? cause.message : 'Something went wrong',
            },
      ),
    [show],
  )

  const success = useCallback((message: string) => show({ kind: 'success', message }), [show])

  const api = useMemo(() => ({ error, success }), [error, success])

  return (
    <ToastContext.Provider value={api}>
      {children}
      <div className="fixed right-4 bottom-4 z-50 flex w-96 max-w-[calc(100vw-2rem)] flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            role={t.kind === 'error' ? 'alert' : 'status'}
            className={`flex items-start gap-3 rounded border p-3 text-sm shadow ${
              t.kind === 'error'
                ? 'border-red-300 bg-red-50 text-red-900'
                : 'border-green-300 bg-green-50 text-green-900'
            }`}
          >
            <div className="flex-1">
              <p>{t.message}</p>
              {t.code && <p className="mt-1 font-mono text-xs text-red-700">{t.code}</p>}
            </div>
            <button
              type="button"
              onClick={() => dismiss(t.id)}
              className={`rounded px-2 ${
                t.kind === 'error' ? 'text-red-700 hover:bg-red-100' : 'text-green-800 hover:bg-green-100'
              }`}
            >
              Dismiss
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}
