import { useCallback, useEffect, useRef, useState } from 'react'
import { pushCapped } from '../../lib/ringBuffer'
import type { MessageView } from './messages'

/** Messages kept in the browser; older ones fall off the front. */
export const LIVE_BUFFER_SIZE = 5000

export type LiveStatus = 'idle' | 'connecting' | 'live' | 'closed' | 'error'

export interface LiveParams {
  start: 'latest' | 'offset' | 'timestamp'
  offset?: number
  timestamp?: number // epoch ms
  partition?: number
}

export interface LiveError {
  code: string
  message: string
}

/** Frames the server sends (backend `api/stream.py`). */
type Frame =
  | { type: 'messages'; items: MessageView[] }
  | { type: 'dropped'; count: number }
  | { type: 'error'; code: string; message: string }
  | { type: 'closed'; reason: string }

const CLUSTER_CHANGED: LiveError = {
  code: 'cluster_changed',
  message: 'The stream stopped: the cluster was edited, deleted or disconnected.',
}

/** The stream endpoint on this page's own origin (`ws:` or `wss:` to match the page). */
export function streamUrl(cluster: string, topic: string, params: LiveParams): string {
  const query = new URLSearchParams({ start: params.start })
  if (params.offset !== undefined) query.set('offset', String(params.offset))
  if (params.timestamp !== undefined) query.set('timestamp', String(params.timestamp))
  if (params.partition !== undefined) query.set('partition', String(params.partition))
  const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:'
  const path = `/api/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(topic)}/stream`
  return `${scheme}//${location.host}${path}?${query}`
}

/** Ask the server to stop (when it can hear us), then close; no handler of ours fires after. */
function release(ws: WebSocket) {
  ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
  if (ws.readyState === WebSocket.OPEN) ws.send('stop')
  if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close(1000)
}

interface Options {
  cluster: string
  topic: string
  params: LiveParams
}

/**
 * A live topic stream over a WebSocket. `start()` (re)opens it with the current `params` and an
 * empty buffer; `stop()`, unmounting, or switching cluster/topic sends "stop" and closes it.
 */
export function useLiveStream({ cluster, topic, params }: Options) {
  const [status, setStatus] = useState<LiveStatus>('idle')
  const [messages, setMessages] = useState<MessageView[]>([])
  const [dropped, setDropped] = useState(0)
  const [error, setError] = useState<LiveError | null>(null)
  const socket = useRef<WebSocket | null>(null)

  const reset = useCallback((next: LiveStatus) => {
    setMessages([])
    setDropped(0)
    setError(null)
    setStatus(next)
  }, [])

  const stop = useCallback(() => {
    const ws = socket.current
    if (!ws) return
    socket.current = null
    release(ws)
    setStatus('closed')
  }, [])

  const start = useCallback(() => {
    if (socket.current) release(socket.current)
    reset('connecting')
    const ws = new WebSocket(streamUrl(cluster, topic, params))
    socket.current = ws
    let ended = false // a final error/closed frame arrived; the close that follows is expected

    ws.onopen = () => setStatus('live')
    ws.onmessage = (event: MessageEvent<string>) => {
      const frame = JSON.parse(event.data) as Frame
      switch (frame.type) {
        case 'messages':
          setMessages((current) => pushCapped(current, frame.items, LIVE_BUFFER_SIZE))
          break
        case 'dropped':
          setDropped((current) => current + frame.count)
          break
        case 'error':
          ended = true
          setError({ code: frame.code, message: frame.message })
          setStatus('error')
          break
        case 'closed':
          ended = true
          setError(CLUSTER_CHANGED)
          setStatus('closed')
          break
      }
    }
    ws.onclose = (event: CloseEvent) => {
      socket.current = null
      if (ended) return
      if (event.code === 1000) {
        setStatus('closed')
        return
      }
      setError({
        code: 'connection_lost',
        message: `The stream connection closed unexpectedly (code ${event.code}).`,
      })
      setStatus('error')
    }
  }, [cluster, topic, params, reset])

  // Unmount, or another cluster/topic in the same view: stop the stream and forget its messages.
  useEffect(
    () => () => {
      if (socket.current) release(socket.current)
      socket.current = null
      reset('idle')
    },
    [cluster, topic, reset],
  )

  return { status, messages, dropped, error, start, stop }
}
