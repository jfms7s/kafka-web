import { useVirtualizer } from '@tanstack/react-virtual'
import { useEffect, useRef, useState } from 'react'
import type { MessageView } from '../api/hooks/messages'
import { formatTimestamp, messageId } from './decoded'
import { DecodedPreview, DecodedValue } from './DecodedValue'

const COLUMNS = 'grid grid-cols-[5rem_5rem_14rem_12rem_minmax(0,1fr)] items-center gap-3 px-3'
const ROW_ESTIMATE_PX = 36

function Detail({ message }: { message: MessageView }) {
  return (
    <section
      aria-label={`Message ${messageId(message)}`}
      className="space-y-3 border-t border-slate-200 bg-slate-50 px-3 py-3 text-sm"
    >
      <div>
        <h3 className="mb-1 text-xs font-medium tracking-wide text-slate-500 uppercase">Key</h3>
        <DecodedValue value={message.key} />
      </div>
      <div>
        <h3 className="mb-1 text-xs font-medium tracking-wide text-slate-500 uppercase">Value</h3>
        <DecodedValue value={message.value} />
      </div>
      <div>
        <h3 className="mb-1 text-xs font-medium tracking-wide text-slate-500 uppercase">Headers</h3>
        {message.headers.length === 0 ? (
          <p className="text-slate-400 italic">none</p>
        ) : (
          <dl className="space-y-2">
            {message.headers.map((header, i) => (
              <div key={i} className="grid grid-cols-[10rem_1fr] gap-3">
                <dt className="font-mono break-all">{header.key}</dt>
                <dd>
                  <DecodedValue value={header.value} />
                </dd>
              </div>
            ))}
          </dl>
        )}
      </div>
    </section>
  )
}

interface MessageTableProps {
  messages: readonly MessageView[]
  /** Keep the newest (last) message in view whenever `messages` changes. */
  followTail?: boolean
}

/**
 * A virtualized, presentation-only table of messages; clicking a row expands its detail.
 * Which rows are expanded is the only state it owns, so any message source can feed it.
 */
export function MessageTable({ messages, followTail = false }: MessageTableProps) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set())

  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_ESTIMATE_PX,
    getItemKey: (index) => messageId(messages[index]),
    overscan: 8,
  })

  // Keyed on the array, not its length: a full live buffer keeps its length while it rolls.
  useEffect(() => {
    if (followTail && messages.length > 0) {
      virtualizer.scrollToIndex(messages.length - 1, { align: 'end' })
    }
  }, [followTail, messages, virtualizer])

  const toggle = (id: string) =>
    setExpanded((current) => {
      const next = new Set(current)
      if (!next.delete(id)) next.add(id)
      return next
    })

  return (
    <div className="overflow-hidden rounded-lg border border-slate-200 bg-white text-sm">
      <div className={`${COLUMNS} bg-slate-100 py-2 font-medium text-slate-600`}>
        <span>Partition</span>
        <span>Offset</span>
        <span>Timestamp</span>
        <span>Key</span>
        <span>Value</span>
      </div>
      <div ref={scrollRef} data-testid="message-scroll" className="h-[28rem] overflow-auto">
        <div className="relative w-full" style={{ height: virtualizer.getTotalSize() }}>
          {virtualizer.getVirtualItems().map((item) => {
            const message = messages[item.index]
            const id = messageId(message)
            const open = expanded.has(id)
            return (
              <div
                key={item.key}
                data-index={item.index}
                ref={virtualizer.measureElement}
                className="absolute top-0 left-0 w-full border-t border-slate-100"
                style={{ transform: `translateY(${item.start}px)` }}
              >
                <button
                  type="button"
                  aria-expanded={open}
                  onClick={() => toggle(id)}
                  className={`${COLUMNS} w-full py-2 text-left hover:bg-slate-50`}
                >
                  <span>{message.partition}</span>
                  <span>{message.offset}</span>
                  <span className="font-mono text-xs">{formatTimestamp(message.timestamp)}</span>
                  <span className="min-w-0 font-mono text-xs">
                    <DecodedPreview value={message.key} />
                  </span>
                  <span className="min-w-0 font-mono text-xs">
                    <DecodedPreview value={message.value} />
                  </span>
                </button>
                {open && <Detail message={message} />}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
