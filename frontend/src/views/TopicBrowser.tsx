import { useState } from 'react'
import { Link, useParams } from 'react-router'
import { topicPath, useTopics } from '../api/hooks/topics'

export function TopicBrowser() {
  const { cluster = '' } = useParams()
  const { data: topics, error, isPending, isFetching, refetch } = useTopics(cluster)
  const [filter, setFilter] = useState('')

  const needle = filter.trim().toLowerCase()
  const visible = topics?.filter((t) => t.name.toLowerCase().includes(needle))

  return (
    <div>
      <div className="flex items-center justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">Topics</h1>
        <button
          type="button"
          onClick={() => void refetch()}
          disabled={isFetching}
          className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50 disabled:opacity-40"
        >
          Refresh
        </button>
      </div>

      <div className="mt-4 flex items-center gap-4">
        <input
          type="search"
          aria-label="Filter topics"
          placeholder="Filter topics…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="w-full max-w-sm rounded border border-slate-300 px-3 py-1.5 text-sm"
        />
        {topics && visible && (
          <p className="text-sm text-slate-500">
            {needle ? `${visible.length} of ${topics.length} topics` : `${topics.length} topics`}
          </p>
        )}
      </div>

      {isPending && <p className="mt-6 text-slate-500">Loading topics…</p>}
      {error && <p className="mt-6 text-red-700">Could not load topics: {error.message}</p>}
      {visible?.length === 0 && needle && (
        <p className="mt-6 text-slate-600">No topics match “{filter.trim()}”.</p>
      )}

      {visible && visible.length > 0 && (
        <table className="mt-4 w-full border-collapse overflow-hidden rounded-lg border border-slate-200 bg-white text-sm">
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className="px-3 py-2 font-medium">Name</th>
              <th className="px-3 py-2 font-medium">Partitions</th>
              <th className="px-3 py-2 font-medium">Replication factor</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((topic) => (
              <tr key={topic.name} className="border-t border-slate-100 hover:bg-slate-50">
                <td className="px-3 py-2">
                  <Link to={topicPath(cluster, topic.name)} className="font-mono text-slate-900 underline-offset-2 hover:underline">
                    {topic.name}
                  </Link>
                  {topic.internal && (
                    <span className="ml-2 rounded-full bg-slate-200 px-2 py-0.5 text-xs text-slate-700">
                      internal
                    </span>
                  )}
                </td>
                <td className="px-3 py-2">{topic.partitions}</td>
                <td className="px-3 py-2">{topic.replication_factor}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
