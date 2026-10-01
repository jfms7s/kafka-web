import { useTopicConfig } from '../api/hooks/topics'

const TH = 'px-3 py-2 font-medium'
const TABLE =
  'w-full border-collapse overflow-hidden rounded-lg border border-slate-200 bg-white text-sm'

export function ConfigTab({ cluster, topic }: { cluster: string; topic: string }) {
  const { data, error, isPending } = useTopicConfig(cluster, topic)

  if (isPending) return <p className="text-slate-500">Loading configuration…</p>
  if (error) return <p className="text-red-700">Could not load configuration: {error.message}</p>

  const count = data.partitions.length
  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-lg font-semibold text-slate-900">Partitions</h2>
        <p className="mt-1 text-sm text-slate-600">
          {count} {count === 1 ? 'partition' : 'partitions'} · replication factor {data.replication_factor}
        </p>
        <table className={`${TABLE} mt-3`}>
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className={TH}>Partition</th>
              <th className={TH}>Leader</th>
              <th className={TH}>Replicas</th>
              <th className={TH}>ISR</th>
            </tr>
          </thead>
          <tbody>
            {data.partitions.map((p) => (
              <tr key={p.id} className="border-t border-slate-100">
                <td className="px-3 py-2">{p.id}</td>
                <td className="px-3 py-2">{p.leader}</td>
                <td className="px-3 py-2">{p.replicas.join(', ')}</td>
                <td className="px-3 py-2">{p.isr.join(', ')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h2 className="text-lg font-semibold text-slate-900">Configuration</h2>
        <p className="mt-1 text-sm text-slate-600">Highlighted rows differ from the defaults.</p>
        <table className={`${TABLE} mt-3`}>
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className={TH}>Name</th>
              <th className={TH}>Value</th>
              <th className={TH}>Source</th>
            </tr>
          </thead>
          <tbody>
            {data.entries.map((entry) => (
              <tr
                key={entry.name}
                data-default={entry.is_default}
                className={`border-t border-slate-100 ${entry.is_default ? '' : 'bg-amber-50'}`}
              >
                <td className="px-3 py-2 font-mono">{entry.name}</td>
                <td className="px-3 py-2 font-mono">
                  {entry.sensitive ? (
                    <span className="text-slate-500 italic">hidden</span>
                  ) : (
                    <span title={entry.value ?? undefined}>{entry.display_value ?? '—'}</span>
                  )}
                </td>
                <td className="px-3 py-2 text-slate-500">{entry.source}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  )
}
