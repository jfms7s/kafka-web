import { useState } from 'react'
import { Link } from 'react-router'
import {
  useClusters,
  useConnect,
  useDeleteCluster,
  useDisconnect,
} from '../api/hooks/clusters'
import type { ClusterView } from '../api/hooks/clusters'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { EnvBadge } from '../components/EnvBadge'
import { useToast } from '../components/Toasts'

const BUTTON = 'rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40'

function StateChip({ cluster }: { cluster: ClusterView }) {
  if (!cluster.usable) {
    return <span className="rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">unusable</span>
  }
  return cluster.connected ? (
    <span className="rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-800">connected</span>
  ) : (
    <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-600">idle</span>
  )
}

export function ClusterManager() {
  const { data: clusters, error, isPending } = useClusters()
  const connect = useConnect()
  const disconnect = useDisconnect()
  const remove = useDeleteCluster()
  const toast = useToast()
  const [pendingDelete, setPendingDelete] = useState<string | null>(null)

  return (
    <div>
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Clusters</h1>
        <Link to="/clusters/new" className="rounded bg-slate-900 px-3 py-1.5 text-sm font-medium text-white hover:bg-slate-700">
          Add cluster
        </Link>
      </div>

      {isPending && <p className="mt-6 text-slate-500">Loading clusters…</p>}
      {error && <p className="mt-6 text-red-700">Could not load clusters: {error.message}</p>}
      {clusters?.length === 0 && (
        <p className="mt-6 text-slate-600">No clusters yet. Add one to get started.</p>
      )}

      <div className="mt-6 grid gap-4 md:grid-cols-2">
        {clusters?.map((cluster) => (
          <article
            key={cluster.name}
            aria-label={cluster.name}
            className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm"
          >
            <div className="flex items-center gap-2">
              <h2 className="text-lg font-semibold text-slate-900">{cluster.name}</h2>
              <EnvBadge env={cluster.env} readOnly={cluster.read_only} />
              <span className="ml-auto">
                <StateChip cluster={cluster} />
              </span>
            </div>
            {cluster.region && <p className="mt-1 text-sm text-slate-600">{cluster.region}</p>}
            <p className="mt-1 font-mono text-sm text-slate-700">{cluster.bootstrap_servers}</p>
            {!cluster.usable && cluster.unusable_reason && (
              <p className="mt-2 text-sm text-red-700">{cluster.unusable_reason}</p>
            )}
            <div className="mt-4 flex flex-wrap gap-2">
              {cluster.connected ? (
                <button type="button" className={BUTTON} onClick={() => disconnect.mutate(cluster.name, { onError: toast.error })}>
                  Disconnect
                </button>
              ) : (
                <button
                  type="button"
                  className={BUTTON}
                  disabled={!cluster.usable || (connect.isPending && connect.variables === cluster.name)}
                  onClick={() => connect.mutate(cluster.name, { onError: toast.error })}
                >
                  Connect
                </button>
              )}
              {cluster.usable && (
                <Link to={`/c/${cluster.name}/topics`} className={BUTTON}>
                  Open
                </Link>
              )}
              <Link to={`/clusters/${cluster.name}/edit`} className={BUTTON}>
                Edit
              </Link>
              <button type="button" className={`${BUTTON} text-red-700`} onClick={() => setPendingDelete(cluster.name)}>
                Delete
              </button>
            </div>
          </article>
        ))}
      </div>

      <ConfirmDialog
        open={pendingDelete !== null}
        title={`Delete cluster ${pendingDelete ?? ''}`}
        description="This removes the cluster configuration, its saved secrets and truststore, and closes its connection."
        expected={pendingDelete ?? ''}
        confirmLabel="Delete cluster"
        onCancel={() => setPendingDelete(null)}
        onConfirm={() => {
          if (pendingDelete) remove.mutate(pendingDelete, { onError: toast.error })
          setPendingDelete(null)
        }}
      />
    </div>
  )
}
