import { Link, useMatch, useNavigate } from 'react-router'
import { useCluster, useStatus } from '../api/hooks/clusters'
import { EnvBadge } from './EnvBadge'

export function Header() {
  const route = useMatch('/c/:cluster/*')
  const current = route?.params.cluster
  const navigate = useNavigate()
  const { data: status } = useStatus()
  const currentCluster = useCluster(current ?? '', current !== undefined).data

  const connected = status?.connections ?? []
  const options = connected.map(optionOf)
  if (current && !options.some((o) => o.name === current)) {
    options.push({ name: current, label: current }) // opened but not (yet) connected
  }

  return (
    <header className="relative border-b border-slate-200 bg-white">
      {currentCluster?.env === 'prd' && (
        <div data-testid="prd-strip" aria-hidden="true" className="absolute inset-x-0 top-0 h-1 bg-red-600" />
      )}
      <div className="mx-auto flex max-w-7xl items-center gap-4 px-4 py-3">
        <Link to="/" className="text-lg font-semibold text-slate-900">
          kafka-web
        </Link>
        <Link to="/" className="text-sm text-slate-600 hover:text-slate-900">
          Clusters
        </Link>
        <div className="ml-auto flex items-center gap-2">
          {currentCluster && <EnvBadge env={currentCluster.env} readOnly={currentCluster.read_only} />}
          <label htmlFor="cluster-switcher" className="sr-only">
            Cluster
          </label>
          <select
            id="cluster-switcher"
            value={current ?? ''}
            onChange={(e) => e.target.value && void navigate(`/c/${e.target.value}/topics`)}
            className="rounded border border-slate-300 px-2 py-1 text-sm"
          >
            {!current && <option value="">Switch cluster…</option>}
            {options.map((o) => (
              <option key={o.name} value={o.name}>
                {o.label}
              </option>
            ))}
          </select>
        </div>
      </div>
    </header>
  )
}

function optionOf(c: { name: string; env: string; read_only: boolean }) {
  return { name: c.name, label: `${c.name} (${c.env})${c.read_only ? ' 🔒' : ''}` }
}
