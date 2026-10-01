import { useId, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useNavigate, useParams } from 'react-router'
import { useCluster } from '../api/hooks/clusters'
import {
  groupsPath,
  useDeleteGroup,
  useGroup,
  useResetOffsets,
} from '../api/hooks/groups'
import type { GroupDetail as GroupDetailView, ResetStrategy } from '../api/hooks/groups'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { GroupStateBadge } from '../components/GroupStateBadge'
import { useToast } from '../components/Toasts'

const BUTTON =
  'rounded border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40'
const INPUT = 'rounded border border-slate-300 px-2 py-1.5 text-sm'
const TABLE =
  'w-full border-collapse overflow-hidden rounded-lg border border-slate-200 bg-white text-sm'
const TH = 'px-3 py-2 font-medium'
const UNKNOWN = '—'

export function GroupDetail() {
  const { cluster = '', group = '' } = useParams()
  const { data, error, isPending, isFetching, refetch } = useGroup(cluster, group)
  const { data: clusterInfo } = useCluster(cluster)

  return (
    <div>
      <p className="text-sm">
        <Link
          to={groupsPath(cluster)}
          className="text-slate-600 underline-offset-2 hover:underline"
        >
          ← Consumer Groups
        </Link>
      </p>
      <div className="mt-2 mb-4 flex items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <h1 className="font-mono text-2xl font-semibold break-all text-slate-900">{group}</h1>
          {data && <GroupStateBadge state={data.state} />}
          {data && <span className="text-sm text-slate-500">{data.type}</span>}
        </div>
        <button
          type="button"
          onClick={() => void refetch()}
          disabled={isFetching}
          className={BUTTON}
        >
          Refresh
        </button>
      </div>

      {isPending && <p className="text-slate-500">Loading group…</p>}
      {error && <p className="text-red-700">Could not load the group: {error.message}</p>}
      {data && (
        <div className="space-y-8">
          <Members detail={data} />
          <Offsets detail={data} />
          <Actions
            cluster={cluster}
            group={group}
            detail={data}
            readOnly={clusterInfo?.read_only ?? true}
          />
        </div>
      )}
    </div>
  )
}

function Members({ detail }: { detail: GroupDetailView }) {
  return (
    <section aria-labelledby="group-members">
      <h2 id="group-members" className="mb-2 text-lg font-semibold text-slate-900">
        Members
      </h2>
      {detail.members.length === 0 ? (
        <p className="text-sm text-slate-600">No active members.</p>
      ) : (
        <table aria-label="Members" className={TABLE}>
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className={TH}>Member id</th>
              <th className={TH}>Client id</th>
              <th className={TH}>Host</th>
              <th className={TH}>Assigned partitions</th>
            </tr>
          </thead>
          <tbody>
            {detail.members.map((m) => (
              <tr key={m.member_id} className="border-t border-slate-100">
                <td className="px-3 py-2 font-mono break-all">{m.member_id}</td>
                <td className="px-3 py-2">{m.client_id}</td>
                <td className="px-3 py-2">{m.host}</td>
                <td className="px-3 py-2 font-mono">
                  {m.assignments.map((a) => `${a.topic}-${a.partition}`).join(', ')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}

function Offsets({ detail }: { detail: GroupDetailView }) {
  const known = detail.offsets.filter((o) => o.lag !== null)
  const totalLag = known.reduce((sum, o) => sum + (o.lag ?? 0), 0)
  const unknown = detail.offsets.length - known.length
  return (
    <section aria-labelledby="group-offsets">
      <div className="mb-2 flex items-baseline gap-4">
        <h2 id="group-offsets" className="text-lg font-semibold text-slate-900">
          Offsets
        </h2>
        <p className="text-sm font-medium text-slate-800">Total lag: {totalLag}</p>
        {unknown > 0 && (
          <p className="text-sm text-slate-500">
            {unknown} {unknown === 1 ? 'partition' : 'partitions'} with unknown lag
          </p>
        )}
      </div>
      {detail.offsets.length === 0 ? (
        <p className="text-sm text-slate-600">This group has no committed offsets.</p>
      ) : (
        <table aria-label="Offsets" className={TABLE}>
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className={TH}>Topic</th>
              <th className={TH}>Partition</th>
              <th className={TH}>Committed</th>
              <th className={TH}>End</th>
              <th className={TH}>Lag</th>
            </tr>
          </thead>
          <tbody>
            {detail.offsets.map((o) => (
              <tr key={`${o.topic}/${o.partition}`} className="border-t border-slate-100">
                <td className="px-3 py-2 font-mono">{o.topic}</td>
                <td className="px-3 py-2">{o.partition}</td>
                <td className="px-3 py-2">{o.committed ?? UNKNOWN}</td>
                <td className="px-3 py-2">{o.end ?? UNKNOWN}</td>
                <td className="px-3 py-2">{o.lag ?? UNKNOWN}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}

interface ActionsProps {
  cluster: string
  group: string
  detail: GroupDetailView
  readOnly: boolean
}

/** Why the write actions are unavailable, or null when they are. */
function blockedReason(readOnly: boolean, state: string): string | null {
  if (readOnly) return 'This cluster is read-only, so group offsets and groups cannot be changed.'
  if (state !== 'empty') {
    return `The group must be empty (it is ${state}); stop its consumers first.`
  }
  return null
}

function Actions({ cluster, group, detail, readOnly }: ActionsProps) {
  const toast = useToast()
  const navigate = useNavigate()
  const reset = useResetOffsets(cluster, group)
  const remove = useDeleteGroup(cluster, group)
  const ids = { topic: useId(), strategy: useId(), timestamp: useId() }

  const topics = useMemo(
    () => [...new Set(detail.offsets.map((o) => o.topic))].sort(),
    [detail.offsets],
  )
  const [topicChoice, setTopic] = useState('')
  const [strategy, setStrategy] = useState<ResetStrategy>('latest')
  const [timestamp, setTimestamp] = useState('')
  const [timestampError, setTimestampError] = useState<string | null>(null)
  const [dialog, setDialog] = useState<'reset' | 'delete' | null>(null)

  const topic = topics.includes(topicChoice) ? topicChoice : (topics[0] ?? '')
  const blocked = blockedReason(readOnly, detail.state)

  const requestReset = (event: FormEvent) => {
    event.preventDefault()
    if (strategy === 'timestamp' && Number.isNaN(new Date(timestamp).getTime())) {
      setTimestampError('Timestamp is required')
      return
    }
    setTimestampError(null)
    setDialog('reset')
  }

  const confirmReset = (confirm: string) =>
    reset.mutate(
      {
        topic,
        strategy,
        ...(strategy === 'timestamp' && { timestamp: new Date(timestamp).getTime() }),
        confirm,
      },
      {
        onSuccess: () => toast.success(`Offsets of ${group} reset.`),
        onError: (failure) => toast.error(failure),
        onSettled: () => setDialog(null),
      },
    )

  const confirmDelete = (confirm: string) =>
    remove.mutate(confirm, {
      onSuccess: () => {
        toast.success(`Deleted group ${group}.`)
        void navigate(groupsPath(cluster))
      },
      onError: (failure) => {
        toast.error(failure)
        setDialog(null)
      },
    })

  return (
    <section aria-labelledby="group-actions" className="space-y-4">
      <h2 id="group-actions" className="text-lg font-semibold text-slate-900">
        Actions
      </h2>
      {blocked && (
        <p className="rounded border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900">
          {blocked}
        </p>
      )}

      <form onSubmit={requestReset} className="flex flex-wrap items-end gap-4">
        <div className="flex flex-col gap-1 text-sm text-slate-600">
          <label htmlFor={ids.topic}>Topic</label>
          <select
            id={ids.topic}
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
            className={INPUT}
          >
            {topics.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>
        <div className="flex flex-col gap-1 text-sm text-slate-600">
          <label htmlFor={ids.strategy}>Strategy</label>
          <select
            id={ids.strategy}
            value={strategy}
            onChange={(e) => setStrategy(e.target.value as ResetStrategy)}
            className={INPUT}
          >
            <option value="earliest">earliest</option>
            <option value="latest">latest</option>
            <option value="timestamp">timestamp</option>
          </select>
        </div>
        {strategy === 'timestamp' && (
          <div className="flex flex-col gap-1 text-sm text-slate-600">
            <label htmlFor={ids.timestamp}>Timestamp</label>
            <input
              id={ids.timestamp}
              type="datetime-local"
              value={timestamp}
              onChange={(e) => setTimestamp(e.target.value)}
              aria-invalid={timestampError ? true : undefined}
              className={INPUT}
            />
            {timestampError && <p className="text-red-700">{timestampError}</p>}
          </div>
        )}
        <span title={blocked ?? undefined}>
          <button
            type="submit"
            disabled={blocked !== null || topics.length === 0 || reset.isPending}
            className={BUTTON}
          >
            Reset offsets
          </button>
        </span>
      </form>

      <div>
        <span title={blocked ?? undefined}>
          <button
            type="button"
            disabled={blocked !== null || remove.isPending}
            onClick={() => setDialog('delete')}
            className="rounded border border-red-300 px-3 py-1.5 text-sm text-red-700 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Delete group
          </button>
        </span>
      </div>

      <ConfirmDialog
        open={dialog === 'reset'}
        title="Reset offsets"
        description={`Resets ${group} on topic ${topic} to ${strategy}. Consumers will resume from there.`}
        expected={group}
        confirmLabel="Reset offsets"
        onConfirm={confirmReset}
        onCancel={() => setDialog(null)}
      />
      <ConfirmDialog
        open={dialog === 'delete'}
        title="Delete group"
        description={`Deletes ${group} and all its committed offsets. This cannot be undone.`}
        expected={group}
        confirmLabel="Delete group"
        onConfirm={confirmDelete}
        onCancel={() => setDialog(null)}
      />
    </section>
  )
}
