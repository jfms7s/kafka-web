import { useId, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useNavigate, useParams } from 'react-router'
import { useCluster } from '../api/hooks/clusters'
import {
  groupsPath,
  useDeleteGroup,
  useGroup,
  usePreviewReset,
  useResetOffsets,
} from '../api/hooks/groups'
import type {
  GroupDetail as GroupDetailView,
  GroupOffset,
  ResetSpec,
  ResetStrategy,
  ResetTarget,
} from '../api/hooks/groups'
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
  const remove = useDeleteGroup(cluster, group)
  const [deleting, setDeleting] = useState(false)
  const blocked = blockedReason(readOnly, detail.state)

  const confirmDelete = (confirm: string) =>
    remove.mutate(confirm, {
      onSuccess: () => {
        toast.success(`Deleted group ${group}.`)
        void navigate(groupsPath(cluster))
      },
      onError: (failure) => {
        toast.error(failure)
        setDeleting(false)
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

      <ResetForm cluster={cluster} group={group} detail={detail} blocked={blocked} />

      <div>
        <span title={blocked ?? undefined}>
          <button
            type="button"
            disabled={blocked !== null || remove.isPending}
            onClick={() => setDeleting(true)}
            className="rounded border border-red-300 px-3 py-1.5 text-sm text-red-700 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Delete group
          </button>
        </span>
      </div>

      <ConfirmDialog
        open={deleting}
        title="Delete group"
        description={`Deletes ${group} and all its committed offsets. This cannot be undone.`}
        expected={group}
        confirmLabel="Delete group"
        onConfirm={confirmDelete}
        onCancel={() => setDeleting(false)}
      />
    </section>
  )
}

const STRATEGY_LABELS: Record<ResetStrategy, string> = {
  earliest: 'earliest',
  latest: 'latest',
  timestamp: 'timestamp',
  to_offset: 'to offset',
  shift_by: 'shift by',
}

/** One partition row of the reset form: whether it is reset and, for to_offset, to where. */
interface RowState {
  selected: boolean
  target: string
}

const rowKey = (o: GroupOffset) => `${o.topic}/${o.partition}`
const defaultRow = (o: GroupOffset): RowState => ({
  selected: true,
  target: o.committed === null ? '' : String(o.committed),
})
const asOffset = (text: string): number | null => {
  const value = Number(text.trim())
  return text.trim() !== '' && Number.isSafeInteger(value) ? value : null
}

interface ResetFormProps {
  cluster: string
  group: string
  detail: GroupDetailView
  blocked: string | null
}

/**
 * Reset one topic's partitions: all or some, to earliest / latest / a timestamp, to explicit
 * offsets or by a shift. The resolved targets are previewed before the typed confirmation.
 */
function ResetForm({ cluster, group, detail, blocked }: ResetFormProps) {
  const toast = useToast()
  const preview = usePreviewReset(cluster, group)
  const reset = useResetOffsets(cluster, group)
  const ids = { topic: useId(), strategy: useId(), timestamp: useId(), shift: useId() }

  const topics = useMemo(
    () => [...new Set(detail.offsets.map((o) => o.topic))].sort(),
    [detail.offsets],
  )
  const [topicChoice, setTopic] = useState('')
  const [strategy, setStrategy] = useState<ResetStrategy>('latest')
  const [timestamp, setTimestamp] = useState('')
  const [shift, setShift] = useState('')
  const [rowEdits, setRowEdits] = useState<Record<string, RowState>>({})
  const [formError, setFormError] = useState<string | null>(null)
  const [plan, setPlan] = useState<{ spec: ResetSpec; targets: ResetTarget[] } | null>(null)

  const topic = topics.includes(topicChoice) ? topicChoice : (topics[0] ?? '')
  const partitions = detail.offsets
    .filter((o) => o.topic === topic)
    .sort((a, b) => a.partition - b.partition)
  const row = (o: GroupOffset) => rowEdits[rowKey(o)] ?? defaultRow(o)
  const editRow = (o: GroupOffset, change: Partial<RowState>) =>
    setRowEdits((current) => ({ ...current, [rowKey(o)]: { ...row(o), ...change } }))
  const chosen = partitions.filter((o) => row(o).selected)

  /** The request body, or the reason the form is not ready. */
  function buildSpec(): ResetSpec | string {
    if (chosen.length === 0) return 'Choose at least one partition'
    const spec: ResetSpec = { topic, strategy }
    if (strategy === 'timestamp') {
      const millis = new Date(timestamp).getTime()
      if (Number.isNaN(millis)) return 'Timestamp is required'
      spec.timestamp = millis
    }
    if (strategy === 'shift_by') {
      const by = asOffset(shift)
      if (by === null) return 'Shift must be a whole number'
      spec.shift = by
    }
    if (strategy === 'to_offset') {
      const targets = chosen.map((o) => ({ partition: o.partition, offset: asOffset(row(o).target) }))
      const bad = targets.find((t) => t.offset === null || t.offset < 0)
      if (bad) return `Partition ${bad.partition} needs a target offset of 0 or more`
      spec.partitions = targets.map((t) => ({ partition: t.partition, offset: t.offset as number }))
    } else if (chosen.length < partitions.length) {
      spec.partitions = chosen.map((o) => ({ partition: o.partition }))
    }
    return spec
  }

  const requestReset = (event: FormEvent) => {
    event.preventDefault()
    const spec = buildSpec()
    if (typeof spec === 'string') {
      setFormError(spec)
      return
    }
    setFormError(null)
    preview.mutate(spec, {
      onSuccess: (targets) => setPlan({ spec, targets }),
      onError: (failure) => toast.error(failure),
    })
  }

  const confirmReset = (confirm: string) => {
    if (!plan) return
    reset.mutate(
      { ...plan.spec, confirm },
      {
        onSuccess: () => toast.success(`Offsets of ${group} reset.`),
        onError: (failure) => toast.error(failure),
        onSettled: () => setPlan(null),
      },
    )
  }

  return (
    <>
      <form onSubmit={requestReset} className="space-y-3">
        <div className="flex flex-wrap items-end gap-4">
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
              {Object.entries(STRATEGY_LABELS).map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
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
                className={INPUT}
              />
            </div>
          )}
          {strategy === 'shift_by' && (
            <div className="flex flex-col gap-1 text-sm text-slate-600">
              <label htmlFor={ids.shift}>Shift by</label>
              <input
                id={ids.shift}
                inputMode="numeric"
                placeholder="-1000 rewinds, 1 skips one"
                value={shift}
                onChange={(e) => setShift(e.target.value)}
                className={INPUT}
              />
            </div>
          )}
          <span title={blocked ?? undefined}>
            <button
              type="submit"
              disabled={blocked !== null || topics.length === 0 || preview.isPending || reset.isPending}
              className={BUTTON}
            >
              Reset offsets
            </button>
          </span>
        </div>
        {formError && <p className="text-sm text-red-700">{formError}</p>}

        {partitions.length > 0 && (
          <table aria-label="Partitions to reset" className={TABLE}>
            <thead className="bg-slate-100 text-left text-slate-600">
              <tr>
                <th className={TH}>Reset</th>
                <th className={TH}>Partition</th>
                <th className={TH}>Committed</th>
                <th className={TH}>End</th>
                {strategy === 'to_offset' && <th className={TH}>Target</th>}
              </tr>
            </thead>
            <tbody>
              {partitions.map((o) => (
                <tr key={rowKey(o)} className="border-t border-slate-100">
                  <td className="px-3 py-2">
                    <input
                      type="checkbox"
                      aria-label={`Reset partition ${o.partition}`}
                      checked={row(o).selected}
                      onChange={(e) => editRow(o, { selected: e.target.checked })}
                    />
                  </td>
                  <td className="px-3 py-2">{o.partition}</td>
                  <td className="px-3 py-2">{o.committed ?? UNKNOWN}</td>
                  <td className="px-3 py-2">{o.end ?? UNKNOWN}</td>
                  {strategy === 'to_offset' && (
                    <td className="flex items-center gap-2 px-3 py-2">
                      <input
                        aria-label={`Target offset of partition ${o.partition}`}
                        inputMode="numeric"
                        value={row(o).target}
                        disabled={!row(o).selected}
                        onChange={(e) => editRow(o, { target: e.target.value })}
                        className={`${INPUT} w-32`}
                      />
                      <button
                        type="button"
                        aria-label={`Skip one on partition ${o.partition}`}
                        title="Committed offset + 1: skips the next message"
                        disabled={!row(o).selected || o.committed === null}
                        onClick={() =>
                          o.committed !== null && editRow(o, { target: String(o.committed + 1) })
                        }
                        className={BUTTON}
                      >
                        Skip one
                      </button>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </form>

      <ConfirmDialog
        open={plan !== null}
        title="Reset offsets"
        description={
          plan
            ? `Resets ${group} on topic ${plan.spec.topic} (${STRATEGY_LABELS[plan.spec.strategy]}). Consumers will resume from these offsets; partitions not listed keep theirs.`
            : ''
        }
        expected={group}
        confirmLabel="Reset offsets"
        onConfirm={confirmReset}
        onCancel={() => setPlan(null)}
      >
        <ul aria-label="Reset targets" className="mt-3 max-h-48 overflow-y-auto font-mono text-sm">
          {plan?.targets.map((t) => (
            <li key={t.partition} className={t.clamped ? 'text-amber-800' : 'text-slate-800'}>
              partition {t.partition}: {t.current ?? 'none'} → {t.target}
              {t.clamped && ' (clamped to the log)'}
            </li>
          ))}
        </ul>
      </ConfirmDialog>
    </>
  )
}
