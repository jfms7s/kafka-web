import { useId, useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useParams } from 'react-router'
import { useCluster } from '../api/hooks/clusters'
import { groupsPath, useCreateGroup, useGroups } from '../api/hooks/groups'
import type { CreateGroupRequest } from '../api/hooks/groups'
import { useTopics } from '../api/hooks/topics'
import { GroupStateBadge } from '../components/GroupStateBadge'
import { useToast } from '../components/Toasts'

const BUTTON =
  'rounded border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40'
const INPUT = 'w-full rounded border border-slate-300 px-2 py-1.5 text-sm'

export function ConsumerGroups() {
  const { cluster = '' } = useParams()
  const { data: groups, error, isPending, isFetching, refetch } = useGroups(cluster)
  const { data: clusterInfo } = useCluster(cluster)
  const [filter, setFilter] = useState('')
  const [creating, setCreating] = useState(false)

  const needle = filter.trim().toLowerCase()
  const visible = groups?.filter((g) => g.group_id.toLowerCase().includes(needle))
  const canCreate = clusterInfo !== undefined && !clusterInfo.read_only

  return (
    <div>
      <div className="flex items-center justify-between gap-4">
        <h1 className="text-2xl font-semibold text-slate-900">Consumer Groups</h1>
        <div className="flex gap-2">
          {canCreate && !creating && (
            <button type="button" onClick={() => setCreating(true)} className={BUTTON}>
              Create group
            </button>
          )}
          <button
            type="button"
            onClick={() => void refetch()}
            disabled={isFetching}
            className={BUTTON}
          >
            Refresh
          </button>
        </div>
      </div>

      {canCreate && creating && (
        <CreateGroupForm cluster={cluster} onDone={() => setCreating(false)} />
      )}

      <div className="mt-4 flex items-center gap-4">
        <input
          type="search"
          aria-label="Filter groups"
          placeholder="Filter groups…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="w-full max-w-sm rounded border border-slate-300 px-3 py-1.5 text-sm"
        />
        {groups && visible && (
          <p className="text-sm text-slate-500">
            {needle ? `${visible.length} of ${groups.length} groups` : `${groups.length} groups`}
          </p>
        )}
      </div>

      {isPending && <p className="mt-6 text-slate-500">Loading groups…</p>}
      {error && <p className="mt-6 text-red-700">Could not load groups: {error.message}</p>}
      {visible?.length === 0 && needle && (
        <p className="mt-6 text-slate-600">No groups match “{filter.trim()}”.</p>
      )}

      {visible && visible.length > 0 && (
        <table className="mt-4 w-full border-collapse overflow-hidden rounded-lg border border-slate-200 bg-white text-sm">
          <thead className="bg-slate-100 text-left text-slate-600">
            <tr>
              <th className="px-3 py-2 font-medium">Group id</th>
              <th className="px-3 py-2 font-medium">State</th>
              <th className="px-3 py-2 font-medium">Type</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((group) => (
              <tr key={group.group_id} className="border-t border-slate-100 hover:bg-slate-50">
                <td className="px-3 py-2">
                  <Link
                    to={groupsPath(cluster, group.group_id)}
                    className="font-mono text-slate-900 underline-offset-2 hover:underline"
                  >
                    {group.group_id}
                  </Link>
                </td>
                <td className="px-3 py-2">
                  <GroupStateBadge state={group.state} />
                </td>
                <td className="px-3 py-2">{group.type}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function CreateGroupForm({ cluster, onDone }: { cluster: string; onDone: () => void }) {
  const toast = useToast()
  const create = useCreateGroup(cluster)
  const { data: topics } = useTopics(cluster)
  const ids = { group: useId(), topic: useId(), start: useId() }
  const [groupId, setGroupId] = useState('')
  const [topic, setTopic] = useState('')
  const [start, setStart] = useState<CreateGroupRequest['start']>('earliest')
  const [errors, setErrors] = useState<{ group_id?: string; topic?: string }>({})

  const submit = (event: FormEvent) => {
    event.preventDefault()
    const problems = {
      ...(groupId.trim() === '' && { group_id: 'Group id is required' }),
      ...(topic === '' && { topic: 'Topic is required' }),
    }
    setErrors(problems)
    if (Object.keys(problems).length > 0) return
    create.mutate(
      { group_id: groupId.trim(), topic, start },
      {
        onSuccess: (created) => {
          toast.success(`Created group ${created.group_id}.`)
          onDone()
        },
        onError: (failure) => toast.error(failure),
      },
    )
  }

  return (
    <form
      onSubmit={submit}
      aria-label="Create group"
      className="mt-4 max-w-xl space-y-3 rounded-lg border border-slate-200 bg-white p-4"
    >
      <p className="text-sm text-slate-600">
        A new group is created by committing its starting offsets for every partition of a topic.
      </p>
      <div className="flex flex-col gap-1 text-sm text-slate-600">
        <label htmlFor={ids.group}>Group id</label>
        <input
          id={ids.group}
          autoComplete="off"
          value={groupId}
          onChange={(e) => setGroupId(e.target.value)}
          aria-invalid={errors.group_id ? true : undefined}
          className={`${INPUT} font-mono`}
        />
        {errors.group_id && <p className="text-red-700">{errors.group_id}</p>}
      </div>
      <div className="flex flex-col gap-1 text-sm text-slate-600">
        <label htmlFor={ids.topic}>Topic</label>
        <select
          id={ids.topic}
          value={topic}
          onChange={(e) => setTopic(e.target.value)}
          aria-invalid={errors.topic ? true : undefined}
          className={INPUT}
        >
          <option value="">Select a topic…</option>
          {topics
            ?.filter((t) => !t.internal)
            .map((t) => (
              <option key={t.name} value={t.name}>
                {t.name}
              </option>
            ))}
        </select>
        {errors.topic && <p className="text-red-700">{errors.topic}</p>}
      </div>
      <div className="flex flex-col gap-1 text-sm text-slate-600">
        <label htmlFor={ids.start}>Start</label>
        <select
          id={ids.start}
          value={start}
          onChange={(e) => setStart(e.target.value as CreateGroupRequest['start'])}
          className={INPUT}
        >
          <option value="earliest">earliest</option>
          <option value="latest">latest</option>
        </select>
      </div>
      <div className="flex gap-2">
        <button type="submit" disabled={create.isPending} className={BUTTON}>
          {create.isPending ? 'Creating…' : 'Create'}
        </button>
        <button type="button" onClick={onDone} className={BUTTON}>
          Cancel
        </button>
      </div>
    </form>
  )
}
