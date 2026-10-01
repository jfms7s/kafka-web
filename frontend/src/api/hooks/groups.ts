import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, unwrap } from '../client'
import type { components } from '../schema'
import { clustersKey } from './clusters'

export type GroupSummary = components['schemas']['GroupSummaryView']
export type GroupDetail = components['schemas']['GroupDetailView']
export type GroupMember = components['schemas']['MemberItem']
export type GroupOffset = components['schemas']['OffsetItem']
export type CreateGroupRequest = components['schemas']['CreateGroupRequest']
export type ResetStrategy = components['schemas']['ResetRequest']['strategy']

export const groupsPath = (cluster: string, group?: string) =>
  `/c/${encodeURIComponent(cluster)}/groups${group === undefined ? '' : `/${encodeURIComponent(group)}`}`

export const groupsKey = (cluster: string) => [...clustersKey, cluster, 'groups'] as const

export const useGroups = (cluster: string) =>
  useQuery({
    queryKey: groupsKey(cluster),
    queryFn: async () =>
      (await unwrap(api.GET('/api/clusters/{name}/groups', { params: { path: { name: cluster } } })))
        .groups,
  })

export const useGroup = (cluster: string, group: string) =>
  useQuery({
    queryKey: [...groupsKey(cluster), group],
    queryFn: () =>
      unwrap(
        api.GET('/api/clusters/{name}/groups/{group}', { params: { path: { name: cluster, group } } }),
      ),
  })

/** Group writes refresh the list and the detail however they end: the broker may have changed. */
function useGroupMutation<TVars, TData>(
  cluster: string,
  mutationFn: (vars: TVars) => Promise<TData>,
) {
  const queryClient = useQueryClient()
  return useMutation({
    // The variables carry the typed confirmation; nothing needs them once the call is over.
    gcTime: 0,
    mutationFn,
    onSettled: () => queryClient.invalidateQueries({ queryKey: groupsKey(cluster) }),
  })
}

export const useCreateGroup = (cluster: string) =>
  useGroupMutation(cluster, (body: CreateGroupRequest) =>
    unwrap(api.POST('/api/clusters/{name}/groups', { params: { path: { name: cluster } }, body })),
  )

export interface ResetVars {
  topic: string
  strategy: ResetStrategy
  /** Epoch milliseconds; only for `strategy: 'timestamp'`. */
  timestamp?: number
  /** The group id, typed by the user in the confirmation dialog. */
  confirm: string
}

export const useResetOffsets = (cluster: string, group: string) =>
  useGroupMutation(cluster, (body: ResetVars) =>
    unwrap(
      api.POST('/api/clusters/{name}/groups/{group}/reset', {
        params: { path: { name: cluster, group } },
        body,
      }),
    ),
  )

/** `confirm` is the group id, typed by the user in the confirmation dialog. */
export const useDeleteGroup = (cluster: string, group: string) =>
  useGroupMutation(cluster, (confirm: string) =>
    unwrap(
      api.DELETE('/api/clusters/{name}/groups/{group}', {
        params: { path: { name: cluster, group }, query: { confirm } },
      }),
    ),
  )
