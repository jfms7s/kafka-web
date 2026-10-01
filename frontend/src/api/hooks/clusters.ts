import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, unwrap } from '../client'
import type { components } from '../schema'

export type ClusterView = components['schemas']['ClusterView']
export type ClusterInput = components['schemas']['ClusterInput']
export type ConnectionView = components['schemas']['ConnectionView']

export const clustersKey = ['clusters'] as const
export const statusKey = ['status'] as const

export const useClusters = () =>
  useQuery({ queryKey: clustersKey, queryFn: () => unwrap(api.GET('/api/clusters')) })

export const useCluster = (
  name: string,
  enabled = true,
  options: { refetchOnMount?: 'always' } = {},
) =>
  useQuery({
    enabled,
    ...options,
    queryKey: [...clustersKey, name],
    queryFn: () => unwrap(api.GET('/api/clusters/{name}', { params: { path: { name } } })),
  })

export const useStatus = () =>
  useQuery({ queryKey: statusKey, queryFn: () => unwrap(api.GET('/api/status')) })

/**
 * A mutation that refreshes the cluster list and connection status however it ends (unless
 * `invalidates` is off).
 *
 * `gcTime: 0` drops the mutation from the cache as soon as nothing observes it: its variables
 * carry the SASL password and truststore, which must not linger in memory.
 */
function useClusterMutation<TVars, TData>(
  mutationFn: (vars: TVars) => Promise<TData>,
  { invalidates = true }: { invalidates?: boolean } = {},
) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn,
    gcTime: 0,
    onSettled: async () => {
      if (!invalidates) return
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: clustersKey }),
        queryClient.invalidateQueries({ queryKey: statusKey }),
      ])
    },
  })
}

export const useCreateCluster = () =>
  useClusterMutation((body: ClusterInput) => unwrap(api.POST('/api/clusters', { body })))

export const useUpdateCluster = (name: string) =>
  useClusterMutation((body: ClusterInput) =>
    unwrap(api.PUT('/api/clusters/{name}', { params: { path: { name } }, body })),
  )

export const useDeleteCluster = () =>
  useClusterMutation((name: string) =>
    unwrap(api.DELETE('/api/clusters/{name}', { params: { path: { name } } })),
  )

/** A connectivity probe: it changes no server state, so there is nothing to refresh. */
export const useTestCluster = () =>
  useClusterMutation(
    ({ input, existing }: { input: ClusterInput; existing?: string }) =>
      unwrap(api.POST('/api/clusters/test', { body: input, params: { query: { existing } } })),
    { invalidates: false },
  )

export const useConnect = () =>
  useClusterMutation((name: string) =>
    unwrap(api.POST('/api/clusters/{name}/connect', { params: { path: { name } } })),
  )

export const useDisconnect = () =>
  useClusterMutation((name: string) =>
    unwrap(api.POST('/api/clusters/{name}/disconnect', { params: { path: { name } } })),
  )
