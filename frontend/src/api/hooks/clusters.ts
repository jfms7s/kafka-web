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

export const useCluster = (name: string, enabled = true) =>
  useQuery({
    enabled,
    queryKey: [...clustersKey, name],
    queryFn: () => unwrap(api.GET('/api/clusters/{name}', { params: { path: { name } } })),
  })

export const useStatus = () =>
  useQuery({ queryKey: statusKey, queryFn: () => unwrap(api.GET('/api/status')) })

/** A mutation that refreshes the cluster list and connection status however it ends. */
function useClusterMutation<TVars, TData>(mutationFn: (vars: TVars) => Promise<TData>) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn,
    onSettled: async () => {
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

export const useTestCluster = () =>
  useClusterMutation(({ input, existing }: { input: ClusterInput; existing?: string }) =>
    unwrap(api.POST('/api/clusters/test', { body: input, params: { query: { existing } } })),
  )

export const useConnect = () =>
  useClusterMutation((name: string) =>
    unwrap(api.POST('/api/clusters/{name}/connect', { params: { path: { name } } })),
  )

export const useDisconnect = () =>
  useClusterMutation((name: string) =>
    unwrap(api.POST('/api/clusters/{name}/disconnect', { params: { path: { name } } })),
  )
