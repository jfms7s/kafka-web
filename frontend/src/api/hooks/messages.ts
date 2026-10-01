import { useMutation } from '@tanstack/react-query'
import { api, unwrap } from '../client'
import type { components, paths } from '../schema'

export type MessageView = components['schemas']['MessageView']
export type Decoded = components['schemas']['Decoded']
export type HeaderView = components['schemas']['HeaderView']
export type SnapshotQuery = NonNullable<
  paths['/api/clusters/{name}/topics/{topic}/messages']['get']['parameters']['query']
>

/**
 * A one-shot snapshot read. A mutation rather than a query: it is run on demand, the result is
 * only meaningful for the form that asked for it, and it must not be refetched in the background.
 */
export const useSnapshot = (cluster: string, topic: string) =>
  useMutation({
    mutationFn: async (query: SnapshotQuery) =>
      (
        await unwrap(
          api.GET('/api/clusters/{name}/topics/{topic}/messages', {
            params: { path: { name: cluster, topic }, query },
          }),
        )
      ).messages,
  })
