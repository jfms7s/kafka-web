import { useQuery } from '@tanstack/react-query'
import { api, unwrap } from '../client'
import type { components } from '../schema'
import { clustersKey } from './clusters'

export type TopicSummary = components['schemas']['TopicSummaryView']
export type TopicConfig = components['schemas']['TopicConfigView']
export type ConfigEntry = components['schemas']['ConfigEntryItem']
export type PartitionDetail = components['schemas']['PartitionView']

/** Topic names are `[a-zA-Z0-9._-]`, but encode anyway: the name goes into a URL path segment. */
export const topicPath = (cluster: string, topic?: string) =>
  `/c/${encodeURIComponent(cluster)}/topics${topic === undefined ? '' : `/${encodeURIComponent(topic)}`}`

export const topicsKey = (cluster: string) => [...clustersKey, cluster, 'topics'] as const

export const useTopics = (cluster: string) =>
  useQuery({
    queryKey: topicsKey(cluster),
    queryFn: async () =>
      (await unwrap(api.GET('/api/clusters/{name}/topics', { params: { path: { name: cluster } } })))
        .topics,
  })

export const useTopicConfig = (cluster: string, topic: string) =>
  useQuery({
    queryKey: [...topicsKey(cluster), topic, 'config'],
    queryFn: () =>
      unwrap(
        api.GET('/api/clusters/{name}/topics/{topic}/config', {
          params: { path: { name: cluster, topic } },
        }),
      ),
  })
