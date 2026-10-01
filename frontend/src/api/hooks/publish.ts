import { useMutation } from '@tanstack/react-query'
import { api, unwrap } from '../client'
import type { components } from '../schema'

export type PublishRequest = components['schemas']['PublishRequest']
export type PublishedView = components['schemas']['PublishedView']
export type BatchResult = components['schemas']['BatchResultView']
export type RowResult = components['schemas']['RowResultView']

/** The server refuses bulk files over this (backend `api/messages.py`). */
export const MAX_UPLOAD_BYTES = 10 * 1024 * 1024

export interface BulkUpload {
  file: File
  /** The topic name, typed by the user in the confirmation dialog. */
  confirm: string
  keyColumn?: string
  valueColumn?: string
}

// `gcTime: 0` on both: the variables are the user's message data (and, for the bulk upload, a
// whole file), which must not linger in the mutation cache once nobody is observing the result.

export const usePublishMessage = (cluster: string, topic: string) =>
  useMutation({
    gcTime: 0,
    mutationFn: (body: PublishRequest) =>
      unwrap(
        api.POST('/api/clusters/{name}/topics/{topic}/messages', {
          params: { path: { name: cluster, topic } },
          body,
        }),
      ),
  })

export const usePublishBulk = (cluster: string, topic: string) =>
  useMutation({
    gcTime: 0,
    mutationFn: ({ file, confirm, keyColumn, valueColumn }: BulkUpload) =>
      unwrap(
        api.POST('/api/clusters/{name}/topics/{topic}/messages/bulk', {
          params: { path: { name: cluster, topic } },
          // The generated type calls the binary file a string; the serializer sends the File.
          body: { file: file as unknown as string, confirm, key_column: keyColumn, value_column: valueColumn },
          bodySerializer: (body) => {
            const form = new FormData()
            // The text fields go first: the server judges `confirm` before it reads the file.
            form.append('confirm', body.confirm)
            if (body.key_column) form.append('key_column', body.key_column)
            if (body.value_column) form.append('value_column', body.value_column)
            form.append('file', body.file as unknown as File)
            return form
          },
        }),
      ),
  })
