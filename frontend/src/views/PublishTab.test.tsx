import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '../components/Toasts'
import { PublishTab } from './PublishTab'

const cluster = (readOnly: boolean) => ({
  name: 'dev',
  env: 'dev',
  region: null,
  bootstrap_servers: 'b:9092',
  read_only: readOnly,
})

type Handler = (request: Request) => Response | Promise<Response>

let fetchMock: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>

function setup({ readOnly = false, publish, bulk }: { readOnly?: boolean; publish?: Handler; bulk?: Handler } = {}) {
  fetchMock = vi.fn(async (request: Request) => {
    const path = new URL(request.url).pathname
    if (path === '/api/clusters/dev') return Response.json(cluster(readOnly))
    if (path.endsWith('/messages/bulk')) {
      return (await bulk?.(request)) ?? Response.json({ succeeded: 0, failed: 0, results: [] })
    }
    if (path.endsWith('/messages')) {
      return (await publish?.(request)) ?? Response.json({ ok: true, partition: 0, offset: 0 })
    }
    return new Response('unexpected', { status: 500 })
  })
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <ToastProvider>
        <PublishTab cluster="dev" topic="orders" />
      </ToastProvider>
    </QueryClientProvider>,
  )
  return userEvent.setup()
}

/**
 * What gets appended to the upload's FormData, in order. jsdom's File cannot be read back out of
 * a Request body (Node's fetch does not recognise it), so the form is observed as it is built.
 */
function spyOnFormData() {
  const appended: [string, string | Blob][] = []
  const append = FormData.prototype.append as (this: FormData, name: string, value: string | Blob) => void
  vi.spyOn(FormData.prototype, 'append').mockImplementation(function (
    this: FormData,
    name: string,
    value: string | Blob,
  ) {
    appended.push([name, value])
    append.call(this, name, value)
  } as never)
  return appended
}

const writes = () => fetchMock.mock.calls.map(([r]) => r).filter((r) => r.method === 'POST')

const csv = new File(['id,body\n1,one\n2,two\n'], 'rows.csv', { type: 'text/csv' })
const json = new File(['  [{"value": "x"}]'], 'rows.json', { type: 'application/json' })
const jsonWithBom = new File([String.fromCharCode(0xfeff) + '\n[{"value": "x"}]'], 'bom.json')

async function loaded() {
  return screen.findByRole('button', { name: 'Publish' })
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('PublishTab on a read-only cluster', () => {
  it('shows a notice and no form', async () => {
    setup({ readOnly: true })

    expect(await screen.findByText(/read-only/i)).toBeInTheDocument()
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Publish' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Upload' })).not.toBeInTheDocument()
  })

  it('shows no form while it is still unknown whether the cluster is read-only', async () => {
    setup({ readOnly: true })

    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
    await screen.findByText(/read-only/i)
  })
})

describe('single publish', () => {
  it('posts the key, value, headers and partition and toasts where it landed', async () => {
    const user = setup({
      publish: () => Response.json({ ok: true, partition: 1, offset: 42 }),
    })
    await loaded()

    await user.type(screen.getByLabelText('Key'), 'order-1')
    await user.type(screen.getByLabelText('Value'), 'hello')
    await user.type(screen.getByLabelText('Headers'), 'trace=t1')
    await user.type(screen.getByLabelText('Partition'), '1')
    await user.click(screen.getByRole('button', { name: 'Publish' }))

    const status = await screen.findByRole('status')
    expect(status).toHaveTextContent('partition 1')
    expect(status).toHaveTextContent('offset 42')
    const [request] = writes()
    expect(new URL(request.url).pathname).toBe('/api/clusters/dev/topics/orders/messages')
    expect(await request.json()).toEqual({
      key: 'order-1',
      value: 'hello',
      headers: 'trace=t1',
      partition: 1,
    })
  })

  it('leaves out the key, headers and partition when they are blank', async () => {
    const user = setup()
    await loaded()

    await user.type(screen.getByLabelText('Value'), 'only a value')
    await user.click(screen.getByRole('button', { name: 'Publish' }))

    await screen.findByRole('status')
    expect(await writes()[0].json()).toEqual({ value: 'only a value' })
  })

  it('shows the header format hint', async () => {
    setup()
    await loaded()

    expect(screen.getByText('JSON object or key=value lines; whitespace around = is ignored')).toBeInTheDocument()
  })

  it('needs a value before it sends anything', async () => {
    const user = setup()
    await loaded()

    await user.click(screen.getByRole('button', { name: 'Publish' }))

    expect(screen.getByText('Value is required')).toBeInTheDocument()
    expect(writes()).toHaveLength(0)
  })

  it('rejects a partition that is not a whole number before sending', async () => {
    const user = setup()
    await loaded()

    await user.type(screen.getByLabelText('Value'), 'v')
    await user.type(screen.getByLabelText('Partition'), '1.5')
    await user.click(screen.getByRole('button', { name: 'Publish' }))

    expect(screen.getByText(/whole number/)).toBeInTheDocument()
    expect(writes()).toHaveLength(0)
  })

  it('shows a field error next to the field it names', async () => {
    const user = setup({
      publish: () =>
        Response.json(
          { code: 'validation_failed', message: 'headers: expected key=value', field: 'headers' },
          { status: 422 },
        ),
    })
    await loaded()

    await user.type(screen.getByLabelText('Value'), 'v')
    await user.type(screen.getByLabelText('Headers'), 'oops')
    await user.click(screen.getByRole('button', { name: 'Publish' }))

    const headers = screen.getByLabelText('Headers')
    await waitFor(() => expect(headers).toHaveAccessibleDescription(/expected key=value/))
    expect(headers).toBeInvalid()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('toasts any other error', async () => {
    const user = setup({
      publish: () => Response.json({ code: 'topic_not_found', message: 'gone' }, { status: 404 }),
    })
    await loaded()

    await user.type(screen.getByLabelText('Value'), 'v')
    await user.click(screen.getByRole('button', { name: 'Publish' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('gone')
  })

  it('disables Publish while the request is in flight', async () => {
    let release: (response: Response) => void = () => {}
    const user = setup({ publish: () => new Promise<Response>((resolve) => (release = resolve)) })
    await loaded()

    await user.type(screen.getByLabelText('Value'), 'v')
    await user.click(screen.getByRole('button', { name: 'Publish' }))

    await waitFor(() => expect(screen.getByRole('button', { name: 'Publishing…' })).toBeDisabled())
    release(Response.json({ ok: true, partition: 0, offset: 1 }))
    await screen.findByRole('status')
    expect(writes()).toHaveLength(1)
  })
})

describe('bulk upload', () => {
  it('cannot upload until a file is chosen', async () => {
    setup()
    await loaded()

    expect(screen.getByRole('button', { name: 'Upload' })).toBeDisabled()
  })

  it('shows the column inputs only for a file that looks like CSV', async () => {
    const user = setup()
    await loaded()
    const input = screen.getByLabelText('CSV or JSON file')

    await user.upload(input, csv)
    expect(await screen.findByLabelText('Key column')).toBeInTheDocument()
    expect(screen.getByLabelText('Value column')).toBeInTheDocument()

    await user.upload(input, json)
    await waitFor(() => expect(screen.queryByLabelText('Key column')).not.toBeInTheDocument())
    expect(screen.queryByLabelText('Value column')).not.toBeInTheDocument()
  })

  it('sees through a byte order mark when deciding whether the file is JSON', async () => {
    const user = setup()
    await loaded()
    const input = screen.getByLabelText('CSV or JSON file')
    await user.upload(input, csv)
    await screen.findByLabelText('Key column')

    await user.upload(input, jsonWithBom)

    await waitFor(() => expect(screen.queryByLabelText('Key column')).not.toBeInTheDocument())
  })

  it('needs the topic typed before it uploads anything', async () => {
    const user = setup()
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)

    await user.click(screen.getByRole('button', { name: 'Upload' }))

    const dialog = await screen.findByRole('dialog')
    expect(within(dialog).getByText('orders')).toBeInTheDocument()
    const confirm = within(dialog).getByRole('button', { name: 'Upload' })
    expect(confirm).toBeDisabled()
    await user.type(within(dialog).getByRole('textbox'), 'order')
    expect(confirm).toBeDisabled()
    expect(writes()).toHaveLength(0)

    await user.type(within(dialog).getByRole('textbox'), 's')
    await user.click(confirm)

    await waitFor(() => expect(writes()).toHaveLength(1))
  })

  it('sends the confirmation, the columns and then the file as multipart', async () => {
    const appended = spyOnFormData()
    const user = setup()
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)
    await user.type(await screen.findByLabelText('Key column'), 'id')
    await user.type(screen.getByLabelText('Value column'), 'body')
    await user.click(screen.getByRole('button', { name: 'Upload' }))
    await user.type(within(await screen.findByRole('dialog')).getByRole('textbox'), 'orders')
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Upload' }))

    await waitFor(() => expect(writes()).toHaveLength(1))
    const request = writes()[0]
    expect(new URL(request.url).pathname).toBe('/api/clusters/dev/topics/orders/messages/bulk')
    expect(request.headers.get('content-type')).toMatch(/^multipart\/form-data; boundary=/)
    // confirm and the columns come before the file so the server can refuse early
    expect(appended.map(([name]) => name)).toEqual(['confirm', 'key_column', 'value_column', 'file'])
    expect(Object.fromEntries(appended.slice(0, 3))).toEqual({
      confirm: 'orders',
      key_column: 'id',
      value_column: 'body',
    })
    expect(appended[3][1]).toBe(csv)
  })

  it('omits blank column inputs', async () => {
    const appended = spyOnFormData()
    const user = setup()
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)
    await screen.findByLabelText('Key column')
    await user.click(screen.getByRole('button', { name: 'Upload' }))
    await user.type(within(await screen.findByRole('dialog')).getByRole('textbox'), 'orders')
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Upload' }))

    await waitFor(() => expect(writes()).toHaveLength(1))
    expect(appended.map(([name]) => name)).toEqual(['confirm', 'file'])
  })

  it('sends nothing when the dialog is cancelled', async () => {
    const user = setup()
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)
    await user.click(screen.getByRole('button', { name: 'Upload' }))

    await user.click(within(await screen.findByRole('dialog')).getByRole('button', { name: 'Cancel' }))

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(writes()).toHaveLength(0)
  })

  it('refuses a file over 10 MB without sending it', async () => {
    const user = setup()
    await loaded()
    const big = new File([new Uint8Array(10 * 1024 * 1024 + 1)], 'big.csv', { type: 'text/csv' })

    await user.upload(screen.getByLabelText('CSV or JSON file'), big)

    expect(await screen.findByText(/larger than 10 MB/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Upload' })).toBeDisabled()
  })

  it('shows the counts and only the failed rows, with their errors', async () => {
    const user = setup({
      bulk: () =>
        Response.json({
          succeeded: 2,
          failed: 2,
          results: [
            { row: 1, ok: true, partition: 0, offset: 5 },
            { row: 2, ok: false, error: 'Expected 2 columns, found 1' },
            { row: 3, ok: true, partition: 1, offset: 9 },
            { row: 4, ok: false, error: 'delivery timeout' },
          ],
        }),
    })
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)
    await user.click(screen.getByRole('button', { name: 'Upload' }))
    await user.type(within(await screen.findByRole('dialog')).getByRole('textbox'), 'orders')
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Upload' }))

    const results = await screen.findByRole('region', { name: 'Upload results' })
    expect(within(results).getByText('2 succeeded')).toBeInTheDocument()
    expect(within(results).getByText('2 failed')).toBeInTheDocument()
    const rows = within(within(results).getByRole('table')).getAllByRole('row').slice(1)
    expect(rows.map((r) => [...r.querySelectorAll('td')].map((c) => c.textContent))).toEqual([
      ['2', 'Expected 2 columns, found 1'],
      ['4', 'delivery timeout'],
    ])
  })

  it('shows no failure table when every row succeeded', async () => {
    const user = setup({
      bulk: () =>
        Response.json({
          succeeded: 1,
          failed: 0,
          results: [{ row: 1, ok: true, partition: 0, offset: 0 }],
        }),
    })
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), json)
    await user.click(screen.getByRole('button', { name: 'Upload' }))
    await user.type(within(await screen.findByRole('dialog')).getByRole('textbox'), 'orders')
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Upload' }))

    const results = await screen.findByRole('region', { name: 'Upload results' })
    expect(within(results).getByText('1 succeeded')).toBeInTheDocument()
    expect(within(results).getByText('0 failed')).toBeInTheDocument()
    expect(within(results).queryByRole('table')).not.toBeInTheDocument()
  })

  it('toasts an API error and shows no results', async () => {
    const user = setup({
      bulk: () =>
        Response.json({ code: 'file_too_large', message: 'The upload is too big' }, { status: 413 }),
    })
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)
    await user.click(screen.getByRole('button', { name: 'Upload' }))
    await user.type(within(await screen.findByRole('dialog')).getByRole('textbox'), 'orders')
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Upload' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('The upload is too big')
    expect(screen.queryByRole('region', { name: 'Upload results' })).not.toBeInTheDocument()
  })

  it('shows a column error next to the column input', async () => {
    const user = setup({
      bulk: () =>
        Response.json(
          { code: 'validation_failed', message: "Column 'nope' is not in the CSV header", field: 'key_column' },
          { status: 422 },
        ),
    })
    await loaded()
    await user.upload(screen.getByLabelText('CSV or JSON file'), csv)
    await user.type(await screen.findByLabelText('Key column'), 'nope')
    await user.click(screen.getByRole('button', { name: 'Upload' }))
    await user.type(within(await screen.findByRole('dialog')).getByRole('textbox'), 'orders')
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Upload' }))

    await waitFor(() =>
      expect(screen.getByLabelText('Key column')).toHaveAccessibleDescription(/not in the CSV header/),
    )
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
})
