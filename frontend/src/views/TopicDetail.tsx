import type { ComponentType } from 'react'
import { Link, useParams, useSearchParams } from 'react-router'
import { topicPath } from '../api/hooks/topics'
import { Tabs } from '../components/Tabs'
import { ConfigTab } from './ConfigTab'
import { LiveTab } from './LiveTab'
import { MessagesTab } from './MessagesTab'
import { PublishTab } from './PublishTab'

interface TabProps {
  cluster: string
  topic: string
}

/**
 * The topic tab registry: order here is the order on screen and the first entry is the default.
 */
const TABS: readonly { id: string; label: string; Panel: ComponentType<TabProps> }[] = [
  { id: 'messages', label: 'Messages', Panel: MessagesTab },
  { id: 'live', label: 'Live', Panel: LiveTab },
  { id: 'config', label: 'Config', Panel: ConfigTab },
  { id: 'publish', label: 'Publish', Panel: PublishTab },
]

export function TopicDetail() {
  const { cluster = '', topic = '' } = useParams()
  const [params, setParams] = useSearchParams()

  const active = TABS.find((t) => t.id === params.get('tab')) ?? TABS[0]
  const { Panel } = active

  return (
    <div>
      <p className="text-sm">
        <Link to={topicPath(cluster)} className="text-slate-600 underline-offset-2 hover:underline">
          ← Topics
        </Link>
      </p>
      <h1 className="mt-2 mb-4 font-mono text-2xl font-semibold break-all text-slate-900">{topic}</h1>
      <Tabs
        label="Topic"
        tabs={TABS}
        active={active.id}
        onChange={(id) => setParams({ tab: id }, { replace: true })}
      >
        <Panel cluster={cluster} topic={topic} />
      </Tabs>
    </div>
  )
}
