import { Link, Route, Routes } from 'react-router'
import { Layout } from './components/Layout'
import { ClusterForm } from './views/ClusterForm'
import { ClusterManager } from './views/ClusterManager'
import { ConsumerGroups } from './views/ConsumerGroups'
import { GroupDetail } from './views/GroupDetail'
import { TopicBrowser } from './views/TopicBrowser'
import { TopicDetail } from './views/TopicDetail'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<ClusterManager />} />
        <Route path="/clusters/new" element={<ClusterForm />} />
        <Route path="/clusters/:name/edit" element={<ClusterForm />} />
        <Route path="/c/:cluster/topics" element={<TopicBrowser />} />
        <Route path="/c/:cluster/topics/:topic" element={<TopicDetail />} />
        <Route path="/c/:cluster/groups" element={<ConsumerGroups />} />
        <Route path="/c/:cluster/groups/:group" element={<GroupDetail />} />
        <Route
          path="*"
          element={
            <p className="text-slate-600">
              Page not found. <Link to="/" className="underline">Back to clusters</Link>
            </p>
          }
        />
      </Route>
    </Routes>
  )
}
