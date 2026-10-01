import { Link, Route, Routes } from 'react-router'
import { Layout } from './components/Layout'
import { ClusterForm } from './views/ClusterForm'
import { ClusterManager } from './views/ClusterManager'

const Placeholder = ({ text }: { text: string }) => <p className="text-slate-600">{text}</p>

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<ClusterManager />} />
        <Route path="/clusters/new" element={<ClusterForm />} />
        <Route path="/clusters/:name/edit" element={<ClusterForm />} />
        <Route path="/c/:cluster/topics" element={<Placeholder text="Topics — coming in Task 5" />} />
        <Route path="/c/:cluster/groups" element={<Placeholder text="Consumer Groups — coming in a later task" />} />
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
