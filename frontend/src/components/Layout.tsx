import { NavLink, Outlet, useMatch } from 'react-router'
import { Header } from './Header'

const NAV_LINK = ({ isActive }: { isActive: boolean }) =>
  `block rounded px-3 py-1.5 text-sm ${isActive ? 'bg-slate-200 font-medium text-slate-900' : 'text-slate-600 hover:bg-slate-100'}`

export function Layout() {
  const cluster = useMatch('/c/:cluster/*')?.params.cluster
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <Header />
      <div className="mx-auto flex max-w-7xl gap-6 px-4 py-6">
        {cluster && (
          <nav aria-label="Cluster" className="w-44 shrink-0 space-y-1">
            <p className="px-3 pb-1 font-mono text-xs text-slate-500">{cluster}</p>
            <NavLink to={`/c/${cluster}/topics`} className={NAV_LINK}>
              Topics
            </NavLink>
            <NavLink to={`/c/${cluster}/groups`} className={NAV_LINK}>
              Consumer Groups
            </NavLink>
          </nav>
        )}
        <main className="min-w-0 flex-1">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
