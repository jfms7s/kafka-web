const COLOURS: Record<string, string> = {
  prd: 'bg-red-600 text-white',
  stg: 'bg-amber-400 text-amber-950',
  qa: 'bg-sky-500 text-white',
}

export function EnvBadge({ env, readOnly = false }: { env: string; readOnly?: boolean }) {
  const colour = COLOURS[env] ?? 'bg-slate-200 text-slate-800'
  return (
    <span className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${colour}`}>
      {readOnly ? `${env} 🔒` : env}
    </span>
  )
}
