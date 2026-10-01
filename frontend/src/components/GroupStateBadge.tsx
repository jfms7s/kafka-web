const TONES: Record<string, string> = {
  stable: 'bg-green-100 text-green-800',
  empty: 'bg-slate-200 text-slate-700',
  dead: 'bg-red-100 text-red-800',
}

export function GroupStateBadge({ state }: { state: string }) {
  const tone = TONES[state] ?? 'bg-amber-100 text-amber-900'
  return <span className={`rounded-full px-2 py-0.5 text-xs ${tone}`}>{state}</span>
}
