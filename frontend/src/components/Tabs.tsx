import type { ReactNode } from 'react'

export interface TabDef<Id extends string> {
  id: Id
  label: string
}

interface TabsProps<Id extends string> {
  tabs: readonly TabDef<Id>[]
  active: Id
  onChange: (id: Id) => void
  label: string
  children: ReactNode
}

const tabId = (id: string) => `tab-${id}`
const panelId = (id: string) => `tabpanel-${id}`

/** An accessible tab strip; `children` is the panel of the `active` tab. */
export function Tabs<Id extends string>({ tabs, active, onChange, label, children }: TabsProps<Id>) {
  return (
    <>
      <div role="tablist" aria-label={label} className="flex gap-1 border-b border-slate-200">
        {tabs.map((tab) => {
          const selected = tab.id === active
          return (
            <button
              key={tab.id}
              id={tabId(tab.id)}
              type="button"
              role="tab"
              aria-selected={selected}
              aria-controls={panelId(tab.id)}
              onClick={() => onChange(tab.id)}
              className={`-mb-px border-b-2 px-4 py-2 text-sm ${
                selected
                  ? 'border-slate-900 font-medium text-slate-900'
                  : 'border-transparent text-slate-600 hover:text-slate-900'
              }`}
            >
              {tab.label}
            </button>
          )
        })}
      </div>
      <div id={panelId(active)} role="tabpanel" aria-labelledby={tabId(active)} className="pt-6">
        {children}
      </div>
    </>
  )
}
