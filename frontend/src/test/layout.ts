import { afterEach, beforeEach, vi } from 'vitest'

/**
 * jsdom does no layout, so a virtualized list sees a 0px viewport and renders nothing. While
 * active, the element marked `data-testid="message-scroll"` is 600px tall and everything else
 * (the rows, as measured by the virtualizer) is `ROW_HEIGHT` px.
 */
const ROW_HEIGHT = 36

export function mockVirtualLayout() {
  beforeEach(() => {
    vi.spyOn(HTMLElement.prototype, 'offsetHeight', 'get').mockImplementation(function (
      this: HTMLElement,
    ) {
      return this.dataset.testid === 'message-scroll' ? 600 : ROW_HEIGHT
    })
    vi.spyOn(HTMLElement.prototype, 'offsetWidth', 'get').mockReturnValue(1000)
  })
  afterEach(() => vi.restoreAllMocks())
}
