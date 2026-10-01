import { describe, expect, it } from 'vitest'
import { pushCapped } from './ringBuffer'

const range = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => from + i)

describe('pushCapped', () => {
  it('appends in order while under the cap', () => {
    expect(pushCapped([1, 2], [3, 4], 10)).toEqual([1, 2, 3, 4])
  })

  it('keeps the last 5,000 by default, oldest dropped first', () => {
    const result = pushCapped(range(0, 4990), range(4990, 5010))

    expect(result).toHaveLength(5000)
    expect(result[0]).toBe(10)
    expect(result.at(-1)).toBe(5009)
    expect(result).toEqual(range(10, 5010))
  })

  it('keeps only the tail of a batch larger than the cap', () => {
    expect(pushCapped([1, 2, 3], range(10, 20), 4)).toEqual([16, 17, 18, 19])
  })

  it('returns a new array and leaves the inputs untouched', () => {
    const buf = [1, 2, 3]
    const items = [4, 5]

    const result = pushCapped(buf, items, 4)

    expect(result).not.toBe(buf)
    expect(buf).toEqual([1, 2, 3])
    expect(items).toEqual([4, 5])
    expect(result).toEqual([2, 3, 4, 5])
  })

  it('handles empty input', () => {
    expect(pushCapped([], [], 3)).toEqual([])
    expect(pushCapped([1], [], 3)).toEqual([1])
  })
})
