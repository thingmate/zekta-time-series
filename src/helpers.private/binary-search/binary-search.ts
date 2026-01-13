/**
 * Performs a binary search over an abstract _sorted_ `collection` of data and finds the index where a `value` may be _inserted_ while preserving the order.
 *
 * @param {number} length - The number of elements in the `collection`.
 * @param {(index: number) => number} compare - A comparator function that is called with the current _insert_ index.
 *    It must compare the value at the current index with the value to be inserted => `(index: number): number => collection[index] - value` (assuming `collection[index]` and `value` are numbers).
 * @returns {number} - The index where `value` may be safely inserted while preserving the order of the `collection`. Range: `[0, length]`.
 *
 * @example
 *
 * ```ts
 * const values: number[] = [1, 2, 3, 4, 5];
 * const valueToInsert: number = 1.5;
 * values.splice(
 *   binarySearch(values.length, (index: number): number => values[index] - valueToInsert),
 *   0,
 *   valueToInsert,
 * );
 * // values => [1, 1.5, 2, 3, 4, 5]
 * ```
 */
export function binarySearch(length: number, compare: (index: number) => number): number {
  let low: number = 0;
  let high: number = length - 1;

  while (low <= high) {
    const mid: number = Math.floor((low + high) / 2);

    const result: number = compare(mid);

    if (result === 0) {
      low = mid;
      break;
    } else if (result < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return low;
}
