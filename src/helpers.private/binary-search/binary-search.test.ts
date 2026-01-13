import { describe, expect, it } from 'vitest';
import { binarySearch } from './binary-search.ts';

describe('binarySearch', () => {
  it('should insert value at correct position while preserving the order', () => {
    const values: number[] = [1, 2, 3, 4, 5];
    const valueToInsert: number = 1.5;
    values.splice(
      binarySearch(values.length, (index: number): number => values[index] - valueToInsert),
      0,
      valueToInsert,
    );
    expect(values).toEqual([1, 1.5, 2, 3, 4, 5]);
  });
});
