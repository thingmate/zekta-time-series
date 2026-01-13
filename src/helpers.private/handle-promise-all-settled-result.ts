export type HandlePromiseAllSettledResultOutputValue<
  GResults extends readonly PromiseSettledResult<unknown>[],
> = {
  -readonly [GKey in keyof GResults]: GResults[GKey] extends PromiseSettledResult<infer GValue>
    ? GValue
    : never;
};

export function handlePromiseAllSettledResult<
  GResults extends readonly PromiseSettledResult<unknown>[],
>(results: GResults): HandlePromiseAllSettledResultOutputValue<GResults> {
  const errors: readonly PromiseRejectedResult[] = results.filter<PromiseRejectedResult>(
    (result: PromiseSettledResult<unknown>): result is PromiseRejectedResult => {
      return result.status === 'rejected';
    },
  );

  if (errors.length === 0) {
    return results.map((result: PromiseSettledResult<unknown>): unknown => {
      return (result as PromiseFulfilledResult<unknown>).value;
    }) as HandlePromiseAllSettledResultOutputValue<GResults>;
  }
  if (errors.length === 1) {
    throw errors[0].reason;
  } else {
    throw new AggregateError(
      errors.map((error: PromiseRejectedResult): unknown => {
        return error.reason;
      }),
    );
  }
}
