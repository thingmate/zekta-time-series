import {
  normalizeTimeSeriesDeleteOptions,
  normalizeTimeSeriesSelectOptions,
  sortTimeSeriesEntries,
  TimeSeries,
  type TimeSeriesDeleteOptions,
  type TimeSeriesEntry,
  type TimeSeriesSelectOptions,
  type TimeSeriesTimeRange,
} from '@thingmate/time-series';
import { Path, type PathInput } from '@xstd/path';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { binarySearch } from './helpers.private/binary-search/binary-search.ts';
import { handlePromiseAllSettledResult } from './helpers.private/handle-promise-all-settled-result.ts';
import { ZektaTimeBucket, type ZektaTimeBucketFlushOptions } from './zekta-time-bucket.ts';

/* CONFIG */

interface ZektaTimeSeriesConfig<GVersion extends number> {
  readonly version: GVersion;
}

interface ZektaTimeSeriesConfigV1 extends ZektaTimeSeriesConfig<1> {
  readonly valueByteLength: number;
}

/* OPEN */

export interface OpenZektaTimeSeriesOptions
  extends
    Partial<Pick<ZektaTimeSeriesConfigV1, 'valueByteLength'>>,
    Omit<ZektaTimeSeriesConfigV1, 'version' | 'valueByteLength'> {
  readonly dirPath: PathInput;
  readonly create?: boolean;
}

interface ZektaTimeSeriesOptions {
  readonly dirPath: PathInput;
  readonly valueByteLength: number;
  readonly bucketsPath: Path;
  readonly buckets: ZektaTimeBucket[];
}

/* CLASS */

export class ZektaTimeSeries extends TimeSeries<Uint8Array> {
  static async open({
    dirPath,
    valueByteLength,
    create = true,
  }: OpenZektaTimeSeriesOptions): Promise<ZektaTimeSeries> {
    dirPath = Path.of(dirPath);

    // 1) load config file
    const configPath: Path = dirPath.concat('zekta.config.json');
    let config: ZektaTimeSeriesConfigV1;

    try {
      // TODO validate config file format/schema
      config = JSON.parse(await readFile(configPath.toString(), { encoding: 'utf8' }));

      if (config.version !== 1) {
        throw new Error(`Unsupported version: ${config.version}`);
      }

      if (valueByteLength === undefined) {
        valueByteLength = config.valueByteLength;
      } else if (config.valueByteLength !== valueByteLength) {
        throw new Error(
          `Incompatible valueByteLength: ${config.valueByteLength} !== ${valueByteLength}`,
        );
      }
    } catch (error: unknown) {
      if ((error as any).code === 'ENOENT' && create) {
        if (valueByteLength === undefined) {
          throw new Error('Missing valueByteLength, while trying to create a new time series.');
        }

        config = {
          version: 1,
          valueByteLength,
        };
        await mkdir(configPath.dirname().toString(), { recursive: true });
        await writeFile(configPath.toString(), JSON.stringify(config), { encoding: 'utf8' });
      } else {
        throw error;
      }
    }

    // 2) load buckets
    const bucketsPath: Path = dirPath.concat('buckets');
    let buckets: ZektaTimeBucket[];
    try {
      buckets = (await readdir(bucketsPath.toString()))
        .map((fileName: string): ZektaTimeBucket => {
          return ZektaTimeBucket.fromFilePath({
            filePath: bucketsPath.concat(fileName),
            valueByteLength,
          });
        })
        .sort(ZektaTimeBucket.sortFnc);
    } catch (error: unknown) {
      if ((error as any).code === 'ENOENT') {
        buckets = [];
      } else {
        throw error;
      }
    }

    return new ZektaTimeSeries({
      ...config,
      dirPath,
      bucketsPath,
      buckets,
    });
  }

  readonly #bucketsPath: Path;
  readonly #valueByteLength: number;
  readonly #buckets: ZektaTimeBucket[]; // sorted list of buckets

  #queue: Promise<any>;

  private constructor({ bucketsPath, valueByteLength, buckets }: ZektaTimeSeriesOptions) {
    super();

    this.#bucketsPath = bucketsPath;
    this.#valueByteLength = valueByteLength;
    this.#buckets = buckets;

    this.#queue = Promise.resolve();
  }

  #run<GReturn>(task: () => PromiseLike<GReturn> | GReturn): Promise<GReturn> {
    return (this.#queue = this.#queue.then(task, task));
  }

  #getBucket(time: number): ZektaTimeBucket {
    const bucketId: number = ZektaTimeBucket.getIdFromTime(time);

    const insertIndexInArray: number = binarySearch(
      this.#buckets.length,
      (indexInArray: number): number => {
        return this.#buckets[indexInArray].id - bucketId;
      },
    );

    if (
      insertIndexInArray < this.#buckets.length &&
      this.#buckets[insertIndexInArray].id === bucketId
    ) {
      return this.#buckets[insertIndexInArray];
    } else {
      const bucket: ZektaTimeBucket = new ZektaTimeBucket({
        bucketsPath: this.#bucketsPath,
        id: bucketId,
        valueByteLength: this.#valueByteLength,
      });
      this.#buckets.splice(insertIndexInArray, 0, bucket);
      return bucket;
    }
  }

  #getTimeRangeBucketIndexesInArray({
    from,
    to /* included */,
  }: TimeSeriesTimeRange): TimeSeriesTimeRange {
    const fromBucketIndex: number = ZektaTimeBucket.getIdFromTime(from);
    const toBucketIndex: number = ZektaTimeBucket.getIdFromTime(to);

    return {
      from: binarySearch(this.#buckets.length, (indexInArray: number): number => {
        return this.#buckets[indexInArray].id - fromBucketIndex;
      }),
      to: Math.min(
        this.#buckets.length,
        binarySearch(this.#buckets.length, (indexInArray: number): number => {
          return this.#buckets[indexInArray].id - toBucketIndex;
        }) + 1,
      ) /* excluded */,
    };
  }

  /* OPERATIONS */

  override push(time: number, value: Uint8Array): Promise<void> {
    return this.#run((): Promise<void> => {
      return this.#getBucket(time).push(time, value);
    });
  }

  override insert(entries: TimeSeriesEntry<Uint8Array>[]): Promise<void> {
    return this.#run((): Promise<void> | void => {
      if (entries.length === 0) {
        return;
      }

      return concurrentPromises(
        entries
          .sort(sortTimeSeriesEntries)
          .map(({ time, value }: TimeSeriesEntry<Uint8Array>): Promise<void> => {
            return this.#getBucket(time).push(time, value);
          }),
      );
    });
  }

  override select(
    options?: TimeSeriesSelectOptions,
  ): Promise<readonly TimeSeriesEntry<Uint8Array>[]> {
    return this.#run(async (): Promise<readonly TimeSeriesEntry<Uint8Array>[]> => {
      const { asc, from, to } = normalizeTimeSeriesSelectOptions(options);

      const { from: fromIndexInArray, to: toIndexInArray } = this.#getTimeRangeBucketIndexesInArray(
        { from, to },
      );

      const promises: Promise<readonly TimeSeriesEntry<Uint8Array>[]>[] = [];

      if (asc) {
        for (let i: number = fromIndexInArray; i < toIndexInArray; i++) {
          promises.push(this.#buckets[i].select({ asc, from, to }));
        }
      } else {
        for (let i: number = toIndexInArray - 1; i >= fromIndexInArray; i--) {
          promises.push(this.#buckets[i].select({ asc, from, to }));
        }
      }

      return handlePromiseAllSettledResult(await Promise.allSettled(promises)).flat();
    });
  }

  override delete(options?: TimeSeriesDeleteOptions): Promise<void> {
    return this.#run((): Promise<void> => {
      const { from, to } = normalizeTimeSeriesDeleteOptions(options);

      const { from: fromIndexInArray, to: toIndexInArray } = this.#getTimeRangeBucketIndexesInArray(
        { from, to },
      );

      const promises: Promise<void>[] = [];

      for (let i: number = fromIndexInArray; i < toIndexInArray; i++) {
        promises.push(this.#buckets[i].delete({ from, to }));
      }

      return concurrentPromises(promises);
    });
  }

  override drop(): Promise<void> {
    return this.#run(async (): Promise<void> => {
      await concurrentPromises(
        this.#buckets.map((bucket: ZektaTimeBucket): Promise<void> => {
          return bucket.drop();
        }),
      );
    });
  }

  /* FLUSH */

  override flush(options?: ZektaTimeBucketFlushOptions): Promise<void> {
    return this.#run((): Promise<void> => {
      // TODO: remove from "buckets" the empty buckets
      return concurrentPromises(
        this.#buckets.map((bucket: ZektaTimeBucket): Promise<void> => {
          return bucket.flush(options);
        }),
      );
    });
  }
}

/* FUNCTIONS */

// async function concurrently<GArguments extends unknown[]>(
//   promiseFactories: Iterable<(...args: GArguments) => PromiseLike<any> | any>,
//   ...args: GArguments
// ): Promise<void> {
//   return concurrentPromises(
//     Array.from(promiseFactories, (factory: (...args: GArguments) => PromiseLike<any> | any) => {
//       return Promise.try(factory, ...args);
//     }),
//   );
// }

async function concurrentPromises(promises: Iterable<PromiseLike<any>>): Promise<void> {
  handlePromiseAllSettledResult(await Promise.allSettled(promises));
}
