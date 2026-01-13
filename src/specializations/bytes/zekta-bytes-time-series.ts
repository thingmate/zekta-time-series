import {
  TimeSeries,
  type TimeSeriesDeleteOptions,
  type TimeSeriesEntry,
  type TimeSeriesSelectOptions,
} from '@thingmate/time-series';
import { uint8ArrayToHex } from '@xstd/hex';
import { Path, type PathInput } from '@xstd/path';
import { sha256 } from '@xstd/sha256';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { handlePromiseAllSettledResult } from '../../helpers.private/handle-promise-all-settled-result.ts';
import { type ZektaTimeBucketFlushOptions } from '../../zekta-time-bucket.ts';
import { type OpenZektaTimeSeriesOptions, ZektaTimeSeries } from '../../zekta-time-series.ts';

/* OPEN */

export interface OpenZektaBytesTimeSeriesOptions extends Omit<
  OpenZektaTimeSeriesOptions,
  'valueByteLength'
> {
  readonly dirPath: PathInput;
}

interface ZektaBytesTimeSeriesOptions {
  readonly timeSeries: ZektaTimeSeries;
  readonly filesPath: Path;
}

/* CLASS */

export class ZektaBytesTimeSeries extends TimeSeries<Uint8Array> {
  static async open({
    dirPath,
    ...options
  }: OpenZektaBytesTimeSeriesOptions): Promise<ZektaBytesTimeSeries> {
    return new ZektaBytesTimeSeries({
      timeSeries: await ZektaTimeSeries.open({
        ...options,
        dirPath,
        valueByteLength: 32,
      }),
      filesPath: Path.of(dirPath).concat('files'),
    });
  }

  readonly #timeSeries: ZektaTimeSeries;
  readonly #filesPath: Path;

  #queue: Promise<any>;

  private constructor({ timeSeries, filesPath }: ZektaBytesTimeSeriesOptions) {
    super();

    this.#timeSeries = timeSeries;
    this.#filesPath = filesPath;

    this.#queue = Promise.resolve();
  }

  #run<GReturn>(task: () => PromiseLike<GReturn> | GReturn): Promise<GReturn> {
    return (this.#queue = this.#queue.then(task, task));
  }

  /* OPERATIONS */

  async #storeFile(data: Uint8Array): Promise<Uint8Array /* id */> {
    const id: Uint8Array = sha256(data);
    const path: Path = this.#filesPath.concat(uint8ArrayToHex(id));
    await mkdir(path.dirname().toString(), {
      recursive: true,
    });
    await writeFile(path.toString(), data);
    return id;
  }

  override push(time: number, value: Uint8Array): Promise<void> {
    return this.#run(async (): Promise<void> => {
      return this.#timeSeries.push(time, await this.#storeFile(value));
    });
  }

  override insert(entries: readonly TimeSeriesEntry<Uint8Array>[]): Promise<void> {
    return this.#run(async (): Promise<void> => {
      return this.#timeSeries.insert(
        handlePromiseAllSettledResult(
          await Promise.allSettled(
            entries.map(({ value }: TimeSeriesEntry<Uint8Array>): Promise<Uint8Array> => {
              return this.#storeFile(value);
            }),
          ),
        ).map((id: Uint8Array, index: number): TimeSeriesEntry<Uint8Array> => {
          return {
            time: entries[index].time,
            value: id,
          };
        }),
      );
    });
  }

  override select(
    options?: TimeSeriesSelectOptions,
  ): Promise<readonly TimeSeriesEntry<Uint8Array>[]> {
    return this.#run(async (): Promise<readonly TimeSeriesEntry<Uint8Array>[]> => {
      return handlePromiseAllSettledResult(
        await Promise.allSettled(
          (await this.#timeSeries.select(options)).map(
            async ({
              time,
              value,
            }: TimeSeriesEntry<Uint8Array>): Promise<TimeSeriesEntry<Uint8Array>> => {
              return {
                time,
                value: await readFile(this.#filesPath.concat(uint8ArrayToHex(value)).toString()),
              };
            },
          ),
        ),
      );
    });
  }

  override delete(options?: TimeSeriesDeleteOptions): Promise<void> {
    return this.#run((): Promise<void> => {
      return this.#timeSeries.delete(options);
    });
  }

  override drop(): Promise<void> {
    return this.#run((): Promise<void> => {
      return this.#timeSeries.drop();
    });
  }

  /* FLUSH */

  override flush(options?: ZektaTimeBucketFlushOptions): Promise<void> {
    return this.#run((): Promise<void> => {
      return this.#timeSeries.flush(options);
    });
  }
}
