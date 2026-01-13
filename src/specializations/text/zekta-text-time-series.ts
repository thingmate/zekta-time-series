import {
  TimeSeries,
  type TimeSeriesDeleteOptions,
  type TimeSeriesEntry,
  type TimeSeriesSelectOptions,
} from '@thingmate/time-series';
import { type ZektaTimeBucketFlushOptions } from '../../zekta-time-bucket.ts';
import {
  type OpenZektaBytesTimeSeriesOptions,
  ZektaBytesTimeSeries,
} from '../bytes/zekta-bytes-time-series.ts';

/* OPEN */

export interface OpenZektaTextTimeSeriesOptions extends OpenZektaBytesTimeSeriesOptions {}

interface ZektaTextTimeSeriesOptions {
  readonly timeSeries: ZektaBytesTimeSeries;
}

/* CLASS */

export class ZektaTextTimeSeries extends TimeSeries<string> {
  static async open(options: OpenZektaTextTimeSeriesOptions): Promise<ZektaTextTimeSeries> {
    return new ZektaTextTimeSeries({
      timeSeries: await ZektaBytesTimeSeries.open(options),
    });
  }

  readonly #timeSeries: ZektaBytesTimeSeries;
  readonly #encoder: TextEncoder;
  readonly #decoder: TextDecoder;

  private constructor({ timeSeries }: ZektaTextTimeSeriesOptions) {
    super();

    this.#timeSeries = timeSeries;
    this.#encoder = new TextEncoder();
    this.#decoder = new TextDecoder();
  }

  /* OPERATIONS */

  override push(time: number, value: string): Promise<void> {
    return this.#timeSeries.push(time, this.#encoder.encode(value));
  }

  override insert(entries: TimeSeriesEntry<string>[]): Promise<void> {
    return this.#timeSeries.insert(
      entries.map(({ time, value }: TimeSeriesEntry<string>): TimeSeriesEntry<Uint8Array> => {
        return {
          time,
          value: this.#encoder.encode(value),
        };
      }),
    );
  }

  override async select(
    options?: TimeSeriesSelectOptions,
  ): Promise<readonly TimeSeriesEntry<string>[]> {
    return (await this.#timeSeries.select(options)).map(
      ({ time, value }: TimeSeriesEntry<Uint8Array>): TimeSeriesEntry<string> => {
        return {
          time,
          value: this.#decoder.decode(value),
        };
      },
    );
  }

  override delete(options?: TimeSeriesDeleteOptions): Promise<void> {
    return this.#timeSeries.delete(options);
  }

  override drop(): Promise<void> {
    return this.#timeSeries.drop();
  }

  /* FLUSH */

  override flush(options?: ZektaTimeBucketFlushOptions): Promise<void> {
    return this.#timeSeries.flush(options);
  }
}
