import {
  TimeSeries,
  type TimeSeriesDeleteOptions,
  type TimeSeriesEntry,
  type TimeSeriesSelectOptions,
} from '@thingmate/time-series';
import { type PathInput } from '@xstd/path';
import { type ZektaTimeBucketFlushOptions } from '../../zekta-time-bucket.ts';
import { type OpenZektaTimeSeriesOptions, ZektaTimeSeries } from '../../zekta-time-series.ts';

/* OPEN */

export interface OpenZektaNumberTimeSeriesOptions extends Omit<
  OpenZektaTimeSeriesOptions,
  'valueByteLength'
> {
  readonly dirPath: PathInput;
  readonly type: ZektaNumberTimeSeriesType;
}

export type ZektaNumberTimeSeriesType = 'u8' | 'i8' | 'u16' | 'i16' | 'u32' | 'i32' | 'f32' | 'f64';

/* INTERNAL */

interface ZektaNumberTimeSeriesOptions {
  readonly timeSeries: ZektaTimeSeries;
  readonly numberToUint8Array: NumberToUint8Array;
  readonly uint8ArrayToNumber: Uint8ArrayToNumber;
}

type NumberToUint8Array = (input: number) => Uint8Array;
type Uint8ArrayToNumber = (input: Uint8Array) => number;

/* CLASS */

export class ZektaNumberTimeSeries extends TimeSeries<number> {
  static async open({
    type,
    ...options
  }: OpenZektaNumberTimeSeriesOptions): Promise<ZektaNumberTimeSeries> {
    return new ZektaNumberTimeSeries({
      timeSeries: await ZektaTimeSeries.open({
        ...options,
        valueByteLength: zektaNumberTimeSeriesTypeToByteLength(type),
      }),
      numberToUint8Array: zektaNumberTimeSeriesTypeToNumberToUint8Array(type),
      uint8ArrayToNumber: zektaNumberTimeSeriesTypeToUint8ArrayToNumber(type),
    });
  }

  readonly #timeSeries: ZektaTimeSeries;
  readonly #numberToUint8Array: NumberToUint8Array;
  readonly #uint8ArrayToNumber: Uint8ArrayToNumber;

  private constructor({
    timeSeries,
    numberToUint8Array,
    uint8ArrayToNumber,
  }: ZektaNumberTimeSeriesOptions) {
    super();

    this.#timeSeries = timeSeries;
    this.#numberToUint8Array = numberToUint8Array;
    this.#uint8ArrayToNumber = uint8ArrayToNumber;
  }

  /* OPERATIONS */

  override push(time: number, value: number): Promise<void> {
    return this.#timeSeries.push(time, this.#numberToUint8Array(value));
  }

  override insert(entries: TimeSeriesEntry<number>[]): Promise<void> {
    return this.#timeSeries.insert(
      entries.map(({ time, value }: TimeSeriesEntry<number>): TimeSeriesEntry<Uint8Array> => {
        return {
          time,
          value: this.#numberToUint8Array(value),
        };
      }),
    );
  }

  override async select(
    options?: TimeSeriesSelectOptions,
  ): Promise<readonly TimeSeriesEntry<number>[]> {
    return (await this.#timeSeries.select(options)).map(
      ({ time, value }: TimeSeriesEntry<Uint8Array>): TimeSeriesEntry<number> => {
        return {
          time,
          value: this.#uint8ArrayToNumber(value),
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

/* FUNCTIONS */

function zektaNumberTimeSeriesTypeToByteLength(type: ZektaNumberTimeSeriesType): number {
  switch (type) {
    case 'u8':
    case 'i8':
      return 1;
    case 'u16':
    case 'i16':
      return 2;
    case 'u32':
    case 'i32':
    case 'f32':
      return 4;
    case 'f64':
      return 8;
    default:
      throw new Error(`Unsupported type: ${type}`);
  }
}

function zektaNumberTimeSeriesTypeToNumberToUint8Array(
  type: ZektaNumberTimeSeriesType,
): NumberToUint8Array {
  switch (type) {
    case 'u8':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(1);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setUint8(0, input);
        return output;
      };
    case 'i8':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(1);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setInt8(0, input);
        return output;
      };
    case 'u16':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(2);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setUint16(0, input, true);
        return output;
      };
    case 'i16':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(2);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setInt16(0, input, true);
        return output;
      };
    case 'u32':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(4);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setUint32(0, input, true);
        return output;
      };
    case 'i32':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(4);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setInt32(0, input, true);
        return output;
      };
    case 'f32':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(4);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setFloat32(
          0,
          input,
          true,
        );
        return output;
      };
    case 'f64':
      return (input: number): Uint8Array => {
        const output = new Uint8Array(8);
        new DataView(output.buffer, output.byteOffset, output.byteLength).setFloat64(
          0,
          input,
          true,
        );
        return output;
      };
    default:
      throw new Error(`Unsupported type: ${type}`);
  }
}

function zektaNumberTimeSeriesTypeToUint8ArrayToNumber(
  type: ZektaNumberTimeSeriesType,
): Uint8ArrayToNumber {
  switch (type) {
    case 'u8':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 1);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getUint8(0);
      };
    case 'i8':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 1);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getInt8(0);
      };
    case 'u16':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 2);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getUint16(0, true);
      };
    case 'i16':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 2);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getInt16(0, true);
      };
    case 'u32':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 4);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getUint32(0, true);
      };
    case 'i32':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 4);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getInt32(0, true);
      };
    case 'f32':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 4);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getFloat32(0, true);
      };
    case 'f64':
      return (input: Uint8Array): number => {
        console.assert(input.byteLength === 8);
        return new DataView(input.buffer, input.byteOffset, input.byteLength).getFloat64(0, true);
      };
    default:
      throw new Error(`Unsupported type: ${type}`);
  }
}
