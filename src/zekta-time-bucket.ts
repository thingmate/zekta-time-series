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
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { binarySearch } from './helpers.private/binary-search/binary-search.ts';
import { ResizeableBuffer } from './helpers.private/resizeable-buffer.ts';

/* TYPES */

export interface ZektaTimeBucketFromFilePathOptions extends Omit<
  ZektaTimeBucketOptions,
  'bucketsPath' | 'id'
> {
  readonly filePath: PathInput;
}

export interface ZektaTimeBucketFlushOptions {
  readonly unload?: boolean;
}

interface RequireFlush {
  (): void;
}

/* CLASS */

export interface ZektaTimeBucketOptions {
  readonly bucketsPath: PathInput;
  readonly id: number;
  readonly valueByteLength: number;
}

export class ZektaTimeBucket extends TimeSeries<Uint8Array> {
  /*
  TODO: optimizations:
  - if only push at the end => append to file instead of writing it all
  - if only delete at the end => trunc file instead of writing it all
   */
  static readonly #autoFlushTime: number = 1000;
  static readonly #autoUnloadTime: number = 5000;

  /* TIME RANGE */

  static readonly #timeRange: number = 512;

  static get timeRange(): number {
    return this.#timeRange;
  }

  static getIndexFromFilePath(filePath: PathInput): number {
    const index: number = Number(Path.of(filePath).stemAndExt().stem);

    if (!Number.isSafeInteger(index)) {
      throw new Error(`Invalid index: ${index}`);
    }

    return index;
  }

  static getIdFromTime(time: number): number {
    return Math.floor(time / ZektaTimeBucket.#timeRange);
  }

  static getTimeFromId(id: number): number {
    console.assert(Number.isSafeInteger(id));
    return id * ZektaTimeBucket.#timeRange;
  }

  /* MISC */

  static fromFilePath({
    filePath,
    ...options
  }: ZektaTimeBucketFromFilePathOptions): ZektaTimeBucket {
    return new ZektaTimeBucket({
      ...options,
      bucketsPath: Path.of(filePath).dirname(),
      id: this.getIndexFromFilePath(filePath),
    });
  }

  static readonly sortFnc = (a: ZektaTimeBucket, b: ZektaTimeBucket): number => {
    return a.#id - b.#id;
  };

  readonly #path: Path;

  readonly #id: number;
  readonly #from: number; // computed
  readonly #to: number; // computed

  readonly #timeByteLength: number; // static
  readonly #valueByteLength: number;
  readonly #entryByteLength: number; // computed

  #queue: Promise<any>;

  #data: ResizeableBuffer | undefined;

  #requireFlush: boolean;

  #autoFlushTimer: any;
  #autoUnloadTimer: any;

  constructor({ bucketsPath, id, valueByteLength }: ZektaTimeBucketOptions) {
    super();

    this.#path = Path.of(bucketsPath).concat(`${id}.bucket`);

    this.#id = id;
    this.#from = ZektaTimeBucket.getTimeFromId(this.#id);
    this.#to = ZektaTimeBucket.getTimeFromId(this.#id + 1);

    this.#timeByteLength = 8;
    this.#valueByteLength = valueByteLength;
    this.#entryByteLength = this.#timeByteLength + this.#valueByteLength;

    this.#queue = Promise.resolve();

    this.#requireFlush = false;
  }

  get id(): number {
    return this.#id;
  }

  get from(): number {
    return this.#from;
  }

  get to(): number {
    return this.#to;
  }

  get valueByteLength(): number {
    return this.#valueByteLength;
  }

  isTimeInRange(time: number): boolean {
    return this.#from <= time && time < this.#to;
  }

  throwIfTimeOutOfRange(time: number): void {
    if (!this.isTimeInRange(time)) {
      throw new RangeError(`Time out-of-range: ${time}, expected: [${this.#from}, ${this.#to}[.`);
    }
  }

  #throwIfValueLengthIsInvalid(value: Uint8Array): void {
    if (value.length !== this.#valueByteLength) {
      throw new Error(`Invalid value length: ${value.length}, expected: ${this.#valueByteLength}.`);
    }
  }

  #run<GReturn>(task: () => PromiseLike<GReturn> | GReturn): Promise<GReturn> {
    return (this.#queue = this.#queue.then(task, task));
  }

  /* LOAD/SAVE DATA */

  #startAutoFlushTimer(): void {
    this.#stopAutoFlushTimer();
    this.#autoFlushTimer = setTimeout((): void => {
      this.#autoFlushTimer = undefined;
      this.flush({ unload: false }).catch(reportError);
    }, ZektaTimeBucket.#autoFlushTime);
  }

  #stopAutoFlushTimer(): void {
    if (this.#autoFlushTimer !== undefined) {
      clearTimeout(this.#autoFlushTimer);
      this.#autoFlushTimer = undefined;
    }
  }

  #startAutoUnloadTimer(): void {
    this.#stopAutoUnloadTimer();
    this.#autoUnloadTimer = setTimeout((): void => {
      this.#autoUnloadTimer = undefined;
      this.flush({ unload: true }).catch(reportError);
    }, ZektaTimeBucket.#autoUnloadTime);
  }

  #stopAutoUnloadTimer(): void {
    if (this.#autoUnloadTimer !== undefined) {
      clearTimeout(this.#autoUnloadTimer);
      this.#autoUnloadTimer = undefined;
    }
  }

  async #loadData(): Promise<void> {
    if (this.#data === undefined) {
      try {
        const bytes: Uint8Array<ArrayBuffer> = await readFile(this.#path.toString());
        this.#data = new ResizeableBuffer(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      } catch (error: unknown) {
        if ((error as any).code === 'ENOENT') {
          this.#data = new ResizeableBuffer();
        } else {
          throw error;
        }
      }
    }
  }

  #runDataOperation<GReturn>(
    task: (requireFlush: RequireFlush) => PromiseLike<GReturn> | GReturn,
  ): Promise<GReturn> {
    return this.#run(async (): Promise<GReturn> => {
      this.#stopAutoFlushTimer();
      this.#stopAutoUnloadTimer();

      try {
        await this.#loadData();
        return await task((): void => {
          this.#requireFlush = true;
        });
      } finally {
        this.#startAutoFlushTimer();
        this.#startAutoUnloadTimer();
      }
    });
  }

  /* READ/WRITE DATA */

  #getTime(entryByteOffset: number): number {
    return this.#data!.view.getFloat64(entryByteOffset, true);
  }

  #setTime(entryByteOffset: number, time: number): void {
    this.#data!.view.setFloat64(entryByteOffset, time, true);
  }

  #getValue(entryByteOffset: number): Uint8Array {
    const valueByteOffset: number = entryByteOffset + this.#timeByteLength;
    return this.#data!.bytes.slice(valueByteOffset, valueByteOffset + this.#valueByteLength);
  }

  #setValue(entryByteOffset: number, value: Uint8Array): void {
    console.assert(value.length === this.#valueByteLength);
    this.#data!.bytes.set(value, entryByteOffset + this.#timeByteLength);
  }

  #getInsertionByteOffset(time: number): number {
    console.assert(this.#data !== undefined);

    if (this.#data!.length === 0) {
      return 0;
    } else {
      const lastEntryByteOffset: number = this.#data!.length - this.#entryByteLength;

      if (time >= this.#getTime(lastEntryByteOffset) /* lastTime*/) {
        // insert at the end
        return this.#data!.length;
      } else if (time <= this.#getTime(0) /* firstTime */) {
        // insert at the beginning
        return 0;
      } else {
        return (
          binarySearch(this.#data!.length / this.#entryByteLength, (index: number): number => {
            return this.#getTime(index * this.#entryByteLength) - time;
          }) * this.#entryByteLength
        );
      }
    }
  }

  #getTimeRangeByteOffsets({ from, to /* included */ }: TimeSeriesTimeRange): TimeSeriesTimeRange {
    let fromEntryByteOffset: number = this.#getInsertionByteOffset(from);
    while (
      fromEntryByteOffset >= this.#entryByteLength &&
      from === this.#getTime(fromEntryByteOffset - this.#entryByteLength)
    ) {
      fromEntryByteOffset -= this.#entryByteLength;
    }

    let toEntryByteOffset: number = this.#getInsertionByteOffset(to);

    while (toEntryByteOffset < this.#data!.length && to === this.#getTime(toEntryByteOffset)) {
      toEntryByteOffset += this.#entryByteLength;
    }

    return {
      from: fromEntryByteOffset,
      to: toEntryByteOffset /* excluded */,
    };
  }

  #pushRaw(time: number, value: Uint8Array): number {
    console.assert(this.isTimeInRange(time));
    console.assert(this.#data !== undefined);

    const insertByteOffset: number = this.#getInsertionByteOffset(time);

    this.#data!.resize(this.#data!.length + this.#entryByteLength); // [f64, f64]
    this.#data!.bytes.copyWithin(
      insertByteOffset + this.#entryByteLength,
      insertByteOffset,
      this.#data!.length,
    );
    this.#setTime(insertByteOffset, time);
    this.#setValue(insertByteOffset, value);

    return insertByteOffset;
  }

  /* OPERATIONS */

  override push(time: number, value: Uint8Array): Promise<void> {
    return this.#runDataOperation(async (requireFlush: RequireFlush): Promise<void> => {
      this.throwIfTimeOutOfRange(time);
      this.#throwIfValueLengthIsInvalid(value);
      this.#pushRaw(time, value);
      requireFlush();
    });
  }

  override insert(entries: TimeSeriesEntry<Uint8Array>[]): Promise<void> {
    return this.#runDataOperation((requireFlush: RequireFlush): void => {
      if (entries.length === 0) {
        return;
      }

      entries.sort(sortTimeSeriesEntries);

      for (let i: number = 0; i < entries.length; i += 1) {
        const { time, value } = entries[i];
        this.throwIfTimeOutOfRange(time);
        this.#throwIfValueLengthIsInvalid(value);
        this.#pushRaw(time, value);
      }

      requireFlush();
    });
  }

  override async select(
    options?: TimeSeriesSelectOptions,
  ): Promise<readonly TimeSeriesEntry<Uint8Array>[]> {
    const { asc, from, to } = normalizeTimeSeriesSelectOptions(options);

    if (from >= this.#to || to < this.#from) {
      // out-of-range
      return [];
    }

    return this.#runDataOperation((): readonly TimeSeriesEntry<Uint8Array>[] => {
      const { from: fromEntryByteOffset, to: toEntryByteOffset } = this.#getTimeRangeByteOffsets({
        from,
        to,
      });

      const entries: TimeSeriesEntry<Uint8Array>[] = new Array(
        (toEntryByteOffset - fromEntryByteOffset) / this.#entryByteLength,
      );

      if (asc) {
        for (
          let entryIndex: number = 0, entryByteOffset: number = fromEntryByteOffset;
          entryIndex < entries.length;
          entryIndex += 1, entryByteOffset += this.#entryByteLength
        ) {
          entries[entryIndex] = {
            time: this.#getTime(entryByteOffset),
            value: this.#getValue(entryByteOffset),
          };
        }
      } else {
        for (
          let entryIndex: number = 0,
            entryByteOffset: number = toEntryByteOffset - this.#entryByteLength;
          entryIndex < entries.length;
          entryIndex += 1, entryByteOffset -= this.#entryByteLength
        ) {
          entries[entryIndex] = {
            time: this.#getTime(entryByteOffset),
            value: this.#getValue(entryByteOffset),
          };
        }
      }

      return entries;
    });
  }

  override async delete(options?: TimeSeriesDeleteOptions): Promise<void> {
    const { from, to } = normalizeTimeSeriesDeleteOptions(options);

    if (from >= this.#to || to < this.#from) {
      // out-of-range
      return;
    }

    return this.#runDataOperation((requireFlush: RequireFlush): void => {
      const { from: fromEntryByteOffset, to: toEntryByteOffset } = this.#getTimeRangeByteOffsets({
        from,
        to,
      });

      if (fromEntryByteOffset === toEntryByteOffset) {
        return;
      }

      this.#data!.bytes.copyWithin(fromEntryByteOffset, toEntryByteOffset, this.#data!.length);
      this.#data!.resize(this.#data!.length - (toEntryByteOffset - fromEntryByteOffset));

      requireFlush();
    });
  }

  override drop(): Promise<void> {
    return this.#runDataOperation((requireFlush: RequireFlush): void => {
      if (this.#data!.length === 0) {
        return;
      }

      this.#data!.resize(0);
      requireFlush();
    });
  }

  /* FLUSH */

  override flush({ unload = false }: ZektaTimeBucketFlushOptions = {}): Promise<void> {
    return this.#run(async (): Promise<void> => {
      if (this.#requireFlush) {
        console.assert(this.#data !== undefined);

        await mkdir(this.#path.dirname().toString(), {
          recursive: true,
        });

        if (this.#data!.length === 0) {
          await rm(this.#path.toString(), {
            force: true,
          });
        } else {
          await writeFile(this.#path.toString(), this.#data!.bytes.subarray(0, this.#data!.length));
        }

        this.#requireFlush = false;
      }

      if (unload) {
        this.#data = undefined;
      }
    });
  }
}
