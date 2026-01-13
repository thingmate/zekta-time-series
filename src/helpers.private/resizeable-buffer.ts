export class ResizeableBuffer {
  #buffer: ArrayBuffer;
  #view: DataView<ArrayBuffer>;
  #bytes: Uint8Array<ArrayBuffer>;
  #length: number;

  constructor(
    buffer: ArrayBuffer = new ArrayBuffer(0x100, {
      maxByteLength: 0x100000000,
    }),
    byteOffset?: number,
    byteLength?: number,
  ) {
    this.#buffer = buffer;
    this.#view = new DataView(buffer, byteOffset, byteLength);
    this.#bytes = new Uint8Array(buffer, byteOffset, byteLength);
    this.#length = byteLength ?? 0;
  }

  get buffer(): ArrayBuffer {
    return this.#buffer;
  }

  get view(): DataView<ArrayBuffer> {
    return this.#view;
  }

  get bytes(): Uint8Array<ArrayBuffer> {
    return this.#bytes;
  }

  get length(): number {
    return this.#length;
  }

  resize(newLength: number): void {
    if (this.#buffer.resizable) {
      if (newLength > this.#buffer!.maxByteLength) {
        throw new Error('Size limit reached.');
      }

      if (newLength > this.#buffer.byteLength) {
        this.#buffer.resize(
          Math.min(this.#buffer.maxByteLength, getOptimalBufferLength(newLength)),
        );
      }
    } else {
      if (newLength > this.#buffer.byteLength) {
        const maxByteLength: number = 0x100000000;
        const newBuffer: ArrayBuffer = new ArrayBuffer(
          Math.min(maxByteLength, getOptimalBufferLength(newLength)),
          {
            maxByteLength,
          },
        );
        const currentBytes: Uint8Array = this.#bytes;
        this.#buffer = newBuffer;
        this.#view = new DataView(newBuffer);
        this.#bytes = new Uint8Array(newBuffer);
        this.#bytes.set(currentBytes);
      }
    }

    this.#length = newLength;
  }
}

function getOptimalBufferLength(length: number): number {
  return (
    (1 <<
      Math.ceil(
        Math.log2(length) /* number of bytes to store the data */ +
          0.5 /* adds a "half-byte" of margin */,
      )) /* round to the upper limit */ >>>
    0
  );
}
