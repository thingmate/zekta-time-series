import { Path } from '@xstd/path';
import { rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { ZektaBytesTimeSeries } from '../../src/built-in/zekta-time-series/specializations/bytes/zekta-bytes-time-series.ts';
import { ZektaNumberTimeSeries } from '../../src/built-in/zekta-time-series/specializations/number/zekta-number-time-series.ts';
import { ZektaTimeBucket } from '../../src/built-in/zekta-time-series/zekta-time-bucket.ts';
import { ZektaTimeSeries } from '../../src/built-in/zekta-time-series/zekta-time-series.ts';

const ROOT_PATH = new Path(fileURLToPath(import.meta.url)).dirname().concat('../..');
const DB_PATH = ROOT_PATH.concat('db/zekta');

async function debugZekta_01() {
  const v = (input: number): Uint8Array => {
    return new Uint8Array([input]);
  };

  await rm(DB_PATH.toString(), { force: true, recursive: true });

  await using bucket = new ZektaTimeBucket({
    bucketsPath: DB_PATH.concat('buckets'),
    id: 0,
    valueByteLength: 1,
  });

  await bucket.push(10, v(1));
  await bucket.push(1, v(2));
  await bucket.push(2, v(3));
  await bucket.push(4, v(4));

  await bucket.delete({ from: 4, to: 4 });

  // await bucket.push(5, 5);

  // console.log(await bucket.select({ from: 1, to: 4 }));
  console.log(await bucket.select());
  await bucket.flush();
}

async function debugZekta_02() {
  await rm(DB_PATH.toString(), { force: true, recursive: true });

  // const from: number = Date.now() / 1000;
  const from: number = 0;
  const to: number = from + 60 * 1000;

  await using series = await ZektaNumberTimeSeries.open({
    dirPath: DB_PATH,
    type: 'u16',
  });

  await series.push(from + 10, 1);
  await series.push(from + 1, 2);
  await series.push(from + 2, 3);
  await series.push(4, 4);
  await series.push(from + 600, 4);
  await series.push(from + 601, 5);

  // await series.delete({ from: -4, to: 4000 });

  await series.flush();
  console.log(await series.select({ from, to, asc: true }));
  // console.log(await series.select({ from: 512, to: 700, asc: false }));
  // console.log(await series.select());
}

async function debugZekta_03_00() {
  await rm(DB_PATH.toString(), { force: true, recursive: true });

  const valueByteLength: number = 8;

  await using series = await ZektaTimeSeries.open({
    dirPath: DB_PATH,
    valueByteLength,
  });

  const value = new Uint8Array(valueByteLength);
  console.time('push');
  for (let i = 0; i < 1e5; i++) {
    series.push(i, value).catch(console.error);
  }
  await series.flush({ unload: false });
  console.timeEnd('push');

  // console.time('insert');
  // series.insert(Array.from({ length: 1e4 }, (_, i) => ({ time: i, value: i })));
  // await series.flush();
  // console.timeEnd('insert');

  console.time('select');
  const entries = await series.select({ from: 0, to: 1e6, asc: true });
  console.timeEnd('select');

  console.log(entries);
}

async function debugZekta_03_01() {
  await rm(DB_PATH.toString(), { force: true, recursive: true });

  await using series = await ZektaNumberTimeSeries.open({
    dirPath: DB_PATH,
    type: 'u16',
  });

  console.time('push');
  for (let i = 0; i < 1e5; i++) {
    series.push(i, i).catch(console.error);
  }
  await series.flush({ unload: false });
  console.timeEnd('push');

  // console.time('insert');
  // series.insert(Array.from({ length: 1e4 }, (_, i) => ({ time: i, value: i })));
  // await series.flush();
  // console.timeEnd('insert');

  console.time('select');
  const entries = await series.select({ from: 0, to: 1e6, asc: true });
  console.timeEnd('select');

  console.log(entries);
}

async function debugZekta_03_02() {
  await rm(DB_PATH.toString(), { force: true, recursive: true });

  await using series = await ZektaBytesTimeSeries.open({
    dirPath: DB_PATH,
  });

  console.time('push');
  for (let i = 0; i < 1e3; i++) {
    series.push(i, new TextEncoder().encode(i.toString())).catch(console.error);
  }
  await series.flush({ unload: false });
  console.timeEnd('push');

  // console.time('insert');
  // series.insert(Array.from({ length: 1e4 }, (_, i) => ({ time: i, value: i })));
  // await series.flush();
  // console.timeEnd('insert');

  console.time('select');
  const entries = await series.select({ from: 0, to: 1e6, asc: true });
  console.timeEnd('select');

  console.log(entries);
}

export async function debugZekta() {
  // await debugZekta_01();
  // await debugZekta_02();
  // await debugZekta_03_00();
  await debugZekta_03_01();
  // await debugZekta_03_02();
}
