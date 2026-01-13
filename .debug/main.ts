import { debugZekta } from './__debug/debug-zekta.ts';

globalThis.reportError ??= console.error;

/*
DOC:
https://www.sqlite.org/datatype3.html

grafana JSON:

https://grafana.com/docs/plugins/yesoreyeram-infinity-datasource/latest/json/
https://thingspeak.com/channels/38629/feed.json?start=${__from:date:YYYY-MM-DD HH:NN:SS}&end=${__to:date:YYYY-MM-DD HH:NN:SS}&average=10
https://thingspeak.com/channels/206644/feed.json?results=100
 */

async function main(): Promise<void> {
  await debugZekta();
}

main();
