import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch';

const cw = new CloudWatchClient({});
// Counts survive between invocations of the same environment and are flushed every minute.
const counts = new Map();
let lastFlush = Date.now();

export const handler = async (event) => {
  for (const record of event.Records) {
    const click = JSON.parse(Buffer.from(record.kinesis.data, 'base64').toString());
    counts.set(click.page, (counts.get(click.page) || 0) + 1);
  }
  if (Date.now() - lastFlush > 60_000) {
    const data = [...counts].map(([page, n]) => ({ MetricName: 'Clicks', Dimensions: [{ Name: 'Page', Value: page }], Value: n }));
    counts.clear();
    lastFlush = Date.now();
    if (data.length) await cw.send(new PutMetricDataCommand({ Namespace: 'Shop', MetricData: data.slice(0, 1000) }));
  }
};
