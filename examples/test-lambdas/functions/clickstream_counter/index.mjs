// Kinesis consumer: decodes each click and records it, keyed by its sequence number.
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';

const ddb = new DynamoDBClient({});

export const handler = async (event) => {
  for (const record of event.Records) {
    const click = JSON.parse(Buffer.from(record.kinesis.data, 'base64').toString('utf8'));
    await ddb.send(new PutItemCommand({
      TableName: process.env.RESULTS_TABLE,
      Item: {
        fn: { S: 'clickstream-counter' }, id: { S: click.click_id },
        page: { S: click.page }, partition: { S: record.kinesis.partitionKey }, seq: { S: record.kinesis.sequenceNumber },
      },
    }));
  }
  return { counted: event.Records.length };
};
