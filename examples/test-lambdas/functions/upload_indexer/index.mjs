// S3 ObjectCreated (prefix incoming/): records each object's key and size.
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';

const ddb = new DynamoDBClient({});

export const handler = async (event) => {
  for (const record of event.Records) {
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
    await ddb.send(new PutItemCommand({
      TableName: process.env.RESULTS_TABLE,
      Item: {
        fn: { S: 'upload-indexer' }, id: { S: key },
        bucket: { S: record.s3.bucket.name }, size: { N: String(record.s3.object.size ?? 0) },
        event: { S: record.eventName },
      },
    }));
  }
  return { indexed: event.Records.length };
};
