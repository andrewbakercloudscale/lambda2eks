// No trigger: other services call it with lambda:Invoke. Needs PRICING_API_KEY (a secret on EKS).
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';

const ddb = new DynamoDBClient({});
const TAX = Number(process.env.TAX_RATE || '0');

export const handler = async (event, context) => {
  if (!process.env.PRICING_API_KEY) throw new Error('PRICING_API_KEY is not set');
  const subtotal = event.items.reduce((sum, i) => sum + i.qty * i.unit_price, 0);
  const total = Math.round(subtotal * (1 + TAX) * 100) / 100;
  await ddb.send(new PutItemCommand({
    TableName: process.env.RESULTS_TABLE,
    Item: { fn: { S: 'price-calculator' }, id: { S: event.quote_id }, total: { N: String(total) } },
  }));
  return { quote_id: event.quote_id, subtotal, total, request_id: context.awsRequestId };
};
