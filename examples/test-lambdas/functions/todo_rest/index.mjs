// REST API (payload 1.0): GET /todos, POST /todos, GET /todos/{id} over DynamoDB.
import { DynamoDBClient, PutItemCommand, GetItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { randomUUID } from 'node:crypto';

const ddb = new DynamoDBClient({});
const TABLE = process.env.TODO_TABLE;

const reply = (statusCode, body) => ({ statusCode, headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
const toTodo = (item) => ({ id: item.id.S, title: item.title.S, done: item.done.BOOL });

export const handler = async (event) => {
  if (event.httpMethod === 'GET' && event.pathParameters?.id) {
    const { Item } = await ddb.send(new GetItemCommand({ TableName: TABLE, Key: { id: { S: event.pathParameters.id } } }));
    return Item ? reply(200, toTodo(Item)) : reply(404, { error: 'not found' });
  }
  if (event.httpMethod === 'GET') {
    const { Items } = await ddb.send(new ScanCommand({ TableName: TABLE, Limit: 100 }));
    return reply(200, { todos: Items.map(toTodo) });
  }
  if (event.httpMethod === 'POST') {
    const body = JSON.parse(event.body || '{}');
    if (!body.title) return reply(400, { error: 'title is required' });
    const todo = { id: body.id || randomUUID(), title: body.title, done: false };
    await ddb.send(new PutItemCommand({ TableName: TABLE, Item: { id: { S: todo.id }, title: { S: todo.title }, done: { BOOL: false } } }));
    return reply(201, todo);
  }
  return reply(405, { error: `${event.httpMethod} not allowed` });
};
