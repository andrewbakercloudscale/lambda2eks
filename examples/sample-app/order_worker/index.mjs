export const handler = async (event) => {
  const batchItemFailures = [];
  for (const record of event.Records) {
    try {
      const order = JSON.parse(record.body);
      if (!order.id) throw new Error('order without id');
      console.log(`processed order ${order.id}`);
    } catch (err) {
      console.error(err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
};
