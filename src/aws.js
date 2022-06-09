'use strict';
// Production adapters: SQS (main queue + DLQ) and DynamoDB (endpoints, attempts, breaker state, dedupe).
const { SQSClient, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

function sqsQueue({ queueUrl, dlqUrl, client = new SQSClient({}) }) {
  return {
    async send(body, delaySec) {
      await client.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(body), DelaySeconds: delaySec }));
    },
    async receive(max) {
      const r = await client.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: Math.min(10, max), WaitTimeSeconds: 20, VisibilityTimeout: 60 }));
      return (r.Messages || []).map((m) => ({ body: JSON.parse(m.Body), receipt: m.ReceiptHandle }));
    },
    async ack(receipt) {
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receipt }));
    },
    async sendDlq(body) {
      await client.send(new SendMessageCommand({ QueueUrl: dlqUrl, MessageBody: JSON.stringify(body) }));
    },
  };
}

/** Single-table design: pk = ENDPOINT#id | DELIVERY#id | BREAKER#id, sk = META | ATTEMPT#n. */
function dynamoStore({ table, client = DynamoDBDocumentClient.from(new DynamoDBClient({})) }) {
  const ttl = (days) => Math.floor(Date.now() / 1000) + days * 86400;
  return {
    async getEndpoint(id) {
      return (await client.send(new GetCommand({ TableName: table, Key: { pk: `ENDPOINT#${id}`, sk: 'META' } }))).Item;
    },
    async disableEndpoint(id, reason) {
      await client.send(new UpdateCommand({ TableName: table, Key: { pk: `ENDPOINT#${id}`, sk: 'META' },
        UpdateExpression: 'SET disabled = :t, disabledReason = :r', ExpressionAttributeValues: { ':t': true, ':r': reason } }));
    },
    async putAttempt(a) {
      await client.send(new PutCommand({ TableName: table, Item: { pk: `DELIVERY#${a.deliveryId}`, sk: `ATTEMPT#${String(a.attempt).padStart(3, '0')}`, ...a, expiresAt: ttl(30) } }));
    },
    async getBreaker(id) {
      return (await client.send(new GetCommand({ TableName: table, Key: { pk: `BREAKER#${id}`, sk: 'META' } }))).Item;
    },
    async putBreaker(id, s) {
      await client.send(new PutCommand({ TableName: table, Item: { pk: `BREAKER#${id}`, sk: 'META', ...s } }));
    },
    async incrementBreakerFailures(id) {
      const r = await client.send(new UpdateCommand({ TableName: table, Key: { pk: `BREAKER#${id}`, sk: 'META' },
        UpdateExpression: 'ADD failures :one', ExpressionAttributeValues: { ':one': 1 }, ReturnValues: 'UPDATED_NEW' }));
      return r.Attributes.failures;
    },
    async markDelivered(id) {
      await client.send(new PutCommand({ TableName: table, Item: { pk: `DELIVERY#${id}`, sk: 'DELIVERED', expiresAt: ttl(7) } }));
    },
    async isDelivered(id) {
      return !!(await client.send(new GetCommand({ TableName: table, Key: { pk: `DELIVERY#${id}`, sk: 'DELIVERED' } }))).Item;
    },
  };
}

module.exports = { sqsQueue, dynamoStore };
