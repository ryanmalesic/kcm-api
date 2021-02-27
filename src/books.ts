import { APIGatewayProxyHandlerV2 } from 'aws-lambda';
import { DynamoDB, QueryInput } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { createPaginatedResponse, fromCursor } from './helper';

const dynamoDB = new DynamoDB({});

export type DBook = {
  Pk: string;
  Sk: string;
  Type: string;
  RunDate: string;
  FileName: string;
  FileSize: string;
  ClassDescs: string[];
};

export type Book = {
  id: string;
  runDate: string;
  fileName: string;
  fileSize: string;
  classDescs: string[];
};

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  const {
    runDate, cursor, limit, sort,
  } = event.queryStringParameters ?? {};

  // Setup query params
  const queryParams: QueryInput = {
    ExpressionAttributeNames: { '#Type': 'Type', '#Sk': 'Sk' },
    ExpressionAttributeValues: marshall({
      ':Type': 'BOOK',
      ':Sk': 'BOOK',
    }),
    IndexName: 'ByType',
    KeyConditionExpression: '#Type = :Type and begins_with(#Sk, :Sk)',
    Limit: parseInt(limit ?? '10', 10),
    ScanIndexForward: (sort ?? 'desc') === 'asc',
    TableName: process.env.TABLE,
  };

  // Rundate provided, set query params to include it in query
  if (runDate) {
    /* eslint-disable-next-line @typescript-eslint/no-non-null-assertion */
    queryParams.ExpressionAttributeValues![':Sk'].S = `BOOK#${runDate}`;
  }

  // Cursor provided, set exclusive start key
  if (cursor) {
    const [Pk, Sk, Type] = fromCursor(cursor);
    queryParams.ExclusiveStartKey = marshall({ Pk, Sk, Type });
  }

  try {
    const queryCommandOutput = await dynamoDB.query(queryParams);
    return createPaginatedResponse<DBook, Book>(event, queryCommandOutput);
  } catch (error) {
    console.log('ERROR OCCURRED IN QUERY', error.message);
    return {
      body: JSON.stringify({ error: error.message }),
      statusCode: 500,
    };
  }
};
