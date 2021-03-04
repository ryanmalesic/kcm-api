import {
  DynamoDB,
  QueryCommandOutput,
  QueryInput,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { fromCursor } from './helper';

const dynamoDB = new DynamoDB({});

interface GetItemsFromClassDescProps {
  classDesc?: string;
  runDate?: string;
  cursor?: string;
  limit?: string;
  sort?: string;
}

async function getItemsFromClassDesc(
  props: GetItemsFromClassDescProps
): Promise<QueryCommandOutput> {
  const queryParams: QueryInput = {
    ExpressionAttributeNames: { '#Pk': 'Pk', '#Sk': 'Sk' },
    ExpressionAttributeValues: marshall({
      ':Pk': `BOOK#${props.runDate}`,
      ':Sk': `ITEM#${props.classDesc}`,
    }),
    KeyConditionExpression: '#Pk = :Pk and begins_with(#Sk, :Sk)',
    Limit: parseInt(props.limit ?? '50', 10),
    ScanIndexForward: props.sort === 'asc',
    TableName: process.env.TABLE,
  };

  if (props.cursor) {
    const [Pk, Sk] = fromCursor(props.cursor);
    queryParams.ExclusiveStartKey = marshall({ Pk, Sk });
  }

  return dynamoDB.query(queryParams);
}

interface GetItemsFromItemCodeProps {
  runDate?: string;
  itemCode: string;
  cursor?: string;
  limit?: string;
  sort?: string;
}

export async function getItemsFromItemCode(
  props: GetItemsFromItemCodeProps
): Promise<QueryCommandOutput> {
  const queryParams: QueryInput = {
    ExpressionAttributeNames: { '#ItemCode': 'ItemCode' },
    ExpressionAttributeValues: marshall({ ':ItemCode': props.itemCode }),
    KeyConditionExpression: '#ItemCode = :ItemCode',
    IndexName: 'ByItemCode',
    Limit: parseInt(props.limit ?? '10', 10),
    ScanIndexForward: props.sort === 'asc',
    TableName: process.env.TABLE,
  };

  if (props.cursor) {
    const [Pk, Sk, ItemCode] = fromCursor(props.cursor);
    queryParams.ExclusiveStartKey = marshall({ Pk, Sk, ItemCode });
  }

  const queryCommandOutput = await dynamoDB.query(queryParams);

  if (props.runDate) {
    return {
      ...queryCommandOutput,
      Items:
        queryCommandOutput.Items?.filter(
          (item) => item.RunDate.S === props.runDate
        ) ?? [],
    };
  }

  return queryCommandOutput;
}

interface GetItemsFromUpcProps {
  runDate?: string;
  upc: string;
  cursor?: string;
  limit?: string;
  sort?: string;
}

export async function getItemsFromUpc(
  props: GetItemsFromUpcProps
): Promise<QueryCommandOutput> {
  const queryParams: QueryInput = {
    ExpressionAttributeNames: { '#Upc': 'Upc' },
    ExpressionAttributeValues: marshall({ ':Upc': props.upc }),
    KeyConditionExpression: '#Upc = :Upc',
    IndexName: 'ByUpc',
    Limit: parseInt(props.limit ?? '10', 10),
    ScanIndexForward: props.sort === 'asc',
    TableName: process.env.TABLE,
  };

  if (props.cursor) {
    const [Pk, Sk, Upc] = fromCursor(props.cursor);
    queryParams.ExclusiveStartKey = marshall({ Pk, Sk, Upc });
  }

  const queryCommandOutput = await dynamoDB.query(queryParams);

  if (props.runDate) {
    return {
      ...queryCommandOutput,
      Items:
        queryCommandOutput.Items?.filter(
          (item) => item.RunDate.S === props.runDate
        ) ?? [],
    };
  }

  return queryCommandOutput;
}

interface GetItemsProps {
  classDesc?: string;
  runDate?: string;
  itemCode?: string;
  upc?: string;
  cursor?: string;
  limit?: string;
  sort?: string;
}

export async function getItems(
  props: GetItemsProps
): Promise<QueryCommandOutput> {
  if (props.classDesc) {
    return getItemsFromClassDesc({
      ...props,
      classDesc: props.classDesc,
    });
  }

  if (props.itemCode) {
    return getItemsFromItemCode({ ...props, itemCode: props.itemCode });
  }

  if (props.upc) {
    return getItemsFromUpc({ ...props, upc: props.upc });
  }

  return dynamoDB.query({
    ExpressionAttributeNames: { '#Pk': 'Pk', '#Sk': 'Sk' },
    ExpressionAttributeValues: marshall({
      ':Pk': `BOOK#${props.runDate}`,
      ':Sk': 'ITEM#',
    }),
    KeyConditionExpression: '#Pk = :Pk and begins_with(#Sk, :Sk)',
    Limit: parseInt(props.limit ?? '10', 10),
    ScanIndexForward: props.sort === 'asc',
    TableName: process.env.TABLE,
  });
}
