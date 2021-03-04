import { AttributeValue, QueryCommandOutput } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from 'aws-lambda';

export const toBase64 = (str: string): string =>
  Buffer.from(str, 'utf-8').toString('base64');

export const fromBase64 = (str: string): string =>
  Buffer.from(str, 'base64').toString('utf-8');

export const toCursor = (obj: { [key: string]: string }): string =>
  toBase64(
    Object.keys(obj).reduce((prev, curr) => {
      if (prev.length === 0) {
        return obj[curr];
      }

      return `${prev}$${obj[curr]}`;
    }, '')
  );

export const fromCursor = (cursor: string): string[] =>
  fromBase64(cursor).split('$');

type D = {
  Pk?: string;
  Sk?: string;
  Type?: string;
};

type I = {
  id: string;
};

export type MarshalledDItem<T extends D> = {
  [P in keyof T]: AttributeValue;
};
export type MarshalledDItems<T extends D> = MarshalledDItem<T>[];

export function formatMarshalledDItem<T extends D, R extends I>(
  marshalledDItem: MarshalledDItem<T>
): R {
  const unmarshalledDItem = unmarshall(marshalledDItem) as T;

  const { Pk, Sk } = unmarshalledDItem;
  const id = toBase64(`${Pk}$${Sk}`);

  const formattedItem: I = {
    id,
  };

  delete unmarshalledDItem.Pk;
  delete unmarshalledDItem.Sk;
  delete unmarshalledDItem.Type;

  const keys = Object.keys(unmarshalledDItem);
  keys.forEach((key) => {
    const lowerCamelKey = `${key.charAt(0).toLowerCase()}${key.slice(1)}`;
    formattedItem[lowerCamelKey as keyof I] = unmarshalledDItem[
      key as keyof D
    ] as string;
  });

  return formattedItem as R;
}

export function formatMarshalledDItems<T extends D, R extends I>(
  marshalledDItems: MarshalledDItems<T>
): R[] {
  return marshalledDItems.map(formatMarshalledDItem) as R[];
}

export function createResponse<T extends D, R extends I>(
  event: APIGatewayProxyEventV2,
  queryCommandOutput: QueryCommandOutput
): APIGatewayProxyResultV2 {
  const items = (queryCommandOutput.Items ?? []) as MarshalledDItems<T>;
  const item = items[0];

  if (!item) {
    return {
      statusCode: 404,
    };
  }

  return {
    body: JSON.stringify(formatMarshalledDItem<T, R>(item)),
    statusCode: 200,
  };
}

export function createPaginatedResponse<T extends D, R extends I>(
  event: APIGatewayProxyEventV2,
  queryCommandOutput: QueryCommandOutput
): APIGatewayProxyResultV2 {
  const items = formatMarshalledDItems<T, R>(
    (queryCommandOutput.Items ?? []) as MarshalledDItems<T>
  );

  if (!queryCommandOutput.LastEvaluatedKey) {
    return {
      body: JSON.stringify(items),
      statusCode: 200,
    };
  }

  const lastEvaluatedKey = unmarshall(queryCommandOutput.LastEvaluatedKey);
  const cursor = toCursor(lastEvaluatedKey);

  const queryStringParamaters = event.queryStringParameters ?? {};
  const queryString = Object.keys(queryStringParamaters).reduce(
    (prev, curr) => {
      if (curr === 'cursor') {
        return prev;
      }

      return `${prev}&${curr}=${queryStringParamaters[curr]}`;
    },
    `?cursor=${cursor}`
  );

  const linkHeader = `<https://${event.requestContext.domainName}${event.rawPath}${queryString}>; rel="next"`;

  return {
    body: JSON.stringify(items),
    headers: { Link: linkHeader },
    statusCode: 200,
  };
}
