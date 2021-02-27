import { DynamoDB, WriteRequest } from '@aws-sdk/client-dynamodb';
import { S3 } from '@aws-sdk/client-s3';
import { marshall } from '@aws-sdk/util-dynamodb';
import { APIGatewayProxyResultV2, S3CreateEvent } from 'aws-lambda';
import parse from 'csv-parse';

const dynamoDBClient = new DynamoDB({});
const s3Client = new S3({});

const columns = [
  'CustNbr',
  'RunDate',
  'EffDate',
  'Zone',
  'ProdCode',
  'Brand',
  'Description',
  'Pack',
  'Size',
  'CusPrd',
  'PoaIdent',
  'ItemCode',
  'RestrictPfInd',
  'DealPackInd',
  'CripPoa',
  'SlowMover',
  'FullCaseInd',
  'DsdInd',
  'ThirteenWk',
  'AkaType',
  'Upc',
  'Allow',
  'AllowInd',
  'AllowEndDate',
  'Cost',
  'CostInd',
  'NetCost',
  'UnitCost',
  'NetUnitCost',
  'ZoneNbr',
  'BaseZoneMult',
  'BaseZoneSrp',
  'BaseZoneInd',
  'BaseZonePct',
  'BaseZonePctInd',
  'RdcdZoneMult',
  'RdcdZoneSrp',
  'RdcdZoneInd',
  'RdcdZonePct',
  'RdcdZonePctInd',
  'BaseCripMult',
  'BaseCripSrp',
  'BaseCripSrpInd',
  'BaseCripPct',
  'BaseCripPctInd',
  'RdcdCripMult',
  'RdcdCripSrp',
  'RdcdCripSrpInd',
  'RdcdCripPct',
  'RdcdCripPctInd',
  'RdcdSrpInd',
  'EndDate',
  'PalletQty',
  'ItemAuth',
  'ItemStatus',
  'CategoryClass',
  'CategoryClassDescription',
  'ClassId',
  'ClassDesc',
  'SubClassId',
  'SubClassDescription',
  'VarietyId',
  'VarietyDesc',
] as const;

const batchPutItems = async (items: WriteRequest[]): Promise<void> => {
  await dynamoDBClient.batchWriteItem({
    RequestItems: { [process.env.TABLE as string]: items },
  });
};

export async function handler(
  event: S3CreateEvent,
): Promise<APIGatewayProxyResultV2> {
  console.log(`Event received: ${JSON.stringify(event)}`);

  const start = new Date();
  const fileName = event.Records[0].s3.object.key;
  const fileSize = Math.round(event.Records[0].s3.object.size / 10485.76) / 100;

  const file = await s3Client.getObject({
    Bucket: process.env.BOOKS,
    Key: fileName,
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const parser = (file.Body as any).pipe(
    parse({
      columns: Array.from(columns),
      fromLine: 4,
      relax: true,
      relaxColumnCount: true,
      // toLine: 10, // Uncomment for testing (won't blow up logs)
      trim: true,
    }),
  );

  const classDescs = new Set();
  let itemCount = 1;
  let itemsToPut: WriteRequest[] = [];
  const itemsPut: { [key: string]: boolean } = {};
  let runDate = '';

  // eslint-disable-next-line no-restricted-syntax
  for await (const record of parser) {
    if (!runDate) {
      runDate = new Date(Date.parse(record.RunDate)).toISOString().slice(0, 10);
    }

    const itemPk = `BOOK#${runDate}`;
    const itemSk = `ITEM#${record.ClassDesc}#${record.Brand}#${record.Description}#${record.Size}#${record.Pack}#${record.ItemCode}`;

    if (!itemsPut[itemSk]) {
      classDescs.add(record.ClassDesc);
      itemCount += 1;
      itemsPut[itemSk] = true;
      itemsToPut.push({
        PutRequest: {
          Item: marshall({
            ...record,
            Pk: itemPk,
            Sk: itemSk,
            Type: 'ITEM',
            RunDate: runDate,
          }),
        },
      });

      if (itemsToPut.length === 25) {
        try {
          await batchPutItems(itemsToPut);
          itemsToPut = [];
        } catch (error) {
          console.log(
            'ERROR OCCURRED IN BATCHPUTITEMS',
            error.message,
            JSON.stringify(itemsToPut),
          );
          return {};
        }
      }
    }
  }

  if (itemsToPut.length > 0) {
    try {
      await batchPutItems(itemsToPut);
    } catch (error) {
      console.log(
        'ERROR OCCURRED IN BATCHPUTITEMS',
        error.message,
        JSON.stringify(itemsToPut),
      );
      return {};
    }
  }

  console.log(`${itemCount} Items inserted.`);
  console.log(`Inserting Book{RunDate=${runDate}}.`);

  const bookPk = `BOOK#${runDate}`;
  const bookSk = `BOOK#${runDate}`;

  try {
    await dynamoDBClient.putItem({
      Item: marshall({
        Pk: bookPk,
        Sk: bookSk,
        Type: 'BOOK',
        ClassDescs: Array.from(classDescs),
        FileName: fileName,
        FileSize: fileSize.toString(),
        ItemCount: itemCount.toString(),
        RunDate: runDate,
      }),
      TableName: process.env.TABLE,
    });
  } catch (error) {
    console.log('ERROR OCCURRED IN PUTITEM BOOK', error.message);
    return {};
  }

  const end = new Date();
  const time = (end.getTime() - start.getTime()) / 1000;

  console.log(
    `Book{RunDate=${runDate}}'s ${itemCount} items inserted in ${time} seconds.`,
  );

  return {};
}
