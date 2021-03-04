import { APIGatewayProxyHandlerV2 } from 'aws-lambda';
import { getItems } from './dynamoUtils';
import { createPaginatedResponse, createResponse, fromCursor } from './helper';

export type DItem = {
  Pk: string;
  Sk: string;
  Type: string;
  CustNbr: string;
  RunDate: string;
  EffDate: string;
  Zone: string;
  ProdCode: string;
  Brand: string;
  Description: string;
  Pack: string;
  Size: string;
  CusPrd: string;
  PoaIdent: string;
  ItemCode: string;
  RestrictPfInd: string;
  DealPackInd: string;
  CripPoa: string;
  SlowMover: string;
  FullCaseInd: string;
  DsdInd: string;
  ThirteenWk: string;
  AkaType: string;
  Upc: string;
  Allow: string;
  AllowInd: string;
  AllowEndDate: string;
  Cost: string;
  CostInd: string;
  NetCost: string;
  UnitCost: string;
  NetUnitCost: string;
  ZoneNbr: string;
  BaseZoneMult: string;
  BaseZoneSrp: string;
  BaseZoneInd: string;
  BaseZonePct: string;
  BaseZonePctInd: string;
  RdcdZoneMult: string;
  RdcdZoneSrp: string;
  RdcdZoneInd: string;
  RdcdZonePct: string;
  RdcdZonePctInd: string;
  BaseCripMult: string;
  BaseCripSrp: string;
  BaseCripSrpInd: string;
  BaseCripPct: string;
  BaseCripPctInd: string;
  RdcdCripMult: string;
  RdcdCripSrp: string;
  RdcdCripSrpInd: string;
  RdcdCripPct: string;
  RdcdCripPctInd: string;
  RdcdSrpInd: string;
  EndDate: string;
  PalletQty: string;
  ItemAuth: string;
  ItemStatus: string;
  CategoryClass: string;
  CategoryClassDescription: string;
  ClassId: string;
  ClassDesc: string;
  SubClassId: string;
  SubClassDescription: string;
  VarietyId: string;
  VarietyDesc: string;
};

export type Item = {
  id: string;
  custNbr: string;
  runDate: string;
  effDate: string;
  zone: string;
  prodCode: string;
  brand: string;
  description: string;
  pack: string;
  size: string;
  cusPrd: string;
  poaIdent: string;
  itemCode: string;
  restrictPfInd: string;
  dealPackInd: string;
  cripPoa: string;
  slowMover: string;
  fullCaseInd: string;
  dsdInd: string;
  thirteenWk: string;
  akaType: string;
  upc: string;
  allow: string;
  allowInd: string;
  allowEndDate: string;
  cost: string;
  costInd: string;
  netCost: string;
  unitCost: string;
  netUnitCost: string;
  zoneNbr: string;
  baseZoneMult: string;
  baseZoneSrp: string;
  baseZoneInd: string;
  baseZonePct: string;
  baseZonePctInd: string;
  rdcdZoneMult: string;
  rdcdZoneSrp: string;
  rdcdZoneInd: string;
  rdcdZonePct: string;
  rdcdZonePctInd: string;
  baseCripMult: string;
  baseCripSrp: string;
  baseCripSrpInd: string;
  baseCripPct: string;
  baseCripPctInd: string;
  rdcdCripMult: string;
  rdcdCripSrp: string;
  rdcdCripSrpInd: string;
  rdcdCripPct: string;
  rdcdCripPctInd: string;
  rdcdSrpInd: string;
  endDate: string;
  palletQty: string;
  itemAuth: string;
  itemStatus: string;
  categoryClass: string;
  categoryClassDescription: string;
  classId: string;
  classDesc: string;
  subClassId: string;
  subClassDescription: string;
  varietyId: string;
  varietyDesc: string;
};

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  const { bookId } = event.pathParameters ?? {};
  const { classDesc, itemCode, upc, cursor, limit, sort } =
    event.queryStringParameters ?? {};

  if (!bookId && !classDesc && !itemCode && !upc) {
    return {
      body: JSON.stringify({
        message: 'ItemCode or Upc query parameter is required',
        description: 'ItemCode and Upc query parameters were both not provided',
        code: 400,
      }),
      statusCode: 400,
    };
  }

  const [Pk] = bookId ? fromCursor(bookId) : [];
  const runDate = Pk?.split('#')[1] ?? undefined;

  try {
    if (runDate && classDesc) {
      return createPaginatedResponse<DItem, Item>(
        event,
        await getItems({
          classDesc: decodeURIComponent(classDesc),
          runDate,
          cursor,
          limit,
          sort,
        })
      );
    }

    if (runDate && (itemCode || upc)) {
      return createResponse(
        event,
        await getItems({
          runDate,
          itemCode,
          upc,
          cursor,
          limit,
          sort,
        })
      );
    }

    return createPaginatedResponse<DItem, Item>(
      event,
      await getItems({
        runDate,
        itemCode,
        upc,
        cursor,
        limit,
        sort,
      })
    );
  } catch (error) {
    console.log('ERROR OCCURRED IN QUERY', error.message);
    return {
      body: JSON.stringify({ error: error.message }),
      statusCode: 500,
    };
  }
};
