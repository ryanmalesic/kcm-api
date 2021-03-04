import { S3, PutObjectCommand } from '@aws-sdk/client-s3';
import { S3RequestPresigner } from '@aws-sdk/s3-request-presigner';
import { createRequest } from '@aws-sdk/util-create-request';
import { formatUrl } from '@aws-sdk/util-format-url';
import { APIGatewayProxyHandlerV2 } from 'aws-lambda';

const s3Client = new S3({});
const signedRequest = new S3RequestPresigner(s3Client.config);

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  const { fileName } = event.queryStringParameters ?? {};

  if (fileName === undefined) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        code: '400',
        description: 'fileName query parameter is required.',
        message: 'fileName query parameter was not provided.',
      }),
    };
  }

  const request = await createRequest(
    s3Client,
    new PutObjectCommand({
      Bucket: process.env.BOOKS,
      Key: fileName,
    })
  );

  const signedUrl = formatUrl(
    await signedRequest.presign(request, {
      expiresIn: 60 * 60 * 24,
    })
  );

  return {
    statusCode: 200,
    body: JSON.stringify({ url: signedUrl }),
  };
};
