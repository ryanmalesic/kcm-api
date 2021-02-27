import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import * as s3n from '@aws-cdk/aws-s3-notifications';
import * as sst from '@serverless-stack/resources';

export default class CategoriesStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props);

    // Create the S3 bucket for books
    const bucket = new s3.Bucket(this, 'Books', {
      cors: [
        {
          maxAge: 3000,
          allowedOrigins: ['*'],
          allowedHeaders: ['*'],
          allowedMethods: [s3.HttpMethods.PUT],
        },
      ],
    });

    // Create the DynamoDB Table
    const table = new sst.Table(this, 'Table', {
      fields: {
        ItemCode: sst.TableFieldType.STRING,
        Pk: sst.TableFieldType.STRING,
        Sk: sst.TableFieldType.STRING,
        Type: sst.TableFieldType.STRING,
        Upc: sst.TableFieldType.STRING,
      },
      primaryIndex: { partitionKey: 'Pk', sortKey: 'Sk' },
      secondaryIndexes: {
        ByType: { partitionKey: 'Type', sortKey: 'Sk' },
        ByItemCode: { partitionKey: 'ItemCode', sortKey: 'Sk' },
        ByUpc: { partitionKey: 'Upc', sortKey: 'Sk' },
      },
    });

    // Create the parse Function
    const parseFunction = new sst.Function(this, 'ParseFunction', {
      handler: 'src/parse.handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      timeout: 900,
      environment: {
        BOOKS: bucket.bucketName,
        TABLE: table.dynamodbTable.tableName,
      },
    });

    // Attach the dynamodb and s3 permissions to the parse function
    parseFunction.attachPermissions([bucket, table]);

    // Add book object created trigger for parse function
    bucket.addObjectCreatedNotification(
      new s3n.LambdaDestination(parseFunction),
      { suffix: '.csv' },
    );
    bucket.addObjectCreatedNotification(
      new s3n.LambdaDestination(parseFunction),
      { suffix: '.CSV' },
    );

    // Create the HTTP Api
    const api = new sst.Api(this, 'Api', {
      cors: true,
      customDomain: {
        domainName: `api.${scope.stage}.karnscategorymanager.com`,
        hostedZone: 'karnscategorymanager.com',
      },
      defaultFunctionProps: {
        environment: {
          BOOKS: bucket.bucketName,
          TABLE: table.dynamodbTable.tableName,
        },
        runtime: lambda.Runtime.NODEJS_14_X,
        srcPath: 'src',
      },
      routes: {
        'GET  /books': 'books.handler',
        'GET  /books/{bookId}/items': 'items.handler',
        'GET  /items': 'items.handler',
        'GET  /presigned': 'presigned.handler',
      },
    });

    // Attach the dynamodb permissions to the api
    api.attachPermissions([bucket, table]);

    // Output the api to console
    new cdk.CfnOutput(this, 'ApiEndpoint', {
      value: api.httpApi.apiEndpoint,
    });
  }
}
