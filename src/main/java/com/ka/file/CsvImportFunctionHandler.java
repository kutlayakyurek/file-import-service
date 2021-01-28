package com.ka.file;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ka.file.constant.Constants;
import com.ka.file.core.CsvParser;
import com.ka.file.model.FunctionResult;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

public class CsvImportFunctionHandler implements RequestHandler<S3Event, FunctionResult> {

  public FunctionResult handleRequest(S3Event s3event, Context context) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    LambdaLogger logger = context.getLogger();
    logger.log("EVENT: " + gson.toJson(s3event));

    long startTime = System.currentTimeMillis();
    FunctionResult statusReport = new FunctionResult();

    try {
      S3EventNotificationRecord record = s3event.getRecords().get(0);
      String srcBucket = record.getS3().getBucket().getName();

      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
      srcKey = URLDecoder.decode(srcKey, Constants.ENCODING);

      AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
      S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
      statusReport.setFileSize(s3Object.getObjectMetadata().getContentLength());

      logger.log("S3 Event Received: " + srcBucket + "/" + srcKey);

      // Read file directly from S3
      InputStream objectData = s3Object.getObjectContent();
      InputStreamReader isr = new InputStreamReader(objectData);
      BufferedReader br = new BufferedReader(isr);

      AmazonDynamoDB dynamoDBClient =
          AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();

      DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

      TableWriteItems energyDataTableWriteItems = new TableWriteItems(
          Constants.DYNAMODB_TABLE_NAME);

      List<Item> items = CsvParser.parse(br);

      for (List<Item> partition : Lists.partition(items, 25)) {
        energyDataTableWriteItems.withItemsToPut(partition);
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(energyDataTableWriteItems);

        do {
          Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

          if (outcome.getUnprocessedItems().size() > 0) {
            logger.log(
                "Retrieving the unprocessed " + outcome.getUnprocessedItems().size()
                    + " items.");
            outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
          }

        } while (outcome.getUnprocessedItems().size() > 0);
      }

      logger.log("Load finish in " + (System.currentTimeMillis() - startTime) + "ms");

      br.close();
      isr.close();
      s3Object.close();

      statusReport.setStatus(true);
    } catch (Exception ex) {
      logger.log(ex.getMessage());
    }

    statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
    return statusReport;
  }

}
