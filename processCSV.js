const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const csvParser = require("csv-parser");
const { Readable } = require("stream");

// Initialize AWS clients
const REGION = process.env.AWS_REGION || "us-east-1"; // AWS region from environment or default
const s3Client = new S3Client({ region: REGION }); // S3 client to fetch files
const sqsClient = new SQSClient({ region: REGION }); // SQS client to send messages to the queue

const QUEUE_URL = process.env.SQS_QUEUE_URL; // SQS FIFO queue URL
const CHUNK_SIZE = 10; // Number of rows per chunk to send to SQS

/**
 * Convert a stream to a string (helper function to read S3 object data).
 * @param {ReadableStream} stream - The input stream.
 * @returns {Promise<Buffer>} - The data as a single buffer.
 */
async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk); // Collect chunks of data
  }
  return Buffer.concat(chunks); // Combine chunks into a single buffer
}

/**
 * Send a chunk of CSV data to the SQS queue.
 * @param {Array} chunk - A chunk of rows from the CSV.
 * @param {string} env - The environment (e.g., "dev", "prod").
 */
async function sendChunkToSQS(chunk, env = "dev") {
  const entries = chunk.map((row, index) => ({
    Id: `row-${index}-${Date.now()}`, // Unique message ID for SQS
    MessageBody: JSON.stringify({ data: row, env }), // Message payload with data and environment
    MessageGroupId: "CSVChunkGroup", // FIFO group for ordering
    MessageDeduplicationId: `dedup-${row.id || index}-${Date.now()}`, // Deduplication ID
  }));

  const params = {
    QueueUrl: QUEUE_URL,
    Entries: entries, // Send all rows as a batch
  };

  try {
    const response = await sqsClient.send(new SendMessageBatchCommand(params));
    console.log("Batch sent successfully:", response);
  } catch (error) {
    console.error("Error sending batch to SQS:", error);
    throw error;
  }
}

/**
 * Lambda handler function to process S3 file upload.
 * @param {Object} event - The S3 event payload.
 */
exports.handler = async (event) => {
  console.log("Event received:", JSON.stringify(event, null, 2));

  // Extract bucket name and object key from the event
  const bucketName = event.Records[0].s3.bucket.name;
  const objectKey = decodeURIComponent(event.Records[0].s3.object.key);

  console.log(
    `Processing file from S3: bucket=${bucketName}, key=${objectKey}`
  );

  let env = bucketName.split("-")[1]; // Derive the environment from the bucket name
  const params = { Bucket: bucketName, Key: objectKey };

  try {
    // Fetch the file from S3
    const s3Object = await s3Client.send(new GetObjectCommand(params));
    const csvData = await streamToString(s3Object.Body);

    // Parse and process the CSV file
    const rows = [];
    let chunkCounter = 0;
    const stream = Readable.from(csvData.toString()).pipe(csvParser());

    for await (const row of stream) {
      rows.push(row); // Add each parsed row

      if (rows.length === CHUNK_SIZE) {
        chunkCounter++;
        console.log(`Sending chunk ${chunkCounter} to SQS...`);
        await sendChunkToSQS(rows, env); // Send rows as a batch
        rows.length = 0; // Clear the buffer
      }
    }

    // Send any remaining rows
    if (rows.length > 0) {
      chunkCounter++;
      console.log(`Sending final chunk ${chunkCounter} to SQS...`);
      await sendChunkToSQS(rows, env);
    }

    console.log("File processed successfully.");
    return {
      statusCode: 200,
      body: "File processed and sent to SQS successfully.",
    };
  } catch (error) {
    console.error("Error processing file:", error);
    return { statusCode: 500, body: "Error processing file." };
  }
};
