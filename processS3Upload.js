const mysql = require("mysql2/promise");
const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");
const { SQSClient, DeleteMessageCommand } = require("@aws-sdk/client-sqs");

// Initialize AWS clients
const ssmClient = new SSMClient({
  region: process.env.AWS_REGION || "us-east-1",
});
const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});
const QUEUE_URL = process.env.SQS_QUEUE_URL; // SQS FIFO queue URL

// Cache for database configuration
let dbConfigCache = new Map();

/**
 * Fetch a parameter from AWS SSM Parameter Store.
 * @param {string} name - The parameter name.
 * @param {boolean} withDecryption - Whether the parameter is encrypted.
 * @returns {Promise<string>} - The parameter value.
 */
async function getParameter(name, withDecryption = false) {
  try {
    const command = new GetParameterCommand({
      Name: name,
      WithDecryption: withDecryption,
    });
    const response = await ssmClient.send(command);
    return response.Parameter.Value;
  } catch (error) {
    console.error(`Error fetching parameter ${name}:`, error);
    throw error;
  }
}

/**
 * Fetch database configuration for the given environment with caching.
 * @param {string} env - The environment (e.g., "dev", "prod").
 * @returns {Promise<object>} - Database configuration.
 */
async function getDatabaseConfig(env) {
  if (dbConfigCache.has(env)) {
    return dbConfigCache.get(env);
  }

  const [host, user, password, database] = await Promise.all([
    getParameter(`/${env}/MYSQL_HOST`, true),
    getParameter(`/${env}/MYSQL_USER`, true),
    getParameter(`/${env}/MYSQL_PASSWORD`, true),
    getParameter(`/${env}/MYSQL_DATABASE`, true),
  ]);

  const config = { host, user, password, database };
  dbConfigCache.set(env, config); // Cache the configuration
  return config;
}

/**
 * Process a single SQS record by inserting/updating the database and deleting the message.
 * @param {object} pool - The MySQL connection pool.
 * @param {object} record - The SQS message.
 */
async function processRecord(pool, record) {
  const payload = JSON.parse(record.body);
  const { name, timestamp } = payload.data;

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const userQuery = `
      INSERT INTO users (id, name, createdAt, updatedAt)
      VALUES (UUID(), ?, ?, ?)
      ON DUPLICATE KEY UPDATE updatedAt = VALUES(updatedAt);
    `;
    await connection.execute(userQuery, [name, timestamp, timestamp]);

    await sqsClient.send(
      new DeleteMessageCommand({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: record.receiptHandle,
      })
    );
    await connection.commit();
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

/**
 * Lambda handler to process messages from the SQS queue.
 */
exports.handler = async (event) => {
  let pool;
  try {
    if (!event.Records || !event.Records.length) {
      throw new Error("No records to process");
    }

    const firstMessage = JSON.parse(event.Records[0].body);
    const { env } = firstMessage;

    const dbConfig = await getDatabaseConfig(env);
    pool = mysql.createPool({
      ...dbConfig,
      port: 3306,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    const concurrencyLimit = 10;
    for (let i = 0; i < event.Records.length; i += concurrencyLimit) {
      const batch = event.Records.slice(i, i + concurrencyLimit);
      await Promise.all(batch.map((record) => processRecord(pool, record)));
    }

    return { statusCode: 200, body: "Messages processed successfully." };
  } catch (error) {
    console.error("Error in handler:", error);
    return { statusCode: 500, body: "Error processing messages." };
  } finally {
    if (pool) await pool.end();
  }
};
