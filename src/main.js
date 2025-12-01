import { Client, Databases, ID } from 'node-appwrite';

/**
 * Appwrite Function to receive nRF Cloud Message Routing Service webhooks
 * and store device data in an Appwrite database.
 * 
 * Expected nRF Cloud payload format (HTTP destination):
 * {
 *   "deviceId": "device-id",
 *   "tenantId": "tenant-id",
 *   "topic": "prod/<team-id>/m/d/<device-id>/d2c",
 *   "receivedAt": "2024-01-15T10:30:00.000Z",
 *   "message": {
 *     "appId": "TEMP",
 *     "messageType": "DATA",
 *     "data": 23.5,
 *     "ts": 1705315800000
 *   }
 * }
 */

export default async ({ req, res, log, error }) => {
  // Initialize Appwrite client
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT || 'https://nyc.cloud.appwrite.io/v1')
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(req.headers['x-appwrite-key'] || process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);

  // Configuration from environment variables
  const DATABASE_ID = process.env.DATABASE_ID;
  const COLLECTION_ID = process.env.COLLECTION_ID;
  const WEBHOOK_SECRET = process.env.NRFCLOUD_WEBHOOK_SECRET;

  // Validate required environment variables
  if (!DATABASE_ID || !COLLECTION_ID) {
    error('Missing required environment variables: DATABASE_ID and COLLECTION_ID');
    return res.json({ success: false, error: 'Server configuration error' }, 500);
  }

  // Health check endpoint
  if (req.method === 'GET') {
    return res.json({
      status: 'ok',
      message: 'nRF Cloud webhook endpoint is active',
      timestamp: new Date().toISOString()
    });
  }

  if (req.method !== 'POST') {
    return res.json({ success: false, error: 'Method not allowed' }, 405);
  }

  try {
    // Parse the incoming webhook payload
    let payload;
    if (typeof req.body === 'string') {
      payload = JSON.parse(req.body);
    } else {
      payload = req.body;
    }

    log(`Received webhook payload: ${JSON.stringify(payload).substring(0, 500)}...`);

    // // Optional: Verify webhook signature
    // if (WEBHOOK_SECRET) {
    //   const signature = req.headers['x-nrfcloud-signature'] || req.headers['authorization'];
    //   if (!verifySignature(signature, WEBHOOK_SECRET)) {
    //     error('Invalid webhook signature');
    //     return res.json({ success: false, error: 'Invalid signature' }, 401);
    //   }
    // }

    // Handle both single message and batch message formats
    const messages = payload.messages || [payload];
    const results = [];

    for (const msg of messages) {
      try {
        const document = await processMessage(databases, DATABASE_ID, COLLECTION_ID, msg, log);
        results.push({ deviceId: msg.deviceId, documentId: document.$id, success: true });
      } catch (err) {
        error(`Failed to process message for device ${msg.deviceId}: ${err.message}`);
        results.push({ deviceId: msg.deviceId, success: false, error: err.message });
      }
    }

    const successCount = results.filter(r => r.success).length;
    log(`Processed ${successCount}/${results.length} messages successfully`);

    return res.json({
      success: true,
      processed: results.length,
      successful: successCount,
      results
    });

  } catch (err) {
    error(`Webhook processing error: ${err.message}`);
    return res.json({ success: false, error: 'Failed to process webhook', details: err.message }, 500);
  }
};

/**
 * Process a single nRF Cloud message and store it in the database
 */
async function processMessage(databases, databaseId, collectionId, msg, log) {
  const { teamId, deviceId, tenantId, topic, receivedAt, message } = msg;

  const appId = message?.appId || 'UNKNOWN';
  const messageType = message?.messageType || 'DATA';
  const data = message?.data;
  const timestamp = message?.ts || message?.time || Date.now();

  const documentData = {
    teamId: teamId,
    deviceId: deviceId,
    tenantId: tenantId || null,
    topic: topic || null,
    appId: appId,
    messageType: messageType,
    timestamp: new Date(timestamp).toISOString(),
    receivedAt: receivedAt || new Date().toISOString(),
    dataValue: typeof data === 'object' ? JSON.stringify(data) : String(data ?? ''),
    dataType: typeof data,
    rawMessage: JSON.stringify(message),
    createdAt: new Date().toISOString()
  };

  log(`Creating document for device ${deviceId}, appId: ${appId}`);

  const document = await databases.createDocument(
    databaseId,
    collectionId,
    ID.unique(),
    documentData
  );

  return document;
}

/**
 * Verify webhook signature
 */
function verifySignature(signature, secret) {
  if (!signature) return false;
  if (signature.startsWith('Bearer ')) {
    return signature.substring(7) === secret;
  }
  return signature === secret;
}
