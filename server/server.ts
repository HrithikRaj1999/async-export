import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { v4 as uuidv4 } from "uuid";

import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const REGION = process.env.AWS_REGION!;
const BUCKET = process.env.S3_BUCKET!;
const QUEUE_URL = process.env.SQS_QUEUE_URL!;
const TABLE = process.env.DDB_TABLE!;
const PRESIGN_EXPIRES_SEC = Number(process.env.PRESIGN_EXPIRES_SEC || "300");
const TTL_HOURS = Number(process.env.TASK_TTL_HOURS || "24");

const sqs = new SQSClient({ region: REGION });
const s3 = new S3Client({ region: REGION });
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

const nowEpoch = () => Math.floor(Date.now() / 1000);

app.post("/exports/trigger", async (req, res) => {
  const { processId } = req.body || {};
  if (!processId)
    return res.status(400).json({ error: "processId is required" });

  const taskId = uuidv4();
  const createdAt = nowEpoch();
  const ttl = createdAt + TTL_HOURS * 3600;

  const s3Key = `exports/${processId}/${taskId}.json`; // final output location

  // 1) Write initial metadata in Dynamo
  await ddb.send(
    new PutCommand({
      TableName: TABLE,
      Item: {
        taskId,
        processId,
        status: "queued",
        s3Bucket: BUCKET,
        s3Key,
        progress: { rowsDone: 0, rowsTotal: null, percent: 0 },
        createdAt,
        updatedAt: createdAt,
        ttl,
        error: null,
      },
    })
  );

  // 2) Enqueue job in SQS
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify({
        taskId,
        processId,
        s3Bucket: BUCKET,
        s3Key,
      }),
    })
  );

  // 3) Return immediately
  return res.status(202).json({
    message: "Export job queued",
    taskId,
  });
});

app.get("/exports/status/:taskId", async (req, res) => {
  const taskId = req.params.taskId;

  const out = await ddb.send(
    new GetCommand({
      TableName: TABLE,
      Key: { taskId },
    })
  );

  if (!out.Item) return res.status(404).json({ error: "Invalid taskId" });

  const t: any = out.Item;

  // If done, generate presigned URL for download
  let downloadUrl: string | null = null;
  if (t.status === "done") {
    downloadUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: t.s3Bucket, Key: t.s3Key }),
      { expiresIn: PRESIGN_EXPIRES_SEC }
    );
  }

  return res.json({
    taskId: t.taskId,
    processId: t.processId,
    status: t.status,
    progress: t.progress,
    error: t.error,
    downloadUrl,
  });
});

const PORT = 31000;
app.listen(PORT, () =>
  console.log(`API server running at http://localhost:${PORT}`)
);
