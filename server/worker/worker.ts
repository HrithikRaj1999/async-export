import dotenv from "dotenv";
import { PassThrough } from "stream";

import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} from "@aws-sdk/client-sqs";

import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";

dotenv.config();

const REGION = process.env.AWS_REGION!;
const QUEUE_URL = process.env.SQS_QUEUE_URL!;
const TABLE = process.env.DDB_TABLE!;
const PROGRESS_EVERY_N_ROWS = Number(
  process.env.PROGRESS_EVERY_N_ROWS || "1000"
);

// Heartbeat settings
const VISIBILITY_TIMEOUT_SEC = 120;
const HEARTBEAT_EVERY_SEC = 45; // < visibility timeout

const sqs = new SQSClient({ region: REGION });
const s3 = new S3Client({ region: REGION });
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

const nowEpoch = () => Math.floor(Date.now() / 1000);

type Job = {
  taskId: string;
  processId: string;
  s3Bucket: string;
  s3Key: string;
};

async function* generateRows(processId: string): AsyncGenerator<any> {
  const total = 50000;
  for (let i = 1; i <= total; i++) {
    if (i % 5000 === 0) await new Promise((r) => setTimeout(r, 50));
    yield {
      processId,
      rowId: i,
      value: Math.random(),
      ts: new Date().toISOString(),
    };
  }
}

function getTotalRowsDemo() {
  return 50000;
}

async function getTask(taskId: string) {
  const out = await ddb.send(
    new GetCommand({ TableName: TABLE, Key: { taskId } })
  );
  return out.Item as any;
}

async function updateStatus(taskId: string, patch: any) {
  const expr: string[] = [];
  const names: any = {};
  const values: any = { ":u": nowEpoch() };
  expr.push("updatedAt = :u");

  if (patch.status) {
    names["#s"] = "status";
    values[":s"] = patch.status;
    expr.push("#s = :s");
  }
  if (patch.progress) {
    names["#p"] = "progress";
    values[":p"] = patch.progress;
    expr.push("#p = :p");
  }
  if (patch.error !== undefined) {
    names["#e"] = "error";
    values[":e"] = patch.error;
    expr.push("#e = :e");
  }
  if (patch.processingOwner !== undefined) {
    names["#o"] = "processingOwner";
    values[":o"] = patch.processingOwner;
    expr.push("#o = :o");
  }
  if (patch.processingStartedAt !== undefined) {
    names["#ps"] = "processingStartedAt";
    values[":ps"] = patch.processingStartedAt;
    expr.push("#ps = :ps");
  }

  await ddb.send(
    new UpdateCommand({
      TableName: TABLE,
      Key: { taskId },
      UpdateExpression: "SET " + expr.join(", "),
      ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
      ExpressionAttributeValues: values,
    })
  );
}

/**
 * Acquire lock: only one worker should process a given taskId.
 * - Start only if status == "queued"
 */
async function tryMarkProcessing(taskId: string, ownerId: string) {
  await ddb.send(
    new UpdateCommand({
      TableName: TABLE,
      Key: { taskId },
      UpdateExpression:
        "SET #s=:processing, processingOwner=:o, processingStartedAt=:t, updatedAt=:u",
      ConditionExpression: "#s = :queued",
      ExpressionAttributeNames: { "#s": "status" },
      ExpressionAttributeValues: {
        ":queued": "queued",
        ":processing": "processing",
        ":o": ownerId,
        ":t": nowEpoch(),
        ":u": nowEpoch(),
      },
    })
  );
}

function startVisibilityHeartbeat(receiptHandle: string) {
  const timer = setInterval(async () => {
    try {
      await sqs.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: VISIBILITY_TIMEOUT_SEC,
        })
      );
    } catch (e) {
      // ignore: if this fails, SQS may re-deliver; Dynamo lock still protects us
      console.error("Visibility heartbeat failed:", e);
    }
  }, HEARTBEAT_EVERY_SEC * 1000);

  return () => clearInterval(timer);
}

async function processJob(job: Job, receiptHandle: string) {
  const task = await getTask(job.taskId);
  if (!task) return;

  const ownerId = `worker-${process.pid}`;

  // Acquire lock (prevents duplicate workers)
  try {
    await tryMarkProcessing(job.taskId, ownerId);
  } catch (err: any) {
    // Condition failed => already processing or already done/failed
    // Safe action: delete message and exit
    await sqs.send(
      new DeleteMessageCommand({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: receiptHandle,
      })
    );
    return;
  }

  // Start SQS visibility heartbeat for long job
  const stopHeartbeat = startVisibilityHeartbeat(receiptHandle);

  const pass = new PassThrough();

  const uploader = new Upload({
    client: s3,
    params: {
      Bucket: job.s3Bucket,
      Key: job.s3Key,
      Body: pass,
      ContentType: "application/json",
    },
    queueSize: 4,
    partSize: 8 * 1024 * 1024,
    leavePartsOnError: false,
  });

  const uploadPromise = uploader.done();

  const totalRows = getTotalRowsDemo();
  let rowsDone = 0;
  let first = true;

  pass.write(
    `{"meta":${JSON.stringify({
      processId: job.processId,
      generatedAt: new Date().toISOString(),
      taskId: job.taskId,
    })},"data":[`
  );

  try {
    for await (const row of generateRows(job.processId)) {
      const chunk = JSON.stringify(row);
      if (!first) pass.write(",");
      pass.write(chunk);
      first = false;

      rowsDone++;

      if (rowsDone % PROGRESS_EVERY_N_ROWS === 0) {
        const percent = Math.floor((rowsDone / totalRows) * 100);
        await updateStatus(job.taskId, {
          progress: { rowsDone, rowsTotal: totalRows, percent },
        });
      }
    }

    pass.write("]}");
    pass.end();

    await uploadPromise;

    await updateStatus(job.taskId, {
      status: "done",
      progress: { rowsDone, rowsTotal: totalRows, percent: 100 },
      error: null,
    });

    stopHeartbeat();
  } catch (err: any) {
    stopHeartbeat();
    try {
      pass.destroy();
    } catch {}

    await updateStatus(job.taskId, {
      status: "failed",
      error: err?.message || "Unknown error",
    });

    throw err;
  }
}

async function main() {
  console.log("Worker started…");

  while (true) {
    const resp = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
        VisibilityTimeout: VISIBILITY_TIMEOUT_SEC,
      })
    );

    const msg = resp.Messages?.[0];
    if (!msg) continue;

    const receiptHandle = msg.ReceiptHandle!;
    const job: Job = JSON.parse(msg.Body!);

    try {
      console.log("Processing job:", job.taskId, job.processId);

      await processJob(job, receiptHandle);

      // delete only after full success
      await sqs.send(
        new DeleteMessageCommand({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: receiptHandle,
        })
      );

      console.log("Job done:", job.taskId);
    } catch (err) {
      console.error("Job failed (SQS will retry):", job.taskId, err);
      // do NOT delete => retry
      // configure DLQ to stop infinite retries
    }
  }
}

main().catch(console.error);
