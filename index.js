require("dotenv").config();
const { SQS } = require("@aws-sdk/client-sqs");
const _ = require("lodash");
const { v4: uuidv4 } = require("uuid");
const messages = require("./messages.json");

const {
  AWS_ACCESS_KEY_SQS,
  AWS_SECRET_KEY_SQS,
  AWS_REGION_SQS,
  AWS_API_VERSION,
  AWS_SQS_URL,
  AWS_SQS_DELAY,
} = process.env;

let SqsClient;

const initialize = () => {
  SqsClient = new SQS({
    apiVersion: AWS_API_VERSION,
    accessKeyId: AWS_ACCESS_KEY_SQS,
    secretAccessKey: AWS_SECRET_KEY_SQS,
    region: AWS_REGION_SQS,
  });
};

const sendEventBatch = async (messages = []) => {
  if (!SqsClient) initialize();

  const chunkEvents = _.chunk(messages, 10);
  const SQSPromises = chunkEvents.map((entries) => {
    return SqsClient.sendMessageBatch({
      QueueUrl: AWS_SQS_URL,
      Entries: [
        {
          Id: uuidv4(),
          MessageBody: JSON.stringify(entries),
        },
      ],
      DelaySeconds: AWS_SQS_DELAY,
    }).catch((error) => {
      console.log(error);
      error.entries = entries.map(({ Id }) => Id);
      return error;
    });
  });

  await Promise.all(SQSPromises);
};

sendEventBatch(messages);
