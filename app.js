require("dotenv").config();
const DataMigration = require("./application");

const start = async () => {
  let env_senderDbURI = process.env.env_senderDbURI;
  let env_receiverDbURI = process.env.env_receiverDbURI;
  let env_dBName = process.env.env_dBName;
  let env_tables = JSON.parse(process.env.env_tables);
  let env_amqpServerURL = process.env.env_amqpServerURL;
  let env_batch = process.env.env_batch;

  let instance = new DataMigration(
    env_senderDbURI,
    env_receiverDbURI,
    env_dBName,
    env_tables,
    env_batch,
    env_amqpServerURL
  );

  instance.producer();
  instance.consumer();
};
start();
