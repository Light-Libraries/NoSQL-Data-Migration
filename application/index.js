const { MongoClient } = require("mongodb");
const amqp = require("amqplib");

class DataMigration {
  #senderDbURI;
  #receiverDbURI;
  #dBName;
  #tables;
  #batch;
  #amqpServerURL;
  #connection;
  #channel;

  constructor(
    senderDbURI,
    receiverDbURI,
    dBName,
    tables,
    batch,
    amqpServerURL
  ) {
    this.#senderDbURI = senderDbURI;
    this.#receiverDbURI = receiverDbURI;
    this.#dBName = dBName;
    this.#tables = tables;
    this.#batch = batch;
    this.#amqpServerURL = amqpServerURL;
  }

  async #produceData() {
    try {
      let senderClient = await this.#senderConnect();
      let limit = this.#batch;

      for (let i = 0; i < this.#tables.length; i++) {
        let table = this.#tables[i];
        let count = await senderClient.collection(table).countDocuments();

        if (count) {
          let pages = Math.ceil(count / limit);
          let skip = 0;

          for (let index = 0; index < pages; index++) {
            let data = await senderClient
              .collection(table)
              .find()
              .skip(skip)
              .limit(limit)
              .toArray();

            // queue data
            this.#channel.sendToQueue(
              this.#dBName,
              Buffer.from(JSON.stringify([table, data]))
            );
            console.log("[Data Queue]", [table, data]);

            skip += limit;
          }
        }
      }
    } catch (err) {
      console.log("[Produce Data To Queue Error]");
      console.log(err);
    }
  }

  async #senderConnect() {
    try {
      console.log("[Sender DB Connection]");

      let client = new MongoClient(this.#senderDbURI);
      await client.connect();

      let database = client.db(this.#dBName);
      console.log("[Sender DB Connected]");

      return database;
    } catch (err) {
      console.log("[Sender DB Connection Error]");
      console.log(err);
      process.exit();
    }
  }

  async #receiverConnect() {
    try {
      console.log("[Receiver DB Connection]");

      let client = new MongoClient(this.#receiverDbURI);
      await client.connect();

      let database = client.db(this.#dBName);
      console.log("[Receiver DB Connected]");

      return database;
    } catch (err) {
      console.log("[Receiver DB Connection Error]");
      console.log(err);
      process.exit();
    }
  }

  async #connectToQueue(key) {
    try {
      console.log("[RabbitMQ Connection]");
      this.#connection = await amqp.connect(this.#amqpServerURL);
      console.log("[RabbitMQ Connected]");

      this.#channel = await this.#connection.createChannel();

      await this.#channel.assertQueue(key);
    } catch (err) {
      console.log("[RabittMQ Connection Error]");
      console.log(err);
      process.exit();
    }
  }

  async producer() {
    try {
      console.log("[Producer Data Migration]");
      // Todo if table is empty retrive all tables
      if (!this.#tables.length) return;

      await this.#connectToQueue(this.#dBName);

      await this.#produceData();
    } catch (err) {
      console.log("[Producer Data Migration Error]");
      console.log(err);
    }
  }

  async consumer() {
    try {
      console.log("[Consumer Data Migration]");
      let receiverClient = await this.#receiverConnect();

      await this.#connectToQueue(this.#dBName);
      console.log("Listening for data.....");

      this.#channel.consume(this.#dBName, async (data) => {
        let parseData = JSON.parse(data.content);
        if (parseData.length) {
          await this.#consumeData(receiverClient, parseData[0], parseData[1]);
          this.#channel.ack(data);
          console.log("[Data Consume and Acknowledge Successfully]");
        }
      });
    } catch (err) {
      console.log("[Consumer Data Migration Error]");
      console.log(err);
      process.exit();
    }
  }

  async #consumeData(db, tableName, data) {
    try {
      if (data.length) {
        for (const obj in data) {
          let exist = await db
            .collection(tableName)
            .find({ _id: data[obj]._id })
            .toArray();

          if (!exist.length) {
            await db.collection(tableName).insertOne(data[obj]);
          }
        }
      }
    } catch (err) {
      console.log("[Consume  Data Error]");
      console.log(err);
    }
  }
}

module.exports = DataMigration;
