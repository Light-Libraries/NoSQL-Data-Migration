const MongoClient = require("mongodb").MongoClient;

const connection = async (dbURI) => {
  try {
    const client = new MongoClient(dbURI, { useUnifiedTopology: true });
    let con = await client.connect();
    console.log("MongoDB connection established successfully");
    return con;
  } catch (err) {
    console.log("MongoDB Connection Failed..");
  }
};

module.exports = { connection };
