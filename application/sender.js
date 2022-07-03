const { connection } = require("./db/index");
module.exports = async (dbURI) => {
  if (!dbURI) {
    console.log(dbURI);
    throw new Error("Fatal Error: Database URI (DB_URI) not define");
  }

  let connect = await connection(dbURI);

  let db = connect.db();
  let collections = await db.collection.count();
  console.log(collections);
};
