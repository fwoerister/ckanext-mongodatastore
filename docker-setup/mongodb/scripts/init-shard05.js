rs.initiate(
   {
      _id: "shard05",
      version: 1,
      members: [
         { _id: 0, host : "mdb-shard05:27022" }
      ]
   }
)
