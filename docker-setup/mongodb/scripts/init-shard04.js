rs.initiate(
   {
      _id: "shard04",
      version: 1,
      members: [
         { _id: 0, host : "mdb-shard04:27021" }
      ]
   }
)
