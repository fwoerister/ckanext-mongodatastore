rs.initiate(
   {
      _id: "shard02",
      version: 1,
      members: [
         { _id: 0, host : "mdb-shard02:27019" },
      ]
   }
)
