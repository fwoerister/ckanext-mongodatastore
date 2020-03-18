rs.initiate(
   {
      _id: "shard01",
      version: 1,
      members: [
         { _id: 0, host : "mdb-shard01:27018" },
      ]
   }
)
