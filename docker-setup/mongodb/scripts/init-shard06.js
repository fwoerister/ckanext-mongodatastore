rs.initiate(
   {
      _id: "shard06",
      version: 1,
      members: [
         { _id: 0, host : "mdb-shard06:27023" }
      ]
   }
)
