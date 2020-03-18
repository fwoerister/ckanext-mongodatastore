rs.initiate(
   {
      _id: "shard03",
      version: 1,
      members: [
         { _id: 0, host : "mdb-shard03:27020" }
      ]
   }
)
