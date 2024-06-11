db = connect('mongodb://localhost/airflow');

db.comments.aggregate([
  {
    $group: {
      _id: "$content",
      count: {
        $sum: 1,
      },
    },
  },
  {
    $sort: {
      count: -1,
    },
  },
  {
    $limit: 5,
  },
  {
    $project: {
      _id: 0,
      comment: "$_id",
      count: 1,
    },
  }
]);

db.comments.find({
  $and: [
    { content: { $type: "string" } },
    { $expr: { $lt: [{ $strLenCP: "$content" }, 5] } }
  ]
});

db.comments.aggregate([
  {
    $group: {
      _id: {
        $dateToString: {
          format: "%Y-%m-%d",
          date: {
            $toDate:"$at"
          }
        }
      },
      averageRating: { $avg: "$score" }
    }
  },
  {
$project: {
      _id: 0,
      date: { $toDate: "$_id" },
      averageRating: 1
    }
  }
]);