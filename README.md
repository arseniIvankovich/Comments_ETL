# Comments_ETL
Use astro configuration for airflow deployment.

### Prerequisites
To start airflow you need Docker. To start:
```
astro dev start
```
## Mongo queries

Collection name comments.

* Top 5 frequently seen comments

```js
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
```

* All entries where the length of the “content" field is less than 5 characters

```js
db.comments.find({
  $and: [
    { content: { $type: "string" } },
    { $expr: { $lt: [{ $strLenCP: "$content" }, 5] } }
  ]
});
```

* Average rating for each day

```js
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
```