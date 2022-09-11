//simple count of all observations
//Question 2
db.BikesColl.find({},{}).count()

//first exclude all null prices, then calculate the average price and the total observations with non-null prices
//Question 3
db.BikesColl.aggregate([
 {
     $match: {
         "ad_data.Price": 
             {$exists: true
             }
         }
    },
     {
     $group: {
         "_id": null,
         "Avg_price": {
            $avg: "$ad_data.Price"
         },
         "Count": {
            $sum: 1
    }
    }
 }, 
 {
    $project: {
    "_id":0,
    "Avg_price":1,
    "Count":1
}
}])


//first exclude all null prices, then calculate the min and max prices
//Question 4
db.BikesColl.aggregate([
{
    $match: {
        "ad_data.Price": 
            {$exists: true
            }
        }
    },
    {
    $group: {
        "_id": null,
        "Max_price": {
            $max: "$ad_data.Price"
        },
        "Min_price": {
            $min: "$ad_data.Price"
    }
    }
}, 
{
    $project: {
    "_id":0,
    "Max_price":1,
    "Min_price":1
}
}])

//count all observations that have Negotiable prices using the "metadata.Negotiable" index
//Question 5
db.BikesColl.find({"metadata.Negotiable":1}).count()


//find the percentage of bikes with negotiable price per brand
//Question 6
db.BikesColl.aggregate([
    {
    $group: {
        "_id": {
            "Brand":"$metadata.brand"},
        "Count_per_brand": {
            $sum: 1
        },
        "Count_of_Negotiable" : {
            $sum:"$metadata.Negotiable"
        }
    }
},
{
    $project: {
     "_id":0,
     "Negotiable percentage": {$divide: ["$Count_of_Negotiable","$Count_per_brand"]},
     "Brand":"$_id.Brand",
     "Count_per_brand":1,
     "Count_of_Negotiable":1
     
}
}])

//find the highest average price of a brand
//Question 7
db.BikesColl.aggregate([
{
    $match: {
        "ad_data.Price": 
            {$exists: true
            }
        }
    },
    {
    $group: {
        "_id": {
            "Brand":"$metadata.brand"},
        "Avg_price_per_brand": {
            $avg: "$ad_data.Price"
        }
    }
},  
{
    $project: {
     "_id":0,
     "Brand":"$_id.Brand",
     "Avg_price_per_brand":1 
}
},
{
    $sort: {
        "Avg_price_per_brand":-1
    }
},
    {
      $limit : 1
    }
    ])
 

//Find the brands with top 10 highest average ages
//Question 8
db.BikesColl.aggregate([
{
    $match: {
        "ad_data.Registration": {
            $exists: true,
            "$ne": null 
    }
}
}
,{
    $group: {
       "_id": {
           "Brand": "$metadata.brand"},   
        "Avg_Registr_Date": {
            $avg: "$ad_data.Registration"
        }
    }
},
{ 
 $project: {
     "_id" : 0,
     "Brand": "$_id.Brand",
     "Avg_Registr_Date":1,
     "Avg_Age": {
         $subtract: [2022,"$Avg_Registr_Date"]
      }
      }
  },
  {
      $project: {
         "Brand":1,
          "Avg_Age": {
              $round: ["$Avg_Age",1]
          }
      }
  },
{
    $sort: {
        "Avg_Age":-1
    }
},
    {
      $limit : 10
    }
])

//count all posts for bikes that include ABS as extra
//Question 9
db.BikesColl.find({"extras": "ABS"},{}).count()
 

//Average mileage for bikes with ABS and and Led lights
//Question 10
db.BikesColl.aggregate([
{
    $match: {
        "ad_data.Mileage": {
            $exists: true
        },
        "extras": { $all : ["ABS", "Led lights"]}
    }
},
{
    $group: {
        "_id" : null
        ,"Avg_Mileage": {
            $avg: "$ad_data.Mileage"
        }
    }
},
{
    $project: {
        "_id":0,
        "Avg_Mileage": {
            $round: ["$Avg_Mileage",2]
        }
    }
}
])


//find the 3 most frequent colors per bike category
//Question 11
db.BikesColl.aggregate([
  { 
    $group : { 
      "_id" :  { 
        "Category" : "$ad_data.Category",
        "Color": "$ad_data.Color"
      },
      total: { $sum : 1 } 
    }
  },
  { $sort : 
      { total : -1 
          } 
  },
  { 
    $group : { 
        _id :  "$_id.Category",
        colors: { 
            $push: { 
                color: "$_id.Color",
                total: "$total"
            }
        }
     }
  }
  ,{ $out : "Categories_Colors" }  // output the documents to 'db.Colors'
]);

db.Categories_Colors.update( {}, {
  $push : {
    colors : { 
      $each : [],
      $slice : 3 
    }
  }
}, {
  multi:true
});

db.Categories_Colors.find()

//identify ads that are considered good deals (price, mileage, registration year and num. of previous owners considered)
//Question 12
var Avg_price = db.BikesColl.aggregate([
    { "$group": { "_id": "null", Avg_price: { $avg: "$ad_data.Price"} }}
]).toArray()[0]["Avg_price"];
    
var Avg_Mileage = db.BikesColl.aggregate([
{ "$group": { "_id": "null", Avg_Mileage: { $avg: "$ad_data.Mileage"} }}
]).toArray()[0]["Avg_Mileage"];

var Avg_Year = db.BikesColl.aggregate([
{ "$group": { "_id": "null", Avg_Year: { $avg: "$ad_data.Registration"} }}
]).toArray()[0]["Avg_Year"];
    
var Avg_owners = db.BikesColl.aggregate([
{ "$group": { "_id": "null", Avg_owners: { $avg: "$ad_data.Previous owners"} }}
]).toArray()[0]["Avg_owners"];


db.BikesColl.find({ $and :[{ "ad_data.Price": { $lte: Avg_price } }
, {"ad_data.Mileage": {$lte: Avg_Mileage}}
, {"ad_data.Registration": {$gte: Avg_Year}}
, {"ad_data.Previous owners": {$lte: Avg_owners}}]}).count()

print("Average price:", Avg_price, ",Average Mileage:", Avg_Mileage, ",Average Registration Year:",Avg_Year, ",Average Number of Owners:",Avg_owners)
