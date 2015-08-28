---
layout: post
title: Data analysis with Spark and Cassandra, part 1
subtitle: Yelp dataset, extracting information the dumb way
---

The full Scala script for this example is available [here](/assets/find_top_reviewers_part_1.scala).

# Data Model

In the previous post, we added the Yelp dataset to Cassandra, keeping the data model from the json files. We basically have 5 different tables.

1. Users
1. Businesses
1. Reviews
1. Check-ins
1. Tip

This is a very relational model. For the purpose of the exercise, let's keep that data model and play with it. We'll update it in the future posts.

# Starting the Spark shell

In order to keep it simple, let's just run everthing in the spark-shell. To start it with the *Spark Cassandra Connector*, add the jar. You can also define the RAM allocated to the spark driver. In this example, I allocated 5 Gb.

    spark-shell --driver-memory 5g --jars ~/opt/packages/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.5.0-M1-SNAPSHOT.jar

You can also start the spark-shell with a scala script using the *-i* flag.

    spark-shell --driver-memory 5g --jars ~/opt/packages/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.5.0-M1-SNAPSHOT.jar -i script.scala

# Doing stuff!

The goal is: Find the users with the most reviews in the dataset along with the city where they reviewed the most and the category that they reviewed the most.

## Imports and configuration

The spark-shell automatically defines a SparkContext, so we need to stop it.

    scala> sc.stop

Then we need to import the Spark and Spark Cassandra Connector

    scala> import com.datastax.spark.connector._
    scala> import org.apache.spark.SparkContext
    scala> import org.apache.spark.SparkContext._
    scala> import org.apache.spark.SparkConf

Let's define our SparkContext. This one connects to a local Cassandra running on *127.0.0.1* and defines a local Spark on a machine with 4 cores.

    scala> val conf = new SparkConf(true)
    scala> conf.set("spark.cassandra.connection.host", "127.0.0.1")
    scala> val sc = new SparkContext("local[4]", "Cassandra Connector Test", conf)


## Defining classes to help with mapping

Let's define classes for *Reviews*, *Businesses* and *Users*.

    scala> case class Review2 (review_id: String,
                       user_id: String,
                       business_id: String,
                       stars: Double,
                       text: String);

    scala> case class User (user_id: String,
                     name: String,
                     yelping_since: String,
                     votes: Map[String, Integer],
                     review_count: Integer,
                     friends: Set[String],
                     fans: Integer,
                     average_stars: Double,
                     compliments: Map[String, Integer],
                     elite: Set[Integer]);

    scala> case class Business (business_id: String,
                         full_address: String,
                         open: Boolean,
                         categories: List[String],
                         city: String, review_count:
                         Integer, name: String,
                         neighborhoods: List[String],
                         longitude: Double,
                         latitude: Double,
                         state: String,
                         stars: Double);

## Get the data from Cassandra

Using the classes we defined previously, we're able to get the data from Cassandra.<sup>[1](#yelpkeyspace)</sup>

    scala> val reviews = sc.cassandraTable[Review]("yelp", "reviews")
    scala> val businesses = sc.cassandraTable[Business]("yelp", "businesses")
    scala> val users = sc.cassandraTable[User]("yelp", "users")


## Printing the top 5 reviewers

Let's start by simply grouping the reviews by user_id, sorting them to get the top 5 reviewers.

- Group reviews by user_id. We get an RDD like (user_id, Iterable[Review])

      val reviewsByUser = reviews.groupBy(r => r.user_id)

- Map RDD to get the number of reviews for each user_id. RDD is now (nb_reviews, user_id)

      val nbReviewsForUser = reviewsByUser.map(reviewByUser => (reviewByUser._2.size, reviewByUser._1)).sortByKey(false).take(5)

      for((nb_reviews, user_id) <- top5 ){
       println( user_id + " has " + nb_reviews + " reviews." );
     }

Here's the output I get

    kGgAARL2UmvCcTRfiscjug has 1427 reviews.
    ikm0UCahtK34LbLCEw4YTw has 1225 reviews.
    Iu3Jo9ROp2IWC9FwtWOaUQ has 1186 reviews.
    glRXVWWD6x1EZKfjJawTOg has 1080 reviews.
    ia1nTRAQEaFWv0cwADeK7g has 941 reviews.

It works! In less that 10 seconds, it goes through more that 360 000 users and 1 500 000 reviews. Pretty good for a first try.

<img src="http://i.giphy.com/6oMKugqovQnjW.gif" alt="Celebration time." style="margin-left: auto; margin-right: auto; width: 400px;"/>


## Adding the user's name

I'm really happy to know that *kGgAARL2UmvCcTRfiscjug* is the top reviewer of that dataset... but I'd rather have his/her name.

- To do that, we need to join the *Reviews* table with the *Users* table, using the user_id. Let's start by still grouping the reviews by user_id, and then defining a PairRDD with the *Users* table: (user_id, User)

      val reviewsByUser = reviews.groupBy(r => r.user_id)
      val usersByUserId = users.keyBy(u => u.user_id)

- Let's join them! We'll have an RDD like : (user_id, (User, Iterable[Review]))

      val joinedUserReviews = usersByUserId.join(reviewsByUser)

- Then, do the same thing as before to sort the RDD by review count and print the top 5.

      val top5WithNames = joinedUserReviews.map(j => (j._2._2.size, j._2._1)).sortByKey(false).take(5)

      for((nb_reviews, user) <- top5WithNames ){
         println( user.name + " has " + nb_reviews + " reviews." );
      }

The output is now:

    J has 1427 reviews.
    Rand has 1225 reviews.
    Norm has 1186 reviews.
    Jade has 1080 reviews.
    Emily has 941 reviews.

Well *kGgAARL2UmvCcTRfiscjug*, I can now call you J.

<img src="http://i.giphy.com/Z7bxVQl7nWes.gif" alt="Celebration time." style="margin-left: auto; margin-right: auto; width: 400px;"/>

## Top city for reviewer

Let's make this even more interesting. For each reviewer, get the top city in which they reviewed. Because the city is only defined in the *Businesses*, we'll need to integrate that in there.

- Let's start by joining the reviews with the businesses. We end up with (business_id, (Business, Review))

      val reviewsByBusinessId = reviews.keyBy(r => r.business_id)
      val businessesByBusinessId = businesses.keyBy(b => b.business_id)
      val reviewsForBusiness = businessesByBusinessId.join(reviewsByBusinessId)

- Then, let's group the reviewsForBusiness by user_id, just like we did before => (user_id, Iterable[(Review, Business)])

      val reviewsForBusinessByUser = reviewsForBusiness.groupBy(r => r._2._2.user_id)

- Join the users with the reviewsForBusiness => (user_id, (User, Iterable[(business_id, (Business, Review))]))

      val usersByUserId = users.keyBy(u => u.user_id)
      val joinedUserReviewsForBusiness = usersByUserId.join(reviewsForBusinessByUser)

- ... and sort by the number of reviews for each user => (nb_reviews, (User, Iterable[(business_id, (Business, Review))])). Notice that we have almost exactly the same RDD that we had in the previous example, but we have added the Business to each Review.

      val topWithNames = joinedUserReviewsForBusiness.map(j => (j._2._2.size, j._2)).sortByKey(false)

- Now this is where it gets tricky... We need to group the reviews by city, and then get the city with the most reviews. We then add that city along with the number of reviews for that user in that city to the RDD. We now have => (nb_reviews, (User, Iterable[(business_id, (Business, Review))], (city, nb_reviews_in_city)))

      val topWithTopCity = topWithNames.map(t => (t._1, (t._2._1, t._2._2, t._2._2.groupBy(rb => rb._2._1.city).mapValues(_.size).maxBy(_._2))))

- Let's print it

      for((nb_reviews,(user, reviews, (city, nb_reviews_in_city))) <- topWithTopCity.take(5) ){
         println( user.name + " has " + nb_reviews + " total reviews. City is " + city + " with " + nb_reviews_in_city + " reviews.");
      }

The output is:

    J has 1427 total reviews. City is Las Vegas with 673 reviews.
    Rand has 1225 total reviews. City is Phoenix with 607 reviews.
    Norm has 1186 total reviews. City is Las Vegas with 1126 reviews.
    Jade has 1080 total reviews. City is Las Vegas with 1029 reviews.
    Emily has 941 total reviews. City is Las Vegas with 865 reviews.

Looks like Las Vegas is the place to be...
<img src="http://i.giphy.com/rreXB9iaKjDTa.gif" alt="Celebration time." style="margin-left: auto; margin-right: auto; width: 400px;"/>

## One more thing!

While were at it, let's add the top category for each reviewer. It's pretty much the same as adding the top city, except that each Business has a list of categories instead of a single city.

- Starting with topWithTopCity, let's define a new RDD with the top category. To do that, we'll need to flatten the category lists of each Business for each Review a User has made. (nb_reviews, (User, Iterable[(business_id, (Business, Review))], (City, nb_reviews_in_city), (category, nb_reviews_for_category)))

      val topWithTopCityAndTopCategory = topWithTopCity.map(t => (t._1, (t._2._1, t._2._2, t._2._3,
                    t._2._2.map(rb =>rb._2._1.categories)
                    .flatten
                    .foldLeft(Map[String,Int]() withDefaultValue 0)
                    {(m,x) => m + (x -> (1 + m(x)))}
                    .maxBy(_._2))))

  - t._2._2: *Iterable[(business_id, (Business, Review))]*
  - rb: *(business_id, (Business, Review))*
  - rb._2._1: *Business*

- Let's print it

      for( (nb_reviews,(user, reviews,(city, city_nb_reviews), (category, category_nb_reviews))) <- topWithTopCityAndTopCategory.take(5) ){
         println( user.name + " has " + nb_reviews + " total reviews. City is " + city + " with " + city_nb_reviews + " reviews. Most reviewed category is " + category + " with " + category_nb_reviews + " reviews.");
      }


And the output is:

    J has 1427 total reviews. City is Las Vegas with 673 reviews. Most reviewed category is Restaurants with 661 reviews.
    Rand has 1225 total reviews. City is Phoenix with 607 reviews. Most reviewed category is Restaurants with 1086 reviews.
    Norm has 1186 total reviews. City is Las Vegas with 1126 reviews. Most reviewed category is Restaurants with 543 reviews.
    Jade has 1080 total reviews. City is Las Vegas with 1029 reviews. Most reviewed category is Restaurants with 586 reviews.
    Emily has 941 total reviews. City is Las Vegas with 865 reviews. Most reviewed category is Restaurants with 560 reviews.

As you can imagine, this is probably not extraordinary performance wise... We'll try and make it better in another session. For now, let's just be happy that it works!

# We're done!

<img src="http://i.giphy.com/9rnSh46IvgcdW.gif" alt="Celebration time." style="margin-left: auto; margin-right: auto; width: 400px;"/>

<a name="yelpkeyspace">1</a>: *yelp* refers to the keyspace.
