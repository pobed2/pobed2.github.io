import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

sc.stop

case class Review (review_id: String, user_id: String, business_id: String, stars: Double, text: String);

case class User (user_id: String, name: String, yelping_since: String, votes: Map[String, Integer], review_count: Integer, friends: Set[String], fans: Integer, average_stars: Double, compliments: Map[String, Integer], elite: Set[Integer]);

case class Business (business_id: String, full_address: String, open: Boolean, categories: List[String], city: String, review_count: Integer, name: String, neighborhoods: List[String], longitude: Double, latitude: Double, state: String, stars: Double);


val conf = new SparkConf(true)
conf.set("spark.cassandra.connection.host", "127.0.0.1")
conf.set("spark.shuffle.memoryFraction", "0.35")
val sc = new SparkContext("local[4]", "Cassandra Connector Test", conf)

val reviews = sc.cassandraTable[Review]("yelp", "reviews")
val businesses = sc.cassandraTable[Business]("yelp", "businesses")
val users = sc.cassandraTable[User]("yelp", "users")

val reviewsByBusinessId = reviews.keyBy(r => r.business_id) //(business_id, review)
val businessesByBusinessId = businesses.keyBy(b => b.business_id) //(business_id, business)

val reviewsForBusiness = businessesByBusinessId.join(reviewsByBusinessId) //(business_id, (business, review))
val reviewsForBusinessByUser = reviewsForBusiness.groupBy(r => r._2._2.user_id) //(user_id, Iterable[(review, business)])
val usersByUserId = users.keyBy(u => u.user_id) //(user_id, user)
val joinedUserReviewsForBusiness = usersByUserId.join(reviewsForBusinessByUser) //(user_id, (User, Iterable[(review, business)]))
val topWithNames = joinedUserReviewsForBusiness.map(j => (j._2._2.size, j._2)).sortByKey(false) //(nb_reviews, (User, Iterable[(business, review)]))
val topWithTopCity = topWithNames.map(t => (t._1, (t._2._1, t._2._2, t._2._2.groupBy(rb => rb._2._1.city).mapValues(_.size).maxBy(_._2)))) //RDD[(Int, ((User, Iterable[(String, (Business, Review))], (String, Int)))]

val topWithTopCityAndTopCategory = topWithTopCity.map(t => (t._1, (t._2._1, t._2._2, t._2._3, t._2._2.map(rb => rb._2._1.categories).flatten.foldLeft(Map[String,Int]() withDefaultValue 0){(m,x) => m + (x -> (1 + m(x)))}.maxBy(_._2))))

for( (nb_reviews,(user, reviews,(city, city_nb_reviews), (category, category_nb_reviews))) <- topWithTopCityAndTopCategory.take(5) ){
   println( user.name + " has " + nb_reviews + " total reviews. City is " + city + " with " + city_nb_reviews + " reviews. Most reviewed category is " + category + " with " + category_nb_reviews + " reviews.");
}

// val reviewsByUser = reviews.groupBy(r => r.user_id)
// val usersByUserId = users.map(u => (u.user_id, u))

// val joinedUserReviews = usersByUserId.join(reviewsByUser)

// val top5WithNames = joinedUserReviews.map(j => (j._2._1, j._2._2.size)).map(item => item.swap).sortByKey(false).map(item => item.swap).take(5)
//
// for( (user, nb_reviews) <- top5WithNames ){
//    println( user.name + " has " + nb_reviews + " reviews." );
// }

// reviewsByUser.first
//val businesses = sc.cassandraTable[Business]("yelp", "businesses").cache

//val reviewByBusinessId = reviews.filter(review => review.stars > 4.9).keyBy(r => r.business_id)
//val businessByBusinessId = businesses.filter(b => b.categories.contains("Mexican")).keyBy(b => b.business_id)

//val joinedBusiness = businessByBusinessId.join(reviewByBusinessId).cache

//joinedBusiness.count

//reviews.filter(review => review.text.contains("tacos")).filter(review => review.stars > 4.9)
