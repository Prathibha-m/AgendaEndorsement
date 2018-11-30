import breeze.numerics.{abs, sqrt}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
object ProjectCF extends App {
  override def main (args:Array[String]):Unit={

    val startTime = System.currentTimeMillis()

    case class TypedRating(user_id: String, business_id: String, stars: Double)

    val spark_config = new SparkConf().setAppName("DM Assignment 2_ModelBasedCF").setMaster("local[*]")
    val sc = new SparkContext(spark_config)

    val sqlContext = new SQLContext(sc)
    var df = sqlContext.read.format("csv").option("header","true").load("usersForBiz_v3.csv")
    val Array(traindf,testdf) = df.randomSplit(Array[Double](1.0,0.0))


    var train_data = df.rdd
    var test_data = testdf.rdd
    var columnNames = train_data.first()
    //train_data = test_data.filter(row => row != columnNames)
    //test_data = test_data.filter(row => row != columnNames)

    val missingUID_BID =scala.collection.mutable.Map[(String, String), Double]()
    val train_ratings: RDD[TypedRating] = train_data.map {
      entry =>

        TypedRating(entry(0).toString, entry(1).toString, entry(2).toString.toDouble)
    }


    val test_ratings: RDD[TypedRating] = test_data.map {
      entry=>

        TypedRating(entry(0).toString, entry(1).toString, entry(2).toString.toDouble)
    }


    val systemAvgMap = train_ratings.map(x => ("temp_key", ( x.stars,1))).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (total, count) => total.toDouble / count }.collectAsMap()
    val overallAverage = systemAvgMap.get("temp_key").get
    val test_ratings_map = test_ratings.map(row => ((row.user_id,row.business_id),row.stars))

    /***/
    val userIdToIntMapping: RDD[(String, Long)] =
      train_ratings.map(_.user_id).distinct().zipWithUniqueId()


    val businessIdToIntMapping: RDD[(String, Long)] =
      train_ratings.map(_.business_id).distinct().zipWithUniqueId()

   userIdToIntMapping.map(x=> x._1+","+x._2).repartition(1).saveAsTextFile("UserIDToInt.txt")
    businessIdToIntMapping.map(x=> x._1+","+x._2).repartition(1).saveAsTextFile("BizIDToInt.txt")

    val reverseMap_UserIdToString: RDD[(Long, String)]=
      userIdToIntMapping map { case (l, r) => (r, l) }

    val reverseMap_BusinessIdToString: RDD[(Long, String)]=
      businessIdToIntMapping map { case (l, r) => (r, l) }


    reverseMap_UserIdToString.map(x=> x._1+","+x._2).repartition(1).saveAsTextFile("UserIDIntToString.txt")
    reverseMap_BusinessIdToString.map(x=> x._1+","+x._2).repartition(1).saveAsTextFile("BizIDIntToString.txt")

    /***/

    val userIDIntMap = userIdToIntMapping.collectAsMap()
    val businessIDIntMap = businessIdToIntMapping.collectAsMap()
    val userIDStringMap = reverseMap_UserIdToString.collectAsMap()
    val businessIDStringMap = reverseMap_BusinessIdToString.collectAsMap()

    val rating_IntTyped =  train_ratings.map(row => {

      val user = row.user_id
      val business = row.business_id
      val stars = row.stars

      Rating(userIDIntMap.get(user).get.toInt, businessIDIntMap.get(business).get.toInt, stars.toDouble)

    })

    val testRating_IntTyped =  test_ratings.map(row => {

      val user = row.user_id
      val business = row.business_id
      val stars = row.stars

      Rating(userIDIntMap.get(user).get.toInt, businessIDIntMap.get(business).get.toInt, stars.toDouble)

    })


    /***/
    val existingUserID_List = train_ratings.map(row=> row.user_id).distinct().collect()
    val existingBusinessID_List = train_ratings.map(row => row.business_id).distinct().collect()
    /***/


    /***/
    val perUserAverage=train_ratings.map(row=> (row.user_id,(row.stars,1))).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2)).map(row =>(row._1,row._2._1/row._2._2)).collectAsMap()
    val perBusinessAverage=train_ratings.map(row=> (row.business_id,(row.stars,1))).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2)).map(row =>(row._1,row._2._1/row._2._2)).collectAsMap()

    val rank = 8
    val numIterations = 15
    rating_IntTyped.cache()
    val model = ALS.train(rating_IntTyped, rank, numIterations, 0.31, -1, seed = 9375629)

//  val uid = 133102
//  val bid=2305
    model.save(sc, "CF Matrix Model")
    //val predictions = model.predict(uid,bid)


  }

}
