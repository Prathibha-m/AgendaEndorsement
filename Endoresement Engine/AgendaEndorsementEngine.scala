import java.io.{File, PrintWriter}

import breeze.numerics.toRadians
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.render
import net.liftweb.json.Printer.pretty
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import net.liftweb.json._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object AgendaEndorsementEngine {
  // Weight which indicates user's bias towards distance or ratings
  var weight:Double=0.5
  var userProvidedWeight=false

  // Classes for writing the output to JSON
  case class InputJSON (user_id:String, tasks: Array[String], lat:Double, long:Double )
  case class PlanRatings(ID: Int, plan: List[(String,String,Double)], averageRating: Double, roundTripDistance : Double)
  case class planScore( plan: PlanRatings, score: Double)
  case class plansJSON(inputRow:InputJSON, randomPlan: planScore, shortestDistPlan: PlanRatings, bestRatedPlan: PlanRatings, optimalPlan: planScore )
  case class outputJSON(results: ListBuffer[plansJSON])
  var results= new ListBuffer[plansJSON]()

  // euclidean distance method
  def euclideanDistance(locations: List[Tuple2[Double,Double]]): Double = {

    var dist: Double = 0.0

    for (index <-(1 to locations.size-1)) {
      dist+=math.sqrt(math.pow(locations(index)._1-locations(index-1)._1, 2)-math.pow(locations(index)._2-locations(index-1)._2, 2))
    }

    dist
  }

  // Sine Distance method to calculate distance between a list of distances
  def sinDistance(locations: List[Tuple2[Double,Double]]): Double = {
    var sum :Double = 0.0
    for(index <-(1 to locations.size-1)) {
      val lat2 = toRadians(locations(index)._1)
      val lat1 = toRadians(locations(index-1)._1)
      val lon2 = toRadians(locations(index)._2)
      val lon1 = toRadians(locations(index-1)._2)


      var dlon = lon2 - lon1
      var dlat = lat2 - lat1
      var a = Math.pow((Math.sin(dlat / 2)),2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow((Math.sin(dlon / 2)),2)
      var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      var d = c * 3961 // distance as provided in the post : https://www.movable-type.co.uk/scripts/latlong.html
      sum +=d
    }
    sum

  }

  // From the top selected businesses from each category, forms all possible combinations by picking each business from each category
  // Code referenced from - https://rosettacode.org/wiki/Cartesian_product_of_two_or_more_lists
  def cartesianProduct[T](top10Map: Map[String, Array[(String, String, Double)]], tasksArray: Array[String]): List[List[(String, String, Double)]] = {

    /**
      * Prepend single element to all lists of list
      * @param e single elemetn
      * @param ll list of list
      * @param a accumulator for tail recursive implementation
      * @return list of lists with prepended element e
      */
    def pel(e: (String, String, Double),
            ll: List[List[(String, String, Double)]],
            a: List[List[(String, String, Double)]] = Nil): List[List[(String, String, Double)]] =
      ll match {
        case Nil => a.reverse
        case x :: xs => pel(e, xs, (e :: x) :: a )
      }

    var lst = ListBuffer[List[(String, String, Double)]]()


    for(task <- tasksArray) {
      var row = top10Map.get(task).get
      lst += row.toList
    }

    lst.toList match {
      case Nil => Nil
      case x :: Nil => List(x)
      case x :: _ =>
        x match {
          case Nil => Nil
          case _ =>
            lst.par.foldRight(List(x))( (l, a) =>
              l.flatMap(pel(_, a))
            ).map(_.dropRight(x.size))
        }
    }
  }

  def agendaEndorser (spark: SparkSession, sc: SparkContext, row: InputJSON):Unit= {

    import spark.implicits._


    //The columns of interest
    val columns = Seq("business_id", "UmbrellaTermCategory", "longitude", "latitude")
    val biz_IDNameColumns = Seq("business_id", "name")
    val uid = row.user_id
    val userLat = row.lat
    val userLong = row.long

    val tasks = row.tasks


    val data = spark.read.format("csv").option("header", "true").load("filteredBusinesses_new.csv")
    val dataColumns = data.select(columns.head, columns.tail:_*)
    val dataColumns1 = data.select(biz_IDNameColumns.head, biz_IDNameColumns.tail:_*)

    data.createOrReplaceTempView("Results")

    // method to return the business name for the business id
    def getBizName(planVar: planScore): planScore ={
      var tempArray = ListBuffer[(String, String, Double)]()
      planVar.plan.plan.foreach( row => {

        var bizID = dataColumns1.filter($"business_id"===row._1)
        var bizName = bizID.first.getString(1)
         var t: (String, String, Double) = (row._1, bizName, row._3.toDouble)
        tempArray +=t
      })

      return planScore(PlanRatings(planVar.plan.ID,tempArray.toList,planVar.plan.averageRating,planVar.plan.roundTripDistance),planVar.score)
    }


    val top10BizTaskMap =scala.collection.mutable.Map[String, Array[(String, String, Double)]]()
    val model = MatrixFactorizationModel.load(sc, "CF Matrix Model")
    val userToIntMap = sc.textFile("UserIdToInt.txt/part-00000")
    val BizToIntMap = sc.textFile("BizIDToInt.txt/part-00000")


    // Maps for user id to Int for the Ratings library
    var userIntMap = userToIntMap.map(row => {
      val column = row.split(',')

      (column(0),column(1))
    }).collectAsMap()

    // Maps for Business id to Int for the Ratings library
    var BizIntMap_rdd = BizToIntMap.map(row => {
      val column = row.split(',')
      (column(0),column(1))
    })
    var BizIntMap=BizIntMap_rdd.collectAsMap()

    // Reverse map for biz id to int
    val reverseMap_BusinessIdToString: RDD[(Long, String)]=
      BizIntMap_rdd map{ case (r, l) => (l.toLong, r) }
    val revBizMap=reverseMap_BusinessIdToString.collectAsMap()


    // Random task selected to compare with the endorsement engine
    var randomTasks = ListBuffer[(String,String,Double)]()
    for( task<- tasks) {
      val task1_sqlDF = dataColumns.filter(($"UmbrellaTermCategory").contains(task)).select("business_id")

      var userTask_map = task1_sqlDF.map (
        row => {
          if(BizIntMap.contains(row.getString(0))) {
            // Biz is present in model
            (userIntMap.get(uid).get.toString.toInt, BizIntMap.get(row.getString(0)).get.toString.toInt)
          }
          else
          {
            (userIntMap.get(uid).get.toString.toInt, -1)
          }
        }
      ).rdd.filter(row => row._2!= -1) // filter out biz not present in model

      //Recommendation library of mllib
      val predictions = model.predict(userTask_map)
      val topBiz = predictions
        .map( row => {
          var bizString = revBizMap.get(row.product).get
          (bizString, "", row.rating)
        }).collect()

      print("-----------------------------------------------------------------------------------------------------")


      val maxRating = topBiz.maxBy(_._3)._3
      val minRating = topBiz.minBy(_._3)._3


      val newMax = 5
      val newMin = 0

      // We encountered negative values from the predictions of Ratings (mllib) class
      // Hence , the normalisation to a range of (0,5)
      def normalize(tuple: (String, String, Double)): (String, String, Double) = {
        val newVal = ((newMax - newMin)*(tuple._3 - minRating)/ (maxRating - minRating)) + newMin
        (tuple._1, tuple._2, newVal)
      }
      val scaledBiz = topBiz.map(normalize)
      val scaledTopBiz = scaledBiz.sortBy(-_._3).take(5)
      val rand = new Random(System.currentTimeMillis())
      val random_index = rand.nextInt(scaledBiz.length)
      randomTasks+=scaledBiz(random_index)
      top10BizTaskMap(task) = scaledTopBiz

    }


    var randomTasksAvg=0.0
    var bizListRand = ListBuffer[Tuple2[Double, Double]]()
    bizListRand += Tuple2(userLat, userLong)
    // Calculating the score of the randomly selected plan
    randomTasks.foreach( task => {

      var dataSelected = dataColumns.filter($"business_id"===task._1)
      var lat = dataSelected.first.getString(3).toDouble
      var long = dataSelected.first.getString(2).toDouble
      bizListRand+= Tuple2(lat,long)
      randomTasksAvg += task._3
    })
    randomTasksAvg=randomTasksAvg/randomTasks.length
    var randomTasksDist = sinDistance(bizListRand.toList)
    var randomPlan = PlanRatings(-1, randomTasks.toList, randomTasksAvg, randomTasksDist)



    // Call cartesian product to form combinations from top 10 selected businesses in each category
    val cartProd = cartesianProduct(top10BizTaskMap.toMap, tasks)//.map(_.mkString("(", ", ", ")")).mkString("{",", ","}")


    // For each of the plan in the combination, we calculate the average rating and round trip distance
    var planRatingMap = cartProd.zipWithIndex.map(
      row => {
        var sum = 0.0
        val count = row._1.size
        val planID = row._2
        val bizList = ListBuffer[Tuple2[Double,Double]]()
        bizList+=Tuple2(userLat,userLong)
        // Creates a list of business location for calculaating the round trip distance
        for (r <- row._1) {
          sum += r._3
          var dataSelected = dataColumns.filter($"business_id"===r._1)
          var lat = dataSelected.first.getString(3).toDouble
          var long = dataSelected.first.getString(2).toDouble
          bizList+= Tuple2(lat,long)
        }
        // Euclidean distance can be used as well, but the distance is very small, so NA values are found
        var dist = sinDistance(bizList.toList)
        PlanRatings(planID, row._1, sum/count, dist)
      }
    )
    // Calculating the max and min values in ratings and distances
    var max_rating = planRatingMap.maxBy(_.averageRating).averageRating
    var min_rating = planRatingMap.minBy(_.averageRating).averageRating
    val max_dist = planRatingMap.maxBy(_.roundTripDistance).roundTripDistance
    val min_dist = planRatingMap.minBy(_.roundTripDistance).roundTripDistance


    val newMax = 10
    val newMin = 0

    // Normalises the values within the range of (newMax,newMin)
    def normalizedScore(plan :PlanRatings): planScore = {

      val newRating = ((newMax - newMin)*(plan.averageRating - min_rating)/ (max_rating - min_rating)) + newMin
      val newDist = ((newMax - newMin)*(plan.roundTripDistance - min_dist)/ (max_dist - min_dist)) + newMin
      val score=weight*newRating+(1-weight)*(newMax-newDist)
      planScore(PlanRatings(plan.ID, plan.plan,plan.averageRating,plan.roundTripDistance),score)
    }

    // If the happiness factor (or user bias towards the trade-off between distance and ratings) is not specified
    // by the user
    if(userProvidedWeight==false)
    {
      // By default three plans are provided
      // Optimal Plan : Gives equal preference to distance and ratings
      // Best Rated Plane : Gives complete preference to Ratings
      // Shortest Plan : Gives complete preference to shortest distance
      val scoreMap=planRatingMap.map(normalizedScore)


      val optimalPlan=getBizName(scoreMap.sortBy(-_.score).take(1)(0))

      val bestRatedPlan=getBizName(scoreMap.sortBy(-_.plan.averageRating).take(1)(0)).plan

      val shortestPlan=getBizName(scoreMap.sortBy(_.plan.roundTripDistance).take(1)(0)).plan

      min_rating = 0.0
      max_rating = 5.0
      val normalizedRand = getBizName(normalizedScore(randomPlan))
      results+= plansJSON(row,normalizedRand,shortestPlan,bestRatedPlan,optimalPlan)

    }
    else
    {
      weight=0.7
      val scoreMap=planRatingMap.map(normalizedScore)
      scoreMap.sortBy(-_.score).take(1)
    }

  }

  def main (args:Array[String]):Unit= {

    // main method for initialising the spark context and other global variables
    val spark_config = new SparkConf().setAppName("Agenda Endorsement").setMaster("local[*]")
    val sc = new SparkContext(spark_config)

    val spark = SparkSession
      .builder.master("local[2]")
      .appName("AE")
      .getOrCreate()

    sc.setLogLevel("ERROR")

    val inputFile = sc.textFile("InputFile_CSV.csv")

    implicit  val formats = DefaultFormats

    val inputData = inputFile.collect().map {
      entry=>
        val column  = entry.split(',')
        val tasks = column(1).split(":")

        InputJSON(column(0),tasks, column(2).toDouble,column(3).toDouble)
    }
    inputData.foreach(println)
    inputData.foreach(row =>agendaEndorser(spark, sc, row))

    // Writing the output
    val printWriter = new PrintWriter(new File("Results.json"))
    printWriter.write( pretty(render(decompose(outputJSON(results)))))
    printWriter.close()


    println("End of Agenda Endorsement Engine")



  }

}
