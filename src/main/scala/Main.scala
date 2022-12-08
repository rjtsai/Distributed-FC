import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

import java.io.PrintWriter
import scala.collection.mutable


/**
 * Data:
 * - for some reason, grabbing all teamAttributes from the 2015/2016 season yields ~30-40 fewer teams than
 *    there are in the Teams table. Querying to include 2013 & 2014 season entries and removing dupe team entries
 *    is also returning fewer team attributes than there are teams...
 *    - Team_Attributes.csv -> only 2015 entries
 *    - Team_Attributes_1.csv -> 2013, 2014 & 2015 szn entries
 *
 * - The Teams data does not include respective league ids. Furthermore, ID in the league table
 *    is actually the countryID in the db, which only appears in association to a team (or two) in
 *    the Matches table which is pretty inconvenient.
 *
 * - Team Attribute stats are stored as descriptions rn, but the db also has numeric values for some
 *    attributes. This may be easier to use for comparing, but the data is slightly inconsistent. I'm
 *    thinking of creating more custom classes to hold different attribute types (bu, cc, d) to better track
 *    compareTo metrics.
 */

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("distributed-fc").setMaster("local[4]")
    val sc = new SparkContext(conf);
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    // Some classes to organize data
    //prolly don't need this
    case class Date(year: Int, month: Int) extends Ordered[Date] {
      import scala.math.Ordered.orderingToOrdered
      def compare(that: Date): Int = (this.year, this.month).compare(that.year, that.month);
    }

    case class Country(id: Int, name: String);

    case class League(countryID: Int, name: String);

    case class Matches(id: Int, leagueID: Int, homeID: Int, awayID: Int, homeGoals: Int, awayGoals: Int, season: String);

    case class Player(id: Int, name: String, height: Double, weight: Int);

    case class Team(id: Int, name: String);
    case class TeamAttributes(id: Int, buSpeed: String, buDribbling: String, buPassing: String, buPositioning: String,
                              ccPassing: String, ccCrossing: String, ccShooting: String, ccPositioning: String, dPressure: String, dAggresion: String, dWidth: String, dLine: String)

    // RDD of data using custom classes

    val matchData = sc.textFile("./data/Matches.csv").map{line => {
      var temp = line.split(",");
      Matches(temp(0).toInt, temp(1).toInt, temp(2).toInt, temp(3).toInt, temp(4).toInt, temp(5).toInt, temp(6).substring(1,temp(6).length-1));
    }}

    val teamData = sc.textFile("./data/Teams.csv").map(line => {
      Team(line.split(",")(0).toInt, line.split(",")(1))
    })

    val teamAttributeData = sc.textFile("./data/Team_Attributes.csv").map(line => {
      var temp = line.split(",")
      var date = temp(1).replaceAll("\"", "").split("-")(0).toInt
      (temp(0).trim.toInt, TeamAttributes(temp(0).trim.toInt, temp(2), temp(3), temp(4), temp(5), temp(6),
        temp(7), temp(8), temp(9), temp(10), temp(11), temp(12), temp(13)))
    }).groupByKey().map{case (_, v) => {
      v.toList(0)
    }}

    val leagueData = sc.textFile("./data/Leagues.csv").map(line => {
      League(line.split(",")(0).toInt, line.split(",")(1))
    })

    // RDDs created from joins

    // teams w associated team attributes
    // (TeamID, TeamAttributes())
    // (9825,TeamAttributes(9825,Balanced,Normal,Short,Organised,Safe,Normal,Normal,Free Form,Medium,Press,Normal,Cover))
    val teamWithAttributes = teamData.map(x => (x.id, x.name)).leftOuterJoin(teamAttributeData.map(x => (x.id, x)))
      .map{case (id, (name, attr)) => {
        (id, attr.getOrElse(None))
      }}.filter(_._2 != None);

    // determine if a team is home or away
    // Matches, Int -> Int
    // 1 = home ; -1 = away
    def findHomeAway(m: Matches, id: Int): Int = if(m.homeID == id) 1 else -1;

    // find outcome of match given team
    // Matches, Int -> Int
    // 1 = win ; -1 = loss ; 0 = draw
    def calculateMatchOutcome(m: Matches, team: Int): Int = {
      var outcome = findHomeAway(m, team) * (m.homeGoals - m.awayGoals)
      if(outcome > 0) 1 else if(outcome < 0) -1 else 0
    }

    //Record that season
    // teams with w/d/l ratio
    // (Team(), List(w, d, l))
    // (Team(9825,Arsenal),List(20, 11, 7))
    val teamWithRecord = sc.parallelize(matchData.map{x => {
      List((x.homeID, x), (x.awayID, x))
    }}.collect().toList.flatten).rightOuterJoin(teamData.map{x =>{
      (x.id, x)
    } }).map{case (_, (m, team)) => {
      (team, m.getOrElse(None))
    }}.filter(_._2 != None).map{case (t, m) => {
      (t, (calculateMatchOutcome(m.asInstanceOf[Matches], t.id), 1))
    }}.groupByKey().mapValues(m => {
      m.groupBy(_._1).mapValues(_.size).toList.sortBy(_._1 * -1).map(_._2);
    }).map({ case (team, record) => (team.id,record)})

    // teams with records split home/away
    // ((Team(), home/away), Map(numLoss -> Int, numWin -> Int, numDraw -> Int)
    // home = 1, away = -1
    // loss = -1, win = 1, draw = 0
    val teamWithRecordHA = sc.parallelize(matchData.map { x => {
      List((x.homeID, x), (x.awayID, x))
    }}.collect().toList.flatten).rightOuterJoin(teamData.map { x => {
      (x.id, x)
    }}).map { case (_, (m, team)) => {
      (team, m.getOrElse(None))
    }}.filter(_._2 != None).map{case (t, m) => {
      var currMatch = m.asInstanceOf[Matches]
      ((t, findHomeAway(currMatch, t.id)), calculateMatchOutcome(currMatch, t.id))
    }}.groupByKey().mapValues(x => x.groupBy(identity).mapValues(_.size))

    def combineRecords(r1: Map[Int, Int], r2: Map[Int, Int]): Map[Int, Int] = {
      var loss: Int = r1.get(-1).get + r2.get(-1).get
      var win = r1.get(1).getOrElse(0) + r2.get(1).getOrElse(0)
      var draw = r1.get(0).getOrElse(0) + r2.get(0).getOrElse(0)
      Map(-1 -> loss, 1 -> win, 0 -> draw)
    }

    // finds w/d/l % of home and away teams
    // ("home" or "away", (win%, draw%, loss%))
    val aggregatedRecordsOfHA = teamWithRecordHA.map({case ((_, ha), record) => {
      (ha, record)
    }}).reduceByKey(combineRecords(_, _)).map{case (ha, record) => {
      var totalGames = record.foldLeft(0)(_+_._2).toDouble
      (if(ha == 1) "home" else "away", (record.get(1).get.toDouble / totalGames, record.get(0).get.toDouble / totalGames, record.get(-1).get.toDouble / totalGames))
    }}

    // find num goals for a team
    // Int, Matches -> Int
    def findNumGoalsFor(id: Int, m: Matches): Int = {
      if(id == m.homeID) m.homeGoals else m.awayGoals
    }

    // find num goals for a team
    // Int, Matches -> Int
    def findNumGoalsAgainst(id: Int, m: Matches): Int = {
      if (id == m.homeID) m.awayGoals else m.homeGoals
    }

    // find xG for team
    // (Team, xG)
    val teamWithxGFor = sc.parallelize(matchData.map { x => {
      List((x.homeID, x), (x.awayID, x))
    }}.collect().toList.flatten).rightOuterJoin(teamData.map { x => {
      (x.id, x)
    }}).map { case (_, (m, team)) => {
      (team, m.getOrElse(None))
    }}.map{ case (t, m) => {
      (t, findNumGoalsFor(t.id, m.asInstanceOf[Matches]))
    }}.groupByKey().map{case (t, xG) => (t.id, (t.name, xG.sum.toDouble / xG.size.toDouble))}

    // find xGA for team
    // (Team, xGA)
    val teamWithxGAgainst = sc.parallelize(matchData.map { x => {
      List((x.homeID, x), (x.awayID, x))
    }}.collect().toList.flatten).rightOuterJoin(teamData.map { x => {
      (x.id, x)
    }}).map { case (_, (m, team)) => {
      (team, m.getOrElse(None))
    }}.map { case (t, m) => {
      (t, findNumGoalsAgainst(t.id, m.asInstanceOf[Matches]))
    }}.groupByKey().map { case (t, xG) => (t.id, (t.name, xG.sum.toDouble / xG.size.toDouble)) }

    // determine which combination of build up play attributes has the highest xG
    // ((buSpeed, buPassing, buDribbline, buPositioning), xG)
    /*
      top 3 results:
      ((Balanced,Short,Little,Free Form),2.400735294117647)
      ((Balanced,Short,Normal,Free Form),1.993421052631579)
      ((Balanced,Mixed,Normal,Free Form),1.739363113666519)
     */
    val xGFromBUAttributes = teamWithxGFor.rightOuterJoin(teamWithAttributes).map { case (_, (xG, attr)) => {
      var tAttr = attr.asInstanceOf[TeamAttributes]
      ((tAttr.buSpeed, tAttr.buPassing, tAttr.buDribbling, tAttr.buPositioning), xG.get._2)
    }}.groupByKey().mapValues(x => x.sum / x.size).collect().sortBy(_._2 * -1)

    // determine which combination of chance creation attributes has the highest xG
    // ((ccPassing, ccCrossing, ccShooting, ccPositioning), xG)
    /*
      top 3 results:
      ((Little,Little,Little,Free Form),2.400735294117647)
      ((Little,Normal,Little,Free Form),1.9184769521843827)
      ((Normal,Normal,Normal,Free Form),1.5470414357924358)
     */
    val xGFromCCAttributes = teamWithxGFor.rightOuterJoin(teamWithAttributes).map{case (_, (xG, attr)) => {
      var tAttr = attr.asInstanceOf[TeamAttributes]
      ((tAttr.ccShooting, tAttr.ccCrossing, tAttr.ccShooting, tAttr.ccPositioning), xG.get._2)
    }}.groupByKey().mapValues(x => x.sum / x.size).collect().sortBy(_._2 * -1)

    // determine which combination of defense attributes has the lowest xGA
    // ((dLine, dWidth, dPressure, dAggresion), xG)
    /*
      top 3 results:
      ((Cover,Normal,High,Press),0.7757352941176471)
      ((Cover,Normal,Medium,Double),1.1231617647058822)
      ((Offside Trap,Normal,Medium,Press),1.3288201178156809)
     */
    val xGFromDAttributes = teamWithxGAgainst.rightOuterJoin(teamWithAttributes).map { case (_, (xGA, attr)) => {
      var tAttr = attr.asInstanceOf[TeamAttributes]
      ((tAttr.dLine, tAttr.dWidth, tAttr.dPressure, tAttr.dAggresion), xGA.get._2)
    }
    }.groupByKey().mapValues(x => x.sum / x.size).collect().sortBy(_._2)

    def createTrainingData(team1: Int, team2: Int): Unit = {
      //Find the first 6 GD scores for all the teams in 2015/16 season premier league
      //still splits the data
      // Set up RDD from out.txt?
      // LR needs to read from the file we just created

      val pw = new PrintWriter("./output/TrainLR.csv")

      val All_teams_GD = matchData.filter(x => {
        x.leagueID == 1729 && x.season == "2015/2016" && (x.homeID != team1 || x.awayID != team2) &&
          (x.homeID != team2 || x.awayID != team1)
      }).
        sortBy(x => x.homeID).
        map(x => (x.homeID, x.homeGoals - x.awayGoals)).
        groupByKey().mapValues(x => x.toList).collect().map({ case (team_id, gd) =>
        pw.write(team_id + ", " + gd(0) + ", " + gd(1) + ", " + gd(2) + ", " + gd(3) + ", " + gd(4) + ", " + gd(5) + "\r\n")
      })
      pw.close()
    }

    def calculateLM(): Unit = {
      //Dependent variable => Goal Difference
      //Independent variable => Match record
      var df = spark.sqlContext.read.format("csv").
        option("header","false").
        option("inferSchema","true").csv("./output/TrainLR.csv").
        toDF("TEAM","GD1","GD2","GD3","GD4","GD5","label")

      val assembler = new VectorAssembler()
        .setInputCols(Array("GD1","GD2","GD3","GD4","GD5","label"))
        .setOutputCol("features")
        .transform(df)

      //for more accurate results
      val normalizer = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(2.0)
        .transform(assembler)

      //LR regression settings
      val lr = new LinearRegression()
        .setLabelCol("label")
        .setFeaturesCol("normFeatures")
        .setMaxIter(100)
        .setRegParam(0.3)

      //Our two teams that we want to predict performance against each other
      var testData = spark.sqlContext.read.format("csv").
        option("header", "false").
        option("inferSchema", "true").csv("./output/TestLR.csv").
        toDF("TEAM", "GD1", "GD2", "GD3", "GD4", "GD5", "label")

      val assembler2 = new VectorAssembler()
        .setInputCols(Array("GD1", "GD2", "GD3", "GD4", "GD5", "label"))
        .setOutputCol("features")
        .transform(testData)

      val normalizer2 = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(2.0)
        .transform(assembler2)

      //USE the entire data set for building the model
      val  trainingData = normalizer
      val lrModel = lr.fit(trainingData)

      lrModel.transform(normalizer2).select("TEAM","label","prediction").show()

      // Summarize the model over the training set and print out some metrics
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//      val trainingSummary = lrModel.summary
//      println(s"numIterations: ${trainingSummary.totalIterations}")
//      println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
//      trainingSummary.residuals.show()
//      println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//      println(s"r2: ${trainingSummary.r2}")
    }

    def getTeamIds(team1: String, team2: String): (Int, Int) = {
      val team1ID = teamData.filter(x=> {x.name == team1}).map(x => x.id).take(1)(0)
      val team2ID = teamData.filter(x=> {x.name == team2}).map(x => x.id).take(1)(0)
      (team1ID, team2ID)
    }

    def getRealMatchOutcome(team1: Int, team2: Int, season: String, leagueID: Int) : mutable.HashSet[String] = {
      var s = new mutable.HashSet[String]()
      val team_GD = matchData.filter(x => {
        x.leagueID == leagueID && x.season == season && (x.homeID == team1 || x.homeID == team2)
      })
        .map(x => (x.homeID, x.homeGoals - x.awayGoals))
        .groupByKey().mapValues(x => x.toList).collect().map({ case (team_id, gd) =>
        s.add("Team ID: " + team_id + " " + "Actual score: " + gd(6).toString)
      })
      s
    }

    def getTwoTeams(team1: Int, team2: Int, season: String, leagueID: Int): Unit = {
      val pw = new PrintWriter("./output/TestLR.csv")
      val team_GD = matchData.filter(x => {
        x.leagueID == leagueID && x.season == season && (x.homeID == team1 || x.homeID == team2)
      })
        .map(x => (x.homeID, x.homeGoals - x.awayGoals))
        .groupByKey().mapValues(x => x.toList).collect().map({ case (team_id, gd) =>
        pw.write(team_id + ", " + gd(0) + ", " + gd(1) + ", " + gd(2) + ", " + gd(3) + ", " + gd(4) + ", " + gd(5) + "\r\n")
      })
      pw.close()
    }

    val teamIDs = getTeamIds("Bournemouth", "Arsenal")
    createTrainingData(teamIDs._1, teamIDs._2)
    getTwoTeams(teamIDs._1, teamIDs._2, "2015/2016", 1729)
    calculateLM()
    val real_outcomes = getRealMatchOutcome(teamIDs._1, teamIDs._2,"2015/2016", 1729)
    real_outcomes.foreach(println)
  }
}